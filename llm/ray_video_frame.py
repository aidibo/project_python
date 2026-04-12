#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0
# https://claude.ai/chat/e8fee18e-3c82-45dd-a248-2860d7deeb13

"""
使用 Ray 并行对 H.265 视频流进行抽帧
支持本地文件、RTSP/HTTP 直播流

依赖安装:
    pip install ray opencv-python-headless av

使用方法:
    # 本地文件抽帧
    python video_frame_extractor.py --input video.mp4 --output ./frames --fps 1

    # RTSP 直播流抽帧
    python video_frame_extractor.py --input rtsp://192.168.1.100/stream --output ./frames --fps 2 --live

    # 批量处理多个文件
    python video_frame_extractor.py --input_dir ./videos --output ./frames --fps 1
"""

import ray
import cv2
import os
import time
import argparse
import logging
from pathlib import Path
from typing import Optional, List, Tuple
from dataclasses import dataclass, field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# 数据结构
# ─────────────────────────────────────────────

@dataclass
class ExtractConfig:
    """抽帧配置"""
    output_dir: str = "./frames"
    target_fps: float = 1.0          # 每秒抽取帧数，0 = 全部帧
    max_frames: int = 0              # 最大帧数限制，0 = 不限制
    image_format: str = "jpg"        # jpg / png
    jpeg_quality: int = 90           # JPEG 质量 (1-100)
    resize: Optional[Tuple[int,int]] = None   # (width, height)，None = 原始尺寸
    gpu_decode: bool = False         # 是否启用 NVDEC 硬件解码（需 CUDA）
    chunk_seconds: float = 10.0      # 视频分块大小（秒），用于并行处理


@dataclass
class FrameResult:
    """单帧处理结果"""
    frame_index: int
    timestamp: float
    file_path: str
    success: bool
    error: str = ""


@dataclass
class ExtractSummary:
    """抽帧汇总"""
    source: str
    total_frames_scanned: int
    frames_saved: int
    duration_seconds: float
    elapsed_seconds: float
    fps_actual: float
    output_dir: str
    errors: List[str] = field(default_factory=list)


# ─────────────────────────────────────────────
# Ray Remote 任务：处理一个视频片段
# ─────────────────────────────────────────────

@ray.remote
def extract_frames_chunk(
    video_path: str,
    start_frame: int,
    end_frame: int,
    config_dict: dict,
    worker_id: int,
) -> List[FrameResult]:
    """
    Ray 任务：抽取视频中 [start_frame, end_frame) 范围内的帧

    参数:
        video_path:   本地视频路径
        start_frame:  起始帧编号（含）
        end_frame:    结束帧编号（不含），-1 表示到末尾
        config_dict:  ExtractConfig.__dict__
        worker_id:    Worker 编号，用于日志

    返回:
        List[FrameResult]
    """
    import cv2
    import os

    cfg = ExtractConfig(**config_dict)
    os.makedirs(cfg.output_dir, exist_ok=True)

    results: List[FrameResult] = []

    # 打开视频，优先使用 FFMPEG 后端以支持 H.265
    cap = cv2.VideoCapture(video_path, cv2.CAP_FFMPEG)
    if not cap.isOpened():
        return [FrameResult(start_frame, 0.0, "", False, f"Worker {worker_id}: 无法打开视频")]

    native_fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    total_native = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    # 定位到起始帧
    if start_frame > 0:
        cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

    # 计算抽帧间隔（帧步长）
    if cfg.target_fps <= 0 or cfg.target_fps >= native_fps:
        step = 1
    else:
        step = max(1, int(round(native_fps / cfg.target_fps)))

    video_stem = Path(video_path).stem
    frame_idx = start_frame
    local_count = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break
        if end_frame != -1 and frame_idx >= end_frame:
            break
        if cfg.max_frames > 0 and len(results) >= cfg.max_frames:
            break

        # 按步长抽帧
        if (frame_idx - start_frame) % step == 0:
            timestamp = frame_idx / native_fps

            # 可选缩放
            if cfg.resize:
                frame = cv2.resize(frame, cfg.resize, interpolation=cv2.INTER_AREA)

            # 保存
            filename = f"{video_stem}_f{frame_idx:08d}_t{timestamp:.3f}.{cfg.image_format}"
            filepath = os.path.join(cfg.output_dir, filename)

            if cfg.image_format == "jpg":
                params = [cv2.IMWRITE_JPEG_QUALITY, cfg.jpeg_quality]
            else:
                params = [cv2.IMWRITE_PNG_COMPRESSION, 3]

            ok = cv2.imwrite(filepath, frame, params)
            results.append(FrameResult(
                frame_index=frame_idx,
                timestamp=timestamp,
                file_path=filepath if ok else "",
                success=ok,
                error="" if ok else f"imwrite 失败: {filepath}",
            ))
            local_count += 1

        frame_idx += 1

    cap.release()
    return results


# ─────────────────────────────────────────────
# Ray Remote Actor：处理实时流
# ─────────────────────────────────────────────

@ray.remote
class LiveStreamActor:
    """
    Ray Actor：持续读取 RTSP/HTTP 直播流并抽帧

    使用 Actor 而非无状态 Task，原因：
      - 直播流连接需要持久维护
      - 便于外部发送停止信号
    """

    def __init__(self, stream_url: str, config_dict: dict):
        import cv2, os
        self.url = stream_url
        self.cfg = ExtractConfig(**config_dict)
        os.makedirs(self.cfg.output_dir, exist_ok=True)

        self._running = False
        self._results: List[FrameResult] = []
        self._errors: List[str] = []

    def start(self) -> None:
        """启动抽帧循环（阻塞，应在单独线程/协程中调用）"""
        import cv2, time, os

        self._running = True
        cap = cv2.VideoCapture(self.url, cv2.CAP_FFMPEG)

        if not cap.isOpened():
            self._errors.append(f"无法连接直播流: {self.url}")
            self._running = False
            return

        native_fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
        step = max(1, int(round(native_fps / self.cfg.target_fps))) if self.cfg.target_fps > 0 else 1

        frame_idx = 0
        stream_name = self.url.split("/")[-1].replace("?", "_")

        while self._running:
            ret, frame = cap.read()
            if not ret:
                # 断流重连
                time.sleep(1.0)
                cap.release()
                cap = cv2.VideoCapture(self.url, cv2.CAP_FFMPEG)
                continue

            if frame_idx % step == 0:
                timestamp = time.time()
                if self.cfg.resize:
                    frame = cv2.resize(frame, self.cfg.resize, interpolation=cv2.INTER_AREA)

                filename = f"live_{stream_name}_{frame_idx:08d}_{timestamp:.3f}.{self.cfg.image_format}"
                filepath = os.path.join(self.cfg.output_dir, filename)

                if self.cfg.image_format == "jpg":
                    params = [cv2.IMWRITE_JPEG_QUALITY, self.cfg.jpeg_quality]
                else:
                    params = []

                ok = cv2.imwrite(filepath, frame, params)
                self._results.append(FrameResult(frame_idx, timestamp, filepath if ok else "", ok))

                if self.cfg.max_frames > 0 and len(self._results) >= self.cfg.max_frames:
                    break

            frame_idx += 1

        cap.release()
        self._running = False

    def stop(self) -> None:
        self._running = False

    def get_results(self) -> List[FrameResult]:
        return list(self._results)

    def is_running(self) -> bool:
        return self._running

    def frame_count(self) -> int:
        return len(self._results)


# ─────────────────────────────────────────────
# 主入口：本地文件并行抽帧
# ─────────────────────────────────────────────

def extract_local_video(
    video_path: str,
    config: ExtractConfig,
    num_workers: int = 4,
) -> ExtractSummary:
    """
    使用 Ray 并行对本地 H.265 视频文件进行抽帧

    工作流：
      1. 获取视频元信息（总帧数、FPS）
      2. 将视频帧范围均匀分成 num_workers 个 chunk
      3. 并发提交 Ray Task
      4. 汇总结果
    """
    t0 = time.time()

    cap = cv2.VideoCapture(video_path, cv2.CAP_FFMPEG)
    if not cap.isOpened():
        raise RuntimeError(f"无法打开视频: {video_path}")

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    native_fps   = cap.get(cv2.CAP_PROP_FPS) or 25.0
    duration     = total_frames / native_fps
    cap.release()

    logger.info(f"视频信息: {total_frames} 帧, {native_fps:.2f} fps, {duration:.1f}s")

    # 分块
    chunk_frames = max(1, int(config.chunk_seconds * native_fps))
    chunks: List[Tuple[int, int]] = []
    start = 0
    while start < total_frames:
        end = min(start + chunk_frames, total_frames)
        chunks.append((start, end))
        start = end

    logger.info(f"分成 {len(chunks)} 个 chunk，每块约 {config.chunk_seconds}s，使用 {num_workers} workers")

    os.makedirs(config.output_dir, exist_ok=True)
    config_dict = config.__dict__.copy()

    # 并发提交 Ray Tasks
    futures = [
        extract_frames_chunk.remote(
            video_path, s, e, config_dict, i
        )
        for i, (s, e) in enumerate(chunks)
    ]

    # 收集结果（带进度）
    all_results: List[FrameResult] = []
    errors: List[str] = []
    done = 0

    while futures:
        ready, futures = ray.wait(futures, num_returns=1, timeout=5.0)
        for ref in ready:
            try:
                chunk_results = ray.get(ref)
                all_results.extend(chunk_results)
                errors.extend(r.error for r in chunk_results if not r.success)
            except Exception as e:
                errors.append(str(e))
        done += len(ready)
        logger.info(f"进度: {done}/{len(chunks) + done} chunks 完成，已保存 {len(all_results)} 帧")

    elapsed = time.time() - t0
    saved = sum(1 for r in all_results if r.success)

    summary = ExtractSummary(
        source=video_path,
        total_frames_scanned=total_frames,
        frames_saved=saved,
        duration_seconds=duration,
        elapsed_seconds=elapsed,
        fps_actual=saved / elapsed if elapsed > 0 else 0,
        output_dir=config.output_dir,
        errors=errors,
    )

    logger.info(
        f"\n{'='*50}\n"
        f"抽帧完成!\n"
        f"  来源       : {summary.source}\n"
        f"  视频时长   : {summary.duration_seconds:.1f}s\n"
        f"  保存帧数   : {summary.frames_saved}\n"
        f"  耗时       : {summary.elapsed_seconds:.2f}s\n"
        f"  处理速度   : {summary.fps_actual:.1f} frames/s\n"
        f"  输出目录   : {summary.output_dir}\n"
        f"  错误数     : {len(summary.errors)}\n"
        f"{'='*50}"
    )
    return summary


# ─────────────────────────────────────────────
# 主入口：直播流抽帧
# ─────────────────────────────────────────────

def extract_live_stream(
    stream_url: str,
    config: ExtractConfig,
    duration_seconds: float = 60.0,
) -> ExtractSummary:
    """
    使用 Ray Actor 对 RTSP/HTTP 直播流进行抽帧

    参数:
        stream_url:       流地址，如 rtsp://... 或 http://...
        config:           抽帧配置
        duration_seconds: 抽帧持续时间（秒）
    """
    t0 = time.time()
    config_dict = config.__dict__.copy()

    actor = LiveStreamActor.remote(stream_url, config_dict)
    task_ref = actor.start.remote()

    logger.info(f"直播流抽帧启动: {stream_url}，持续 {duration_seconds}s")

    # 等待指定时长，期间每 2 秒打印进度
    deadline = t0 + duration_seconds
    while time.time() < deadline:
        time.sleep(2.0)
        count = ray.get(actor.frame_count.remote())
        elapsed = time.time() - t0
        logger.info(f"  直播抽帧进度: {elapsed:.0f}s 已过，已保存 {count} 帧")

    # 发送停止信号
    ray.get(actor.stop.remote())
    time.sleep(1.0)  # 等待当前帧处理完毕

    results: List[FrameResult] = ray.get(actor.get_results.remote())
    elapsed = time.time() - t0
    saved = sum(1 for r in results if r.success)

    summary = ExtractSummary(
        source=stream_url,
        total_frames_scanned=saved,
        frames_saved=saved,
        duration_seconds=duration_seconds,
        elapsed_seconds=elapsed,
        fps_actual=saved / elapsed if elapsed > 0 else 0,
        output_dir=config.output_dir,
        errors=[r.error for r in results if not r.success],
    )

    logger.info(
        f"\n{'='*50}\n"
        f"直播抽帧完成!\n"
        f"  来源       : {summary.source}\n"
        f"  保存帧数   : {summary.frames_saved}\n"
        f"  耗时       : {summary.elapsed_seconds:.2f}s\n"
        f"  处理速度   : {summary.fps_actual:.1f} frames/s\n"
        f"  输出目录   : {summary.output_dir}\n"
        f"{'='*50}"
    )
    return summary


# ─────────────────────────────────────────────
# 批量处理目录下所有视频
# ─────────────────────────────────────────────

def extract_batch(
    input_dir: str,
    config: ExtractConfig,
    num_workers: int = 4,
    extensions: Tuple[str,...] = (".mp4", ".mkv", ".ts", ".mov", ".avi"),
) -> List[ExtractSummary]:
    """批量处理目录下所有视频文件"""
    videos = [
        str(p) for p in Path(input_dir).rglob("*")
        if p.suffix.lower() in extensions
    ]
    if not videos:
        logger.warning(f"目录 {input_dir} 下未找到视频文件")
        return []

    logger.info(f"找到 {len(videos)} 个视频文件，开始批量处理...")
    summaries = []
    for i, video in enumerate(videos, 1):
        logger.info(f"\n[{i}/{len(videos)}] 处理: {video}")
        # 每个视频输出到独立子目录
        sub_cfg = ExtractConfig(**config.__dict__)
        sub_cfg.output_dir = os.path.join(config.output_dir, Path(video).stem)
        try:
            s = extract_local_video(video, sub_cfg, num_workers)
            summaries.append(s)
        except Exception as e:
            logger.error(f"  处理失败: {e}")

    return summaries


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Ray + H.265 视频流抽帧工具",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input",      type=str, help="视频文件路径 或 RTSP/HTTP 流地址")
    parser.add_argument("--input_dir",  type=str, help="批量处理：视频目录")
    parser.add_argument("--output",     type=str, default="./frames", help="帧输出目录")
    parser.add_argument("--fps",        type=float, default=1.0, help="每秒抽取帧数（0=全部帧）")
    parser.add_argument("--max_frames", type=int,   default=0,   help="最大帧数（0=不限制）")
    parser.add_argument("--format",     type=str,   default="jpg", choices=["jpg","png"], help="输出图片格式")
    parser.add_argument("--quality",    type=int,   default=90,   help="JPEG 质量 (1-100)")
    parser.add_argument("--resize",     type=str,   default=None, help="缩放尺寸，格式: 1280x720")
    parser.add_argument("--workers",    type=int,   default=4,    help="Ray worker 数量（本地文件）")
    parser.add_argument("--chunk",      type=float, default=10.0, help="分块大小（秒）")
    parser.add_argument("--live",       action="store_true",      help="直播流模式")
    parser.add_argument("--live_dur",   type=float, default=60.0, help="直播抽帧持续时间（秒）")
    parser.add_argument("--ray_addr",   type=str,   default=None, help="Ray 集群地址（默认本地启动）")

    args = parser.parse_args()

    # 解析 resize
    resize = None
    if args.resize:
        w, h = args.resize.lower().split("x")
        resize = (int(w), int(h))

    config = ExtractConfig(
        output_dir=args.output,
        target_fps=args.fps,
        max_frames=args.max_frames,
        image_format=args.format,
        jpeg_quality=args.quality,
        resize=resize,
        chunk_seconds=args.chunk,
    )

    # 初始化 Ray
    if args.ray_addr:
        ray.init(address=args.ray_addr)
        logger.info(f"已连接到 Ray 集群: {args.ray_addr}")
    else:
        ray.init(ignore_reinit_error=True)
        logger.info("已在本地启动 Ray")

    try:
        if args.input_dir:
            extract_batch(args.input_dir, config, args.workers)
        elif args.input:
            if args.live:
                extract_live_stream(args.input, config, args.live_dur)
            else:
                extract_local_video(args.input, config, args.workers)
        else:
            parser.print_help()
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()