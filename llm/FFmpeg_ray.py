#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0


# https://www.perplexity.ai/search/ffmpegyu-rayji-cheng-chou-qu-h-eQD84IMQT2ao1kTrqYlg7A
# dibozhang2023@gmail.com

"""

设计原则与整体架构

    用 FFmpeg 专注做解码和抽帧，Python/Ray 专注做并行计算与业务逻辑。

不要一次性把整段视频读进内存，而是用管道（pipe）/流式逐帧读取，边读边分发到 Ray。

把「拉流/解码」和「帧处理」拆成两个角色：

    解码器进程（FFmpeg 子进程或单独容器）

    Ray 集群上的 worker（task/actor），按帧或按小 batch 处理。

生产环境建议一个 Ray 集群跑一个主要工作 job，以避免多租户资源抢占，方便监控和调优。

    ​

FFmpeg 解码 H.265 的推荐命令

以本地文件为例（RTSP/HLS 只要把 -i input.mp4 换成你的 URL 即可），输出 raw BGR 或 RGB 帧给 Python：

bash
ffmpeg \
  -loglevel error \
  -hwaccel auto \
  -i input_hevc.mp4 \
  -an -vsync 0 \
  -pix_fmt bgr24 \
  -f rawvideo -

要点：

    -hwaccel auto：优先用 GPU/硬件解码（视环境支持情况），减轻 CPU 压力。

​

-f rawvideo -pix_fmt bgr24：输出未压缩像素，方便直接转成 numpy。分辨率为 W×HW×H，单帧字节数是 W×H×3W×H×3。

​

-vsync 0：按原始帧输出，避免重复/丢帧逻辑干扰抽帧策略。

    ​

Python + FFmpeg 管道读取帧

核心思路是 subprocess.Popen 打开 FFmpeg，把 stdout 当成字节流；每次 read(frame_bytes) 刚好拿到一帧，再交给 Ray。

python
import subprocess as sp
import ray
import numpy as np

VIDEO_W = 1920
VIDEO_H = 1080
PIX_FMT = "bgr24"
BYTES_PER_FRAME = VIDEO_W * VIDEO_H * 3  # 对应 pix_fmt=bgr24

ray.init(address="auto")  # 或本地 ray.init()

@ray.remote
def process_frame(frame_idx, frame_np):
    # 这里写你的业务逻辑，比如检测、OCR、编码等
    # 示例：返回平均亮度
    return frame_idx, float(frame_np.mean())

def start_ffmpeg_reader(input_url, sample_interval=1):
    cmd = [
        "ffmpeg",
        "-loglevel", "error",
        "-hwaccel", "auto",
        "-i", input_url,
        "-an", "-vsync", "0",
        "-pix_fmt", PIX_FMT,
        "-f", "rawvideo",
        "-"
    ]

    proc = sp.Popen(cmd, stdout=sp.PIPE, bufsize=BYTES_PER_FRAME * 4)
    frame_idx = 0
    pending = []

    while True:
        raw = proc.stdout.read(BYTES_PER_FRAME)
        if len(raw) < BYTES_PER_FRAME:
            break

        if frame_idx % sample_interval != 0:
            frame_idx += 1
            continue

        frame = np.frombuffer(raw, np.uint8).reshape((VIDEO_H, VIDEO_W, 3))

        # 把 numpy 转成 bytes 传给 Ray，可以减少对象序列化负担
        ref = process_frame.remote(frame_idx, frame)
        pending.append(ref)

        if len(pending) >= 128:
            _ = ray.get(pending)
            pending.clear()

        frame_idx += 1

    if pending:
        _ = ray.get(pending)

    proc.stdout.close()
    proc.wait()

if __name__ == "__main__":
    start_ffmpeg_reader("input_hevc.mp4", sample_interval=5)

要点：

    必须提前知道或探测到 W/H，可以先用一次 ffprobe 或 ffmpeg -i 拿元数据。

    bufsize 设置成至少几帧大小，减少系统调用，提升吞吐。

    ​

    sample_interval 控制抽帧频率（如 5 表示每 5 帧取 1 帧），尽量在 Python 层做，这样一个 FFmpeg 流可以支持多种抽帧策略。

Ray 端的结构与实践

在 Ray 这边可以有几种模式，推荐按吞吐和复杂度选择。
1. 简单模式：一帧一个 task（轻负载）

    适合：H.265 帧率不高、处理逻辑也比较轻（如简单统计、缩略图）。

    上面的示例就是这种模式：process_frame.remote(frame_idx, frame)。

    注意控制 pending 队列长度（比如 128～512），用 ray.get 回收，避免内存炸掉。

2. 批处理模式：一批多帧一个 task（IO/CPU 重处理）

    适合：复杂算法（检测、识别、重编码）占主导时，可以提高 CPU 利用率。

    做法是在 Python 这边先把帧打包成 list，每 N 帧提交一个任务：

python
BATCH_SIZE = 16
batch = []
batch_idx = 0

while True:
    raw = proc.stdout.read(BYTES_PER_FRAME)
    if len(raw) < BYTES_PER_FRAME:
        break

    if frame_idx % sample_interval == 0:
        frame = np.frombuffer(raw, np.uint8).reshape((H, W, 3))
        batch.append((frame_idx, frame))

        if len(batch) == BATCH_SIZE:
            ref = process_batch.remote(batch_idx, batch)
            pending.append(ref)
            batch_idx += 1
            batch = []
    frame_idx += 1

3. Actor 模式：每路流一个解码 Actor

    多路 H.265 流时，建议用 Ray actor 把「解码+分发」封装为服务：

        每个 actor 内部起 FFmpeg 子进程，读取某一路流；

        actor 内部把帧再分发给其它 worker task/actor。

    好处：

        易于按路数水平扩展；

        每个流的状态、统计（PTS、丢帧、重连）集中在 actor 内维护。

性能与稳健性最佳实践

    解码层：

        尽量启用硬件解码（NVIDIA NVDEC、Intel QSV、VAAPI 等）。

    ​

    对低延迟场景，关闭不必要的缓冲：-fflags nobuffer -flags low_delay 等（视实际需求调试）。

传输层：

    尽量用 rawvideo 管道，避免在 Python 中做重复解码。

    如果跨机器传输原始帧压力太大，可在 FFmpeg 端先缩放、裁剪、降帧：-vf scale=1280:-1,fps=5。

Ray 侧：

    使用 Prometheus/Grafana 或 Ray dashboard 监控 task 延迟、CPU/GPU 使用情况并做资源配额调优。

​

一个集群上建议只跑一个主 job，避免多 job 抢资源导致实时性波动。

        ​

推荐的职责划分表
部分	推荐工具/位置	说明
部分	推荐工具/位置	说明
H.265 解码	FFmpeg 子进程 / 容器	善用硬件解码，输出 rawvideo。
​
抽帧频率控制	Python 读帧循环	通过 sample_interval 灵活调整。
尺寸/预处理	优先用 FFmpeg -vf	如缩放、裁剪、fps 降采样。
​
并行任务调度	Ray task / actor	一帧或一批帧一个任务。
多路流管理	Ray actor per stream	内部维护 FFmpeg 子进程。
监控 & 扩缩容	Ray dashboard + 外部监控	基于 CPU/GPU/延迟做扩缩容。
​

如果你方便透露更多信息（比如：是离线文件还是 RTSP 实时流、单机还是多机集群、是否有 GPU、目标 QPS/帧率），我可以帮你直接写一版贴近你场景的 Ray actor + FFmpeg 集成模板。

"""