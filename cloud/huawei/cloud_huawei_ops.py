#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0


import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple
import time

# ==================== 配置区 ====================
# 请根据实际情况修改
BUCKET_NAME = "your-bucket-name"  # 华为云 OBS Bucket 名称
PREFIX = "tags/"  # tags 目录前缀（注意以 / 结尾）
MAX_WORKERS = 50  # 多线程数量，根据你的网络和机器性能调整（推荐 30~100）
LOCAL_DIR = None  # 如果文件已下载到本地，可填本地目录路径；否则用 OBS SDK 读取


# 如果使用本地文件（推荐先下载再处理，速度更快）
# LOCAL_DIR = r"C:\data\huawei_tags"      # 示例本地路径
# ===============================================

def load_json_name(file_path: str) -> Tuple[str, str]:
    """读取单个 JSON 文件并返回 (文件名, name值)"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 支持多种可能的 name 字段位置，根据你的 JSON 结构调整
        name = None
        if isinstance(data, dict):
            name = data.get('name') or data.get('Name') or data.get('NAME')
            # 如果 name 在嵌套结构中，可继续扩展，例如：
            # if not name and 'metadata' in data:
            #     name = data['metadata'].get('name')

        return file_path, name if name is not None else "N/A"

    except json.JSONDecodeError:
        return file_path, "JSON_PARSE_ERROR"
    except Exception as e:
        return file_path, f"ERROR: {str(e)}"


def main():
    start_time = time.time()

    file_paths: List[str] = []

    if LOCAL_DIR:
        # === 从本地目录读取 ===
        print(f"从本地目录读取: {LOCAL_DIR}")
        for filename in os.listdir(LOCAL_DIR):
            if filename.endswith('.json'):
                full_path = os.path.join(LOCAL_DIR, filename)
                file_paths.append(full_path)
    else:
        # === 从华为云 OBS 读取（需安装 obs-sdk）===
        print("从华为云 OBS 读取...")
        try:
            from obs import ObsClient

            obs_client = ObsClient(
                access_key_id='YOUR_AK',
                secret_access_key='YOUR_SK',
                server='https://obs.cn-north-4.myhuaweicloud.com'  # 根据你的区域修改
            )

            resp = obs_client.listObjects(BUCKET_NAME, prefix=PREFIX, maxKeys=1000)
            for content in resp.body.contents:
                if content.key.endswith('.json'):
                    # 注意：这里返回的是对象 key，需要后续下载或使用 getObject
                    # 推荐方式：先批量下载到本地再处理，效率更高
                    print(f"发现文件: {content.key} （建议先下载到本地）")

            print("OBS 模式下推荐先下载文件到本地再运行！")

            json_objects = [
                obj.key for obj in resp.body.contents
                if obj.key.endswith('.json')
            ]

            return json_objects

        except ImportError:
            print("未安装 obs 库，请先 pip install esdk-obs-python")
            return
        except Exception as e:
            print(f"OBS 连接错误: {e}")
            return

    print(f"共找到 {len(file_paths)} 个 JSON 文件，开始多线程解析...")

    results: List[Tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {executor.submit(load_json_name, path): path for path in file_paths}

        for future in as_completed(future_to_file):
            file_path, name = future.result()
            results.append((file_path, name))

            # 实时打印进度（可选）
            if len(results) % 100 == 0:
                print(f"已完成 {len(results)} / {len(file_paths)}")

    # 输出结果
    print("\n=== 解析完成 ===")
    for file_path, name in results[:10]:  # 只打印前10条示例
        print(f"{os.path.basename(file_path):<40} → {name}")

    if len(results) > 10:
        print(f"... 以及其余 {len(results)-10} 个文件")

    print(f"\n总耗时: {time.time() - start_time:.2f} 秒")
    print(f"成功解析文件数: {len([n for _, n in results if not n.startswith('ERROR') and n != 'N/A'])}")

    # 如果需要保存结果到 CSV
    import csv
    with open('tags_name_result.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['filename', 'name'])
        for file_path, name in results:
            writer.writerow([os.path.basename(file_path), name])


if __name__ == "__main__":
    main()