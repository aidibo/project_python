#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple

from obs import ObsClient, GetObjectRequest

# ==================== 配置区 ====================
BUCKET_NAME = "your-bucket-name"                    # 替换为你的桶名
PREFIX = "tags/"                                    # tags 目录（注意以 / 结尾）
MAX_WORKERS = 60                                    # 多线程数（网络带宽充足时可设 50~100）
REGION_ENDPOINT = "https://obs.cn-north-4.myhuaweicloud.com"  # 根据你的区域修改

# AK/SK（强烈建议使用环境变量或配置文件，不要硬编码到代码中）
AK = "YOUR_ACCESS_KEY_ID"
SK = "YOUR_SECRET_ACCESS_KEY"
# 如果使用临时凭证，增加 security_token 参数
# SECURITY_TOKEN = "your-security-token"

# ===============================================

def load_json_name_from_obs(object_key: str, obs_client: ObsClient) -> Tuple[str, str]:
    """从 OBS 直接读取字节流并解析 name 字段"""
    try:
        # 使用 loadStreamInMemory=True 直接把内容加载到内存
        get_req = GetObjectRequest()
        resp = obs_client.getObject(
            bucketName=BUCKET_NAME,
            objectKey=object_key,
            getObjectRequest=get_req,
            loadStreamInMemory=True   # 关键参数：加载到内存，返回 buffer
        )

        if resp.status < 300:  # 请求成功
            # resp.body.buffer 是 bytes 类型
            content_bytes = resp.body.buffer
            data = json.loads(content_bytes.decode('utf-8'))

            # 提取 name（根据你的 JSON 结构调整）
            name = None
            if isinstance(data, dict):
                name = (data.get('name') or
                       data.get('Name') or
                       data.get('NAME') or
                       (data.get('metadata', {}).get('name') if isinstance(data.get('metadata'), dict) else None))

            return object_key, name if name is not None else "N/A"

        else:
            return object_key, f"HTTP_ERROR_{resp.status}"

    except json.JSONDecodeError:
        return object_key, "JSON_PARSE_ERROR"
    except Exception as e:
        return object_key, f"ERROR: {str(e)[:100]}"


def main():
    start_time = time.time()

    # 创建 ObsClient（每个线程可复用同一个 client，OBS SDK 线程安全）
    obs_client = ObsClient(
        access_key_id=AK,
        secret_access_key=SK,
        server=REGION_ENDPOINT
        # security_token=SECURITY_TOKEN   # 如需临时凭证则取消注释
    )

    print(f"正在列出 {BUCKET_NAME}/{PREFIX} 下的 JSON 文件...")

    # 列出所有 .json 文件
    object_keys: List[str] = []
    marker = None
    while True:
        resp = obs_client.listObjects(
            bucketName=BUCKET_NAME,
            prefix=PREFIX,
            marker=marker,
            maxKeys=1000
        )

        if resp.status >= 300:
            print(f"列出对象失败: {resp.errorCode} - {resp.errorMessage}")
            obs_client.close()
            return

        for content in resp.body.contents:
            if content.key.endswith('.json'):
                object_keys.append(content.key)

        marker = resp.body.nextMarker
        if not marker:
            break

    print(f"共找到 {len(object_keys)} 个 JSON 文件，开始多线程读取字节流解析...")

    results: List[Tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有任务（每个任务传入同一个 obs_client）
        future_to_key = {
            executor.submit(load_json_name_from_obs, key, obs_client): key
            for key in object_keys
        }

        for future in as_completed(future_to_key):
            object_key, name = future.result()
            results.append((object_key, name))

            if len(results) % 100 == 0:
                print(f"已完成 {len(results)} / {len(object_keys)}")

    # 关闭客户端
    obs_client.close()

    # 输出结果示例
    print("\n=== 解析完成 ===")
    for key, name in results[:15]:
        filename = key.split('/')[-1]
        print(f"{filename:<50} → {name}")

    success_count = sum(1 for _, n in results if not n.startswith('ERROR') and n != 'N/A' and n != 'JSON_PARSE_ERROR')
    print(f"\n总耗时: {time.time() - start_time:.2f} 秒")
    print(f"成功解析: {success_count} / {len(results)}")

    # 保存结果到 CSV
    import csv
    with open('obs_tags_name_result.csv', 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['object_key', 'name'])
        writer.writerows(results)

    print("结果已保存到 obs_tags_name_result.csv")


if __name__ == "__main__":
    main()
