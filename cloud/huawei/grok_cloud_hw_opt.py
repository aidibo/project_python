import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple

from obs import ObsClient

# ==================== 配置区 ====================
BUCKET_NAME = "your-bucket-name"                    # 替换为你的桶名
PREFIX = "tags/"                                    # tags 目录（注意以 / 结尾）
MAX_WORKERS = 30                                    # 重要调整：7MB 文件，建议 20~40（太高会占用过多内存和带宽）
REGION_ENDPOINT = "https://obs.cn-north-4.myhuaweicloud.com"  # 根据你的区域修改

AK = "YOUR_ACCESS_KEY_ID"
SK = "YOUR_SECRET_ACCESS_KEY"
# SECURITY_TOKEN = "your-security-token"            # 如使用临时凭证则取消注释

# ===============================================

def load_json_name_from_obs(object_key: str, obs_client: ObsClient) -> Tuple[str, str]:
    """从 OBS 以流式方式读取 JSON 并提取 name 字段（适合 7MB 文件）"""
    try:
        # 不设置 loadStreamInMemory=True，使用 streaming 模式
        resp = obs_client.getObject(
            bucketName=BUCKET_NAME,
            objectKey=object_key
            # loadStreamInMemory 默认 False，返回可读流
        )

        if resp.status >= 300:
            return object_key, f"HTTP_ERROR_{resp.status}"

        # resp.body 是可读的流对象
        stream = resp.body
        # 一次性读取全部内容（7MB 对现代机器来说很小，但比 loadStreamInMemory 更可控）
        content_bytes = stream.read()
        stream.close()                    # 及时关闭流

        data = json.loads(content_bytes.decode('utf-8'))

        # 提取 name 值（根据你的实际 JSON 结构调整）
        name = None
        if isinstance(data, dict):
            name = (data.get('name') or
                   data.get('Name') or
                   data.get('NAME') or
                   (data.get('metadata', {}).get('name') if isinstance(data.get('metadata'), dict) else None))

        return object_key, name if name is not None else "N/A"

    except json.JSONDecodeError:
        return object_key, "JSON_PARSE_ERROR"
    except Exception as e:
        return object_key, f"ERROR: {str(e)[:120]}"


def main():
    start_time = time.time()

    obs_client = ObsClient(
        access_key_id=AK,
        secret_access_key=SK,
        server=REGION_ENDPOINT
        # security_token=SECURITY_TOKEN
    )

    print(f"正在列出 {BUCKET_NAME}/{PREFIX} 下的 JSON 文件...")

    # 列出所有 .json 文件（支持分页）
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

    total_files = len(object_keys)
    print(f"共找到 {total_files} 个 JSON 文件（每个约 7MB），开始多线程流式解析...")

    results: List[Tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_key = {
            executor.submit(load_json_name_from_obs, key, obs_client): key
            for key in object_keys
        }

        for i, future in enumerate(as_completed(future_to_key), 1):
            object_key, name = future.result()
            results.append((object_key, name))

            if i % 50 == 0 or i == total_files:
                print(f"已完成 {i} / {total_files}  ({i/total_files*100:.1f}%)")

    obs_client.close()

    # 输出结果
    print("\n=== 解析完成 ===")
    for key, name in results[:15]:
        filename = key.split('/')[-1]
        print(f"{filename:<55} → {name}")

    success_count = sum(1 for _, n in results
                       if not str(n).startswith('ERROR') and n not in ('N/A', 'JSON_PARSE_ERROR'))

    print(f"\n总耗时: {time.time() - start_time:.2f} 秒")
    print(f"成功解析: {success_count} / {total_files}")

    # 保存结果到 CSV
    import csv
    with open('obs_tags_name_result.csv', 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['object_key', 'name'])
        writer.writerows(results)

    print("结果已保存到 obs_tags_name_result.csv")


if __name__ == "__main__":
    main()
