#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0


import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from obs import ObsClient
from tqdm import tqdm
from typing import List, Dict


class HuaweiOBSJSONParser:
    """华为OBS JSON文件解析器"""

    def __init__(self, access_key: str, secret_key: str, server: str,
                 bucket_name: str, prefix: str = ""):
        """
        初始化OBS客户端

        参数:
            access_key: 华为云Access Key
            secret_key: 华为云Secret Key
            server: OBS服务端点，如 'obs.cn-east-3.myhuaweicloud.com'
            bucket_name: 桶名称
            prefix: 对象前缀（目录路径），如 'tags/'
        """
        self.client = ObsClient(
            access_key=access_key,
            secret_key=secret_key,
            server=server
        )
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.results = []

    def list_json_objects(self) -> List[str]:
        """列出OBS中指定前缀下的所有JSON对象"""
        try:
            response = self.client.list_objects(
                bucket_name=self.bucket_name,
                prefix=self.prefix,
                max_keys=1000  # 一次最多列出1000个对象
            )

            if response.status != 200:
                print(f"❌ 列出对象失败: {response.error_message}")
                return []

            json_objects = [
                obj.key for obj in response.body.contents
                if obj.key.endswith('.json')
            ]

            print(f"✅ 找到 {len(json_objects)} 个JSON文件")
            return json_objects

        except Exception as e:
            print(f"❌ 列出对象失败: {e}")
            return []

    def read_json_from_obs(self, object_key: str) -> Dict:
        """从OBS读取单个JSON文件"""
        try:
            # response = self.client.get_object(
            #     bucket_name=self.bucket_name,
            #     key=object_key
            # )
            response = self.client.getObject(
                bucketName=self.bucket_name,
                objectKey=object_key,
                loadStreamInMemory=True
            )

            if response.status != 200:
                return {
                    'key': object_key,
                    'name': None,
                    'status': 'error',
                    'error': f"HTTP {response.status}"
                }

            # 读取内容
            # content = response.body.read().decode('utf-8')
            # data = json.loads(content)

            # resp.body.buffer 是 bytes 类型
            content = response.body.buffer.decode('utf-8')
            data = json.loads(content)

            return {
                'key': object_key,
                'name': data.get('name', 'Unknown'),
                'status': 'success',
                'size': len(content)
            }

        except json.JSONDecodeError as e:
            return {
                'key': object_key,
                'name': None,
                'status': 'json_error',
                'error': str(e)
            }
        except Exception as e:
            return {
                'key': object_key,
                'name': None,
                'status': 'error',
                'error': str(e)
            }

    def parse_all_files(self, max_workers: int = 10) -> List[Dict]:
        """多线程解析所有JSON文件"""
        # 列出所有JSON对象
        json_objects = self.list_json_objects()

        if not json_objects:
            print("❌ 未找到JSON文件")
            return []

        print(f"⚙️  使用 {max_workers} 个线程处理...\n")

        # 多线程处理
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.read_json_from_obs, obj): obj
                for obj in json_objects
            }

            # 使用进度条
            with tqdm(total=len(futures), desc="处理进度", unit="files") as pbar:
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        self.results.append(result)

                        if result['status'] == 'success':
                            pbar.write(f"✅ {result['key']}: {result['name']}")
                        else:
                            pbar.write(f"❌ {result['key']}: {result['error']}")

                        pbar.update(1)
                    except Exception as e:
                        print(f"❌ 任务执行错误: {e}")

        return self.results

    def get_statistics(self) -> Dict:
        """获取统计信息"""
        success = [r for r in self.results if r['status'] == 'success']
        errors = [r for r in self.results if r['status'] != 'success']

        return {
            'total': len(self.results),
            'success': len(success),
            'error': len(errors),
            'success_rate': f"{len(success)/len(self.results)*100:.2f}%" if self.results else "0%",
            'names': [r['name'] for r in success]
        }

    def print_summary(self):
        """打印摘要"""
        stats = self.get_statistics()

        print("\n" + "=" * 70)
        print("📊 解析结果摘要")
        print("=" * 70)
        print(f"✅ 总文件数: {stats['total']}")
        print(f"✅ 成功: {stats['success']}")
        print(f"❌ 失败: {stats['error']}")
        print(f"📈 成功率: {stats['success_rate']}")

        print(f"\n📝 Name值 (前20个):")
        for i, name in enumerate(stats['names'][:20], 1):
            print(f"   {i:3d}. {name}")

        if len(stats['names']) > 20:
            print(f"   ... 共 {len(stats['names'])} 个name值")

        # 输出失败的文件
        if self.results:
            errors = [r for r in self.results if r['status'] != 'success']
            if errors:
                print(f"\n❌ 失败文件 (前10个):")
                for error in errors[:10]:
                    print(f"   - {error['key']}: {error.get('error', '未知错误')}")

        print("=" * 70 + "\n")


# 使用示例
if __name__ == "__main__":
    # 华为云凭证配置
    ACCESS_KEY = "your_access_key"
    SECRET_KEY = "your_secret_key"
    SERVER = "obs.cn-east-3.myhuaweicloud.com"  # 修改为实际的区域端点
    BUCKET = "your_bucket_name"
    PREFIX = "tags/"  # OBS中的目录路径

    # 创建解析器
    parser = HuaweiOBSJSONParser(
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        server=SERVER,
        bucket_name=BUCKET,
        prefix=PREFIX
    )

    # 解析所有文件
    results = parser.parse_all_files(max_workers=10)

    # 打印摘要
    parser.print_summary()

    # 保存结果到JSON
    with open("obs_tags_results.json", 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"✅ 结果已保存到 obs_tags_results.json")