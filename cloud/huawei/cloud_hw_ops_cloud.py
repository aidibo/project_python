#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0


import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from obs import ObsClient
from tqdm import tqdm
from typing import List, Dict, Optional
import csv
from datetime import datetime
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('obs_parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class HuaweiOBSParser:
    """华为OBS解析器（企业级）"""

    def __init__(self, access_key: str, secret_key: str, server: str,
                 bucket_name: str, prefix: str = ""):
        """初始化OBS客户端"""
        try:
            self.client = ObsClient(
                access_key=access_key,
                secret_key=secret_key,
                server=server
            )
            self.bucket_name = bucket_name
            self.prefix = prefix
            self.results = []
            logger.info(f"✅ OBS客户端初始化成功")
        except Exception as e:
            logger.error(f"❌ OBS客户端初始化失败: {e}")
            raise

    def list_json_objects(self) -> List[str]:
        """列出所有JSON对象"""
        try:
            json_objects = []
            marker = ""  # 用于分页

            while True:
                response = self.client.list_objects(
                    bucket_name=self.bucket_name,
                    prefix=self.prefix,
                    marker=marker,
                    max_keys=1000
                )

                if response.status != 200:
                    logger.error(f"列出对象失败: {response.error_message}")
                    break

                # 收集JSON文件
                for obj in response.body.contents:
                    if obj.key.endswith('.json'):
                        json_objects.append(obj.key)

                # 检查是否还有更多对象
                if response.body.is_truncated:
                    marker = response.body.next_marker
                else:
                    break

            logger.info(f"✅ 找到 {len(json_objects)} 个JSON文件")
            return json_objects

        except Exception as e:
            logger.error(f"❌ 列出对象失败: {e}")
            return []

    def read_json_from_obs(self, object_key: str) -> Dict:
        """从OBS读取单个JSON文件"""
        try:
            response = self.client.getObject(
                bucketName=self.bucket_name,
                objectKey=object_key,
                loadStreamInMemory=True
            )

            if response.status != 200:
                logger.warning(f"读取失败 {object_key}: HTTP {response.status}")
                return {
                    'key': object_key,
                    'name': None,
                    'status': 'error',
                    'error': f"HTTP {response.status}"
                }

            # 读取和解析
            # content = response.body.read().decode('utf-8')
            # resp.body.buffer 是 bytes 类型
            content = response.body.buffer.decode('utf-8')
            data = json.loads(content)

            return {
                'key': object_key,
                'name': data.get('name', 'Unknown'),
                'data': data,
                'status': 'success',
                'size': len(content)
            }

        except json.JSONDecodeError as e:
            logger.warning(f"JSON解析失败 {object_key}: {e}")
            return {
                'key': object_key,
                'name': None,
                'status': 'json_error',
                'error': str(e)
            }
        except Exception as e:
            logger.warning(f"读取失败 {object_key}: {e}")
            return {
                'key': object_key,
                'name': None,
                'status': 'error',
                'error': str(e)
            }

    def parse_all_files(self, max_workers: int = 10) -> List[Dict]:
        """多线程解析所有JSON文件"""
        logger.info(f"开始列出对象...")
        json_objects = self.list_json_objects()

        if not json_objects:
            logger.error("未找到JSON文件")
            return []

        logger.info(f"使用 {max_workers} 个线程处理...")

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.read_json_from_obs, obj): obj
                for obj in json_objects
            }

            with tqdm(total=len(futures), desc="处理进度", unit="files") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    self.results.append(result)
                    pbar.update(1)

        elapsed = time.time() - start_time
        logger.info(f"✅ 处理完成，耗时 {elapsed:.2f} 秒")

        return self.results

    def export_to_json(self, output_file: str = "obs_results.json"):
        """导出为JSON"""
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ JSON文件已保存: {output_file}")
        except Exception as e:
            logger.error(f"❌ 保存JSON失败: {e}")

    def export_to_csv(self, output_file: str = "obs_results.csv"):
        """导出为CSV"""
        try:
            success_results = [r for r in self.results if r['status'] == 'success']

            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Key', 'Name', 'Size'])
                for result in success_results:
                    writer.writerow([result['key'], result['name'], result.get('size', 0)])

            logger.info(f"✅ CSV文件已保存: {output_file}")
        except Exception as e:
            logger.error(f"❌ 保存CSV失败: {e}")


    def get_statistics(self) -> Dict:
        """获取统计信息"""
        success = [r for r in self.results if r['status'] == 'success']
        errors = [r for r in self.results if r['status'] != 'success']

        return {
            'total': len(self.results),
            'success': len(success),
            'error': len(errors),
            'success_rate': f"{len(success)/len(self.results)*100:.2f}%" if self.results else "0%",
            'names': [r['name'] for r in success],
            'total_size': sum(r.get('size', 0) for r in success)
        }

    def print_report(self):
        """打印完整报告"""
        stats = self.get_statistics()

        report = f"""
╔═══════════════════════════════════════════════════════════════════╗
║          华为OBS JSON文件解析报告                                  ║
║          {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                               ║
╚═══════════════════════════════════════════════════════════════════╝

📊 统计信息:
  ✅ 总文件数:     {stats['total']}
  ✅ 成功:        {stats['success']}
  ❌ 失败:        {stats['error']}
  📈 成功率:      {stats['success_rate']}
  💾 总大小:      {stats['total_size'] / 1024:.2f} KB

📝 Name值列表 (前30个):
"""
        for i, name in enumerate(stats['names'][:30], 1):
            report += f"  {i:3d}. {name}\n"

        if len(stats['names']) > 30:
            report += f"  ... 共 {len(stats['names'])} 个name值\n"

        # 失败文件列表
        errors = [r for r in self.results if r['status'] != 'success']
        if errors:
            report += f"\n❌ 失败文件 (前10个):\n"
            for error in errors[:10]:
                report += f"  - {error['key']}: {error.get('error', '未知')}\n"

        report += f"\n{'═'*67}\n"

        print(report)
        logger.info(report)


# 使用示例
if __name__ == "__main__":
    # 配置信息
    config = {
        'access_key': 'your_access_key',  # 修改为实际的AK
        'secret_key': 'your_secret_key',  # 修改为实际的SK
        'server': 'obs.cn-east-3.myhuaweicloud.com',  # 修改为实际的端点
        'bucket_name': 'your_bucket',  # 修改为实际的桶名
        'prefix': 'tags/'  # OBS中的目录路径
    }

    try:
        # 创建解析器
        parser = HuaweiOBSParser(**config)

        # 解析所有文件
        results = parser.parse_all_files(max_workers=10)

        # 打印报告
        parser.print_report()

        # 导出结果
        parser.export_to_json("obs_results.json")
        parser.export_to_csv("obs_results.csv")

    except Exception as e:
        logger.error(f"执行出错: {e}")