import os
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tqdm import tqdm
from typing import List, Dict
import time
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('json_parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class HuaweiTagsParser:
    """华为云Tags解析器"""

    def __init__(self, tags_dir: str, max_workers: int = 10, batch_size: int = 100):
        self.tags_dir = tags_dir
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.results = []

    def read_json_file(self, file_path: str) -> Dict:
        """读取JSON文件"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                name = data.get("name")
                return {
                    'file': os.path.basename(file_path),
                    'name': data.get('name', 'Unknown'),
                    'full_data': data,
                    'name': name,
                    'status': 'success'
                }
        except Exception as e:
            logger.error(f"读取文件失败 {file_path}: {e}")
            return {
                'file': os.path.basename(file_path),
                'name': None,
                'status': 'error',
                'error': str(e)
            }

    def parse_files(self) -> List[Dict]:
        """多线程解析文件"""
        json_files = sorted(list(Path(self.tags_dir).glob('*.json')))

        if not json_files:
            logger.warning(f"未找到JSON文件: {self.tags_dir}")
            return []

        logger.info(f"开始处理 {len(json_files)} 个文件")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.read_json_file, str(f)): f
                for f in json_files
            }

            with tqdm(total=len(futures), desc="处理进度", unit="files") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    self.results.append(result)
                    pbar.update(1)

        logger.info(f"处理完成，共 {len(self.results)} 个文件")
        return self.results

    def get_statistics(self) -> Dict:
        """获取统计信息"""
        success = [r for r in self.results if r['status'] == 'success']
        errors = [r for r in self.results if r['status'] != 'success']

        return {
            'total': len(self.results),
            'success': len(success),
            'error': len(errors),
            'success_rate': f"{len(success)/len(self.results)*100:.2f}%",
            'names': [r['name'] for r in success]
        }

    def export_to_csv(self, output_file: str = "tags.csv"):
        """导出为CSV"""
        import csv

        success = [r for r in self.results if r['status'] == 'success']

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['File', 'Name'])
            for result in success:
                writer.writerow([result['file'], result['name']])

        logger.info(f"CSV文件已保存: {output_file}")

    def export_to_json(self, output_file: str = "tags.json"):
        """导出为JSON"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)

        logger.info(f"JSON文件已保存: {output_file}")

    def print_report(self):
        """打印报告"""
        stats = self.get_statistics()

        print("\n" + "=" * 70)
        print(f"📊 华为云Tags解析报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        print(f"✅ 总文件数: {stats['total']}")
        print(f"✅ 成功: {stats['success']}")
        print(f"❌ 失败: {stats['error']}")
        print(f"📈 成功率: {stats['success_rate']}")
        print("\n📝 Name值列表 (前20个):")
        for i, name in enumerate(stats['names'][:20], 1):
            print(f"   {i:3d}. {name}")

        if len(stats['names']) > 20:
            print(f"   ... 共 {len(stats['names'])} 个name值")
        print("=" * 70 + "\n")


# 使用示例
if __name__ == "__main__":
    start_time = time.time()

    # 创建解析器
    import os
    cpu_count = os.cpu_count()  # 获取CPU核心数
    # 推荐设置
    max_workers = cpu_count * 2  # 通常为 16-32

    parser = HuaweiTagsParser(
        tags_dir="/Users/zhangjiafa/PycharmProjects/github/project_python/cloud/huawei/data",
        max_workers=max_workers
    )

    # 解析文件
    parser.parse_files()

    # 打印报告
    parser.print_report()

    # 导出结果
    parser.export_to_json("tags_results.json")
    parser.export_to_csv("tags_results.csv")

    elapsed = time.time() - start_time
    logger.info(f"总耗时: {elapsed:.2f} 秒")