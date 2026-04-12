#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, get_json_object
import time

# 初始化Spark会话
spark = SparkSession.builder \
    .appName("ReadHuaweiCloudJSON") \
    .getOrCreate()

# ===== 方法一：直接读取JSON文件 =====
# 假设JSON文件在华为云OBS上，路径格式为：s3a://bucket-name/tags/*.json
# 或者本地路径：/path/to/tags/*.json

# 配置华为云OBS访问（如果需要）
# spark.hadoop.fs.s3a.access.key = "your_access_key"
# spark.hadoop.fs.s3a.secret.key = "your_secret_key"
# spark.hadoop.fs.s3a.endpoint = "obs.cn-east-2.myhuaweicloud.com"


def read_cloud_hw():
    json_path = ['/Users/zhangjiafa/PycharmProjects/github/project_python/cloud/huawei/data/test02.json',
                 '/Users/zhangjiafa/PycharmProjects/github/project_python/cloud/huawei/data/test.json',
                 '/Users/zhangjiafa/PycharmProjects/github/project_python/cloud/huawei/data/test.json'
                 ]

    #json_path = ['/opt/transaction.json', '/opt/transaction2.json', '/opt/transaction3.json']
    #json_path = ['/opt/transaction.json']
    # 读取所有文件
    '''
    df_final = spark.read \
        .option("multiline", "true") \
        .option("inferSchema", "true") \
        .option("mode", "PERMISSIVE") \
        .json(json_path)
    '''

    df_final = spark.read \
        .option("inferSchema", "true") \
        .option("mode", "PERMISSIVE") \
        .json(json_path)

    print(df_final.count())

    return df_final


def read_local():
    # 读取JSON文件
    #json_path = "s3a://your-bucket-name/tags/*.json"  # 修改为实际的华为云路径

    # 或本地路径
    # json_path = "/Users/zhangjiafa/PycharmProjects/github/project_python/cloud/huawei/data/*.json"  # 修改为实际的华为云路径
    json_path = "/Users/zhangjiafa/PycharmProjects/github/project_python/cloud/huawei/data"  # 修改为实际的华为云路径

    # 读取tags下所有子目录的JSON
    # json_path = "s3a://bucket/tags/*/*.json"
    # 或读取任意深度的JSON
    # json_path = "s3a://bucket/tags/**/*.json"

    df = spark.read.option("multiline", "true").json(json_path)

    # 显示读取的数据（检查结构）
    print("原始数据结构：")
    #df.printSchema()
    print("\n原始数据内容：")
    #df.show(truncate=False)
    return df


def exec_sql(df):
    # ===== 注册为临时表 =====
    df.createOrReplaceTempView("json_tags_table")
    # ===== 方法二：使用SQL解析JSON中的name值 =====
    # 如果JSON结构嵌套较深，可以用get_json_object提取
    df_with_name = spark.sql("""
        SELECT 
            *,
            _id as parsed_name,
            input_file_name(),
            element_at(split(input_file_name(),'/'),-1) AS filename
        FROM json_tags_table
    """)
    df_with_name.show(100, truncate=False)


if __name__ == '__main__':
    #df = read_local()
    df = read_cloud_hw()
    df.show(100, truncate=False)

    exec_sql(df=df)
    # time.sleep(1000)
    spark.stop()
