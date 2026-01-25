#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0

import datetime
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
import logging
import sys
import json
import io
from loguru import logger
import gzip

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger.add('get_mcap_file_{time}.log', rotation='00:00', retention='300 days')


# Define your COS credentials and region
secret_id = 'xxxxx'  # Replace with your SecretId
secret_key = 'xxxxx'  # Replace with your SecretKey
region = 'ap-xxx'  # Replace with your region
token = None  # Using temporary key, if not, set to None
scheme = 'https'
# Initialize the COS client
config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token, Scheme=scheme)
client = CosS3Client(config)
logger.info("cos client init success")


# Define the bucket name and object key
bucket_name = 'xx-xxx-xxx-xxx-xxxx'
upload_bucket_name = "xxxxx"


def date_str():
    current_datetime = datetime.datetime.now()
    datetime_str = current_datetime.strftime('%Y%m%d')
    return datetime_str


def upload_dictdata_gz_to_cos(dict_data, filename, topic):
    """
    将字典类型数据上传到cos对应的文件中，json.gz格式
    """
    try:
        # 使用BytesIO和gzip将JSON数据压缩
        compressed_data = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed_data, mode='w') as gz_file:
            gz_file.write(json.dumps(dict_data).encode('utf-8'))

        # 确保将指针移回文件开头
        compressed_data.seek(0)

        response = client.put_object(
            Bucket=upload_bucket_name,
            Body=compressed_data.getvalue(),
            Key='{}/{}/{}/{}'.format('DataSight',date_str(), topic, filename),
            StorageClass='STANDARD'
        )
        logger.info("{} write dict data to cos success".format(filename))
    except Exception as e:
        logger.error(e)


def upload_dictdata_to_cos(dict_data, filename):
    """
    将字典类型数据上传到cos对应的文件中，json格式
    """
    try:
        response = client.put_object(
            Bucket=upload_bucket_name,
            Body=json.dumps(dict_data),
            #LocalFilePath='{}/{}'.format('data', filename),
            Key='{}/{}/{}'.format('DataSight','function-vehicle_in-mcu_data_10ms',filename),
            StorageClass='STANDARD',
            ContentType='application/json; charset=utf-8'
        )
        logger.info("{} write dict data to cos success".format(filename))
    except Exception as e:
        logger.error(e)


def upload_cosfile_from_local(filename):
    """
    将本地的文件上传到cos对应的文件中
    """
    # '{}/{}.json'.format(json_directory,json_name)
    try:
        response = client.upload_file(
            Bucket=upload_bucket_name,
            LocalFilePath='{}/{}'.format('fault-fim_data', filename),
            Key='DataSight/{}'.format(filename)
        )
        logger.info("{} update local file to cos success".format(filename))
    except Exception as e:
        print(e)


def download_cosfile_to_local(cos_directory, local_directory, filename):
    """
    将cos文件下载到本地
    """
    try:
        response = client.download_file(
            Bucket=bucket_name,
            # Key='upload/mcap/FY1/HJNFBABN2RP000330/20240903/20240903T090354.mcap',
            Key='{}/{}'.format(cos_directory, filename),
            #DestFilePath='20240903T090354.mcap'
            DestFilePath = '{}/{}'.format(local_directory, filename)
        )
    except Exception as e:
        print(f"Error read file: {e}")
