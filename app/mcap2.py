#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0

import logging
import sys
import time

from kafka import KafkaConsumer
import json
#from adwsdk.adw_client import AdwClient
#from adwsdk import cmm
from loguru import logger
import pymysql
from pymysql.cursors import DictCursor

import json
import pika

# 正常情况日志级别使用 INFO，需要定位时可以修改为 DEBUG，此时 SDK 会打印和服务端的通信信息
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger.add('get_mcap_file_{time}.log', rotation='00:00', retention='300 days')

#adw_client = AdwClient(env='stg', adw_user='jiafa.zhang.o', adw_pass='UUA2VEEV9Z')
# 获取mysql连接 = (
conn = pymysql.connect(host='10.161.160.79', port=3306, user='root', password='123456', db='test', charset='utf8')
# conn = pymysql.connect(host='127.0.0.1', port=3306, user='dibo', password='dibo', db='test', charset='utf8')

connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.161.160.79', heartbeat=360))
#connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1', heartbeat=360))

channel = connection.channel()

def rabbitmq_publish_message(message):
    channel.exchange_declare('stg_snapshot_exchange', exchange_type='direct')
    channel.queue_declare(queue='stg_snapshot_queue')
    channel.queue_bind(exchange='stg_snapshot_exchange', queue='stg_snapshot_queue', routing_key='stg_snapshot_queue')
    # 将消息转换为JSON格式
    message_body = json.dumps(message)
    channel.basic_publish(exchange='stg_snapshot_exchange',
                          routing_key='stg_snapshot_queue',
                          body= message_body)


def concatenate_numbers(year, month, day, hour, minute):
    return str(f"{year:04d}{month:02d}{day:02d}{hour:02d}{minute:02d}")

def consumer_json_from_kafka(topic):
    # topic_name="aa-stg-production-fy-dlb-data"
    group_id="dibo888-test-consumer"
    servers=['10.134.176.16:9092']

    client=KafkaConsumer(
        topic,
        group_id=group_id,
        bootstrap_servers=servers,
        auto_commit_interval_ms=1000,
        auto_offset_reset='latest'
        #auto_offset_reset='earliest'
    )

    # 消费模块
    try:
        for msg in client:
            message_value = msg.value.decode('utf-8')
            json_data = json.loads(message_value)
            #print(f'Received message: {json_data["uuid"]}')
            #print(f'Received message: {json_data}')
            namespace = json_data["namespace"]
            if namespace == "production/fy/snapshot":
                print(f'Received message: {json_data["uuid"]}')
                # print(f'Received message: {json_data}')
                # 'namespace' in ('production/fy/snapshot', 'production/fy/log')
                uuid = json_data["uuid"]
                if uuid is not None:
                    #download_url, dict_prefix_msg = get_download_url(uuid)
                    dict_prefix_msg = get_download_url(uuid)
                    #tmp_row_message = parse_pb_from_url(download_url, dict_prefix=dict_prefix)
                    if dict_prefix_msg:
                        #send to rabbitmq
                        rabbitmq_publish_message(dict_prefix_msg)
    except KeyboardInterrupt as e:
            print (e)
            sys.exit(1)


def get_download_url(uuid):
    table_name = 'production/fy/snapshot/datafiles'
    #uuid = 'aa43c261-5e34-481f-8f97-4953838c65e5'
    where = "uuid='{uuid}' and file_type='mcap'".format(uuid=uuid)
    #scan = cmm.Scan(table_name=table_name, where=where, limit=10)
    #scan_result_iterator = adw_client.scan(scan)
    scan_result_iterator = []
    url = ''
    dict_prefix = {}
    for scan_row in scan_result_iterator:
        # adw 自动生成唯一键
        #print(scan_row.rowkey)
        #url = adw_client.generate_download_url(scan_row)
        download_url = "https://bit.nioint.com/adw-stg/FyADWTableForSnapshot/" + scan_row.rowkey
        #print(url)
        date_str = str(scan_row.meta["date"])
        dict_prefix = {"rowkey": scan_row.rowkey, "obj_key": scan_row.meta["file_path"],  "vin": scan_row.meta["vehicle_id"],
                       "vehicle_uuid": scan_row.meta["vehicle_uuid"], "vid": scan_row.meta["vehicle_id"],
                       #"date_str": scan_row.meta["date"],
                       "date_str": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
                       'mcap_file': scan_row.meta["file_name"], 'uuid': scan_row.meta["uuid"],
                       'url':url, 'download_url': download_url,
                       'file_size': scan_row.meta["data_size"]}
    #return download_url, dict_prefix
    #print(dict_prefix)
    return dict_prefix


if __name__ == '__main__':
    topic_name = "aa-stg-production-fy-dlb-data"
    consumer_json_from_kafka(topic_name)