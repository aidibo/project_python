#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0

import logging
import sys
import json
from loguru import logger
import pika

# 正常情况日志级别使用 INFO，需要定位时可以修改为 DEBUG，此时 SDK 会打印和服务端的通信信息
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger.add('get_mcap_file_{time}.log', rotation='00:00', retention='300 days')

connection = pika.BlockingConnection(pika.ConnectionParameters(host='xxxx.xxx.xxxx.79', heartbeat=360))
channel = connection.channel()
channel.exchange_declare('mcapfile_exchange', exchange_type='direct')
channel.queue_declare(queue='mcapfile_queue')
channel.queue_bind(exchange='mcapfile_exchange', queue='mcapfile_queue', routing_key='mcapfile_queue')


def rabbitmq_publish_message(message):
    # 将消息转换为JSON格式
    message_body = json.dumps(message)
    channel.basic_publish(exchange='mcapfile_exchange',
                          routing_key='mcapfile_queue',
                          body=message_body)


def send_message():
    rabbitmq_publish_message("test_rabbitmq")
    channel.close()
    connection.close()





if __name__ == '__main__':
    send_message()
