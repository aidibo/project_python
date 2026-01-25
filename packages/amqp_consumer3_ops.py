#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0

import pika
from loguru import logger
import json


def main_loop_rabbitmq():
    #connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.161.160.79', heartbeat=360))
    #channel = connection.channel()
    #channel.queue_declare(queue='stg_mcapfile_bigtopics_queue', durable=True, passive=True)
    #channel.basic_qos(prefetch_count=5)

    def callback(ch, method, properties, body):
        global ids_list
        logger.info("body: = %r" % (body))
        dict_data = json.loads(body)
        mysql_id = dict_data["mysql_id"]
        if mysql_id is not None:
            ids_list.append(mysql_id)
            print(" [x] Received {}".format(mysql_id))

        if len(ids_list) >= BATCH_SIZE:
            ch.basic_ack(delivery_tag=method.delivery_tag, multiple=True)
            ids_str = ', '.join(map(str, ids_list))
            print("ids_str %s" % (ids_str))
            # do_work
            # get_mcap_file(g_big_topic_ids, ids_str)
            #print("ids_str %s" % (ids_str))
            ids_list = []  # 清空消息列表
            #print(ids_str)

    channel.basic_consume(queue='stg_mcapfile_bigtopics_queue',
                          auto_ack=False,
                          on_message_callback=callback)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    g_big_topic_ids = [
        'function/fct/fct_debug_out',
        'perception/freespace_fusion-6v',
        'function/parking/zm_acore_sdk_internal_debug'
    ]

    # 批量消费的数量
    BATCH_SIZE = 8
    ids_list = []
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='xxxx.xxx.xxxx.79', heartbeat=360))
    channel = connection.channel()
    channel.queue_declare(queue='stg_mcapfile_bigtopics_queue', durable=True, passive=True)
    channel.basic_qos(prefetch_count=8)
    main_loop_rabbitmq()