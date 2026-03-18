#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0


import logging
import sys
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client

from mcap_protobuf.decoder import DecoderFactory
from mcap.reader import make_reader
from google.protobuf.json_format import MessageToDict
import json

import pymysql
from pymysql.cursors import DictCursor
import io
from loguru import logger
import time
import argparse

from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col  # ,lit
from pyspark.sql.functions import col
import pika
import requests
import datetime

spark = SparkSession.builder.appName("stg_big_topics_etl").enableHiveSupport().getOrCreate()

# 正常情况日志级别使用 INFO，需要定位时可以修改为 DEBUG，此时 SDK 会打印和服务端的通信信息
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger.add('get_mcap_file_{time}.log', rotation='00:00', retention='300 days')

# Define your COS credentials and region
secret_id = 'xxxxxxxx'  # Replace with your SecretId
secret_key = 'xxxxxxx'  # Replace with your SecretKey
region = 'ap-xxxxxx'  # Replace with your region
token = None  # Using temporary key, if not, set to None
scheme = 'https'
# Initialize the COS client
config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token, Scheme=scheme)
client = CosS3Client(config)
logger.info("cos client init success")

# Define the bucket name and object key
bucket_name = 'p-fyad-xxxx-xxxx-xxxxx'

parser = argparse.ArgumentParser()
parser.add_argument('-vin')
parser.add_argument('-start')
parser.add_argument('-end')
parser.add_argument('-table_name')
args = parser.parse_args()
vin = args.vin
start_date = args.start
end_date = args.end
table_name = args.table_name


def concatenate_numbers(year, month, day, hour, minute):
    return str(f"{year:04d}{month:02d}{day:02d}{hour:02d}{minute:02d}")


def parse_pb_from_cos(file_path, topic_ids, dict_prefix):
    ####  获取文件到response
    try:
        response = client.get_object(
            Bucket=bucket_name,
            Key=file_path
        )
        file_content = response['Body'].get_raw_stream().read()
        # Convert the file content to a BinaryIO object
        binary_io = io.BytesIO(file_content)

        # 记录cost download cos 时间
        cost_download_cos = time.time() - dict_prefix["adventure_start_time"]
        dict_prefix["cost_download_cos"] = cost_download_cos

        # Use the binary_io object in a with statement
        with binary_io as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            # for topic_id in topic_ids:
            messages = []
            messages_rows = []
            sensor_data_dict = []
            # tmp_topic_ids = []
            # tmp_topic_ids.append(topic_id)
            # logger.info("tmp_topic_ids=%s" % (tmp_topic_ids))
            for schema, channel, message, proto_msg in reader.iter_decoded_messages(topics=topic_ids):
                # if channel.topic in topic_id:
                sensor_data_dict = MessageToDict(proto_msg, preserving_proto_field_name=True)
                sensor_data_dict['vin'] = dict_prefix['vin']
                sensor_data_dict['sw_id'] = dict_prefix['sw_id']
                sensor_data_dict['date_str'] = dict_prefix['date_str']
                sensor_data_dict['mcap_file'] = dict_prefix['mcap_file']
                sensor_data_dict['mysql_id'] = dict_prefix['mysql_id']
                sensor_data_dict['mcap_file_info'] = dict_prefix['obj_key']
                sensor_data_dict['topic_id'] = channel.topic
                messages.append(sensor_data_dict)
                messages_rows.append(Row(vin=dict_prefix['vin'], sw_id=dict_prefix['sw_id'],
                                         datetime=dict_prefix['date_str'], mcap_file=dict_prefix['mcap_file'],
                                         mysql_id=dict_prefix['mysql_id'], mcap_file_info=dict_prefix['obj_key'],
                                         publish_ts=sensor_data_dict['publish_ts'],
                                         topic=channel.topic.replace('/', '_'),
                                         json_str=json.dumps(sensor_data_dict, ensure_ascii=False)
                                         ))
            if messages:
                dict_prefix["record_count"] = len(messages_rows)
                print("The messages_rows time is", datetime.datetime.now())
                # messages_json = json.dumps(messages)
                # sync_df_to_iceberg_table_process(messages_json, topic_id.replace('/','_'))
                # flag = sync_df_to_iceberg_table_process(messages_rows, topic_id.replace('/', '_'))
                sync_df_ds_status(dict_prefix, 'big_topics')
                logger.info("mcap file sync to ds ==> id=%s,vin=%s,obj_key=%s,topid_id=%s" % (
                dict_prefix['mysql_id'], dict_prefix['vin'], file_path, 'big_topics'))
                return messages_rows
            else:
                # update mysql sync_flag_cos = 2 where vin id = ?  2记录为空
                sync_df_ds_empty(dict_prefix, 'big_topics')
                logger.info("mcap file is empty ==> id=%s,vin=%s,obj_key=%s,topid_id=%s" % (
                dict_prefix['mysql_id'], dict_prefix['vin'], file_path, 'big_topics'))
                return []
    except Exception as e:
        # 异常写入数据库
        # dict_prefix['error_status'] = '-9'
        sync_read_mcap_error(dict_prefix, 'big_topics')
        logger.exception(e)
        return []


def main_loop_rabbitmq():
    #connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.161.160.79', heartbeat=360))
    # connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1', heartbeat=360))
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
            get_mcap_file(g_big_topic_ids, ids_str)
            #print("ids_str %s" % (ids_str))
            ids_list = []  # 清空消息列表
            #print(ids_str)

    channel.basic_consume(queue='stg_mcapfile_bigtopics_queue',
                          auto_ack=False,
                          on_message_callback=callback)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def get_mcap_file(topic_ids, mysql_ids):
    record_count = 0
    # 连接数据库
    conn = pymysql.connect(host='xx.xx.xxx.xxx', port=3306, user='xxx', password='xxxx',
                           db='adventure_backend', charset='utf8')
    # 获取游标
    cursor = conn.cursor(DictCursor)

    # query_sql = f""" select * from mcap_data where date_str >= '2024-08-26' and date_str < '2024-08-27' order by id asc limit {i*batch_size}, {batch_size} """
    # query_sql = f""" select * from mcap_data where date_str >= '{start_date}' and date_str < '{end_date}' and size > 1 order by id asc limit {i*batch_size}, {batch_size} """
    query_sql = f""" select * from mcap_data where id in ( {mysql_ids} ) """
    logger.info(query_sql)
    cursor.execute(query_sql)
    # 获取结果
    result = cursor.fetchall()

    ds_df_row = []
    append_ids_list = []
    for row in result:
        mcap_file = row['obj_key'].replace('/upload/', 'upload/')
        logger.info("id=%s,obj_key=%s, vin=%s, sw_id=%s" % (row['id'], mcap_file, row['vin'], row['sw_id']))
        dict_prefix = {"mysql_id": row['id'], "obj_key": row['obj_key'], "vin": row['vin'], "sw_id": row['sw_id'],
                       "date_str": row['date_str'], 'mcap_file': row['file_name']}
        start_time = time.time()
        dict_prefix["topic_ids"] = topic_ids
        dict_prefix["adventure_start_time"] = start_time
        sync_flag = check_write_iceberg(dict_prefix, 'big_topics')
        if sync_flag:
            tmp_row = parse_pb_from_cos(mcap_file, topic_ids, dict_prefix)
            if tmp_row:
                append_ids_list.append(row['id'])
                ds_df_row.extend(tmp_row)
        end_time = time.time()
        logger.info("cost time: = %s" % (str(end_time - start_time)))
    # 关闭游标
    cursor.close()
    conn.close()

    if len(ds_df_row) >= 1:
        append_ids_str = ','.join(map(str, append_ids_list))
        print("append_ids_str: %s" % (append_ids_str))
        record_count = record_count + len(ds_df_row)
        df_source = spark.createDataFrame(ds_df_row)
        df = df_source.select(col("vin").alias("vin"), col("sw_id").alias("sw_id"), col("mcap_file").alias("mcap_file"),
                              col("mcap_file_info").alias("mcap_file_info"), col("publish_ts").alias("publish_ts"),
                              col("json_str").alias("json_str"),
                              col("topic").alias("topic"),
                              col("datetime").alias("datetime")
                              )
        df.createOrReplaceTempView("t_tmp")
        # spark.sql("select * from t_tmp").show()
        # spark.sql("select count(vin) as cnt from t_tmp").show()
        insert_sql = f""" insert into fy_dwm_stg.fy_ad_big_topics
                            select vin, sw_id, mcap_file, mcap_file_info,json_str,topic,publish_ts,datetime from t_tmp"""
        logger.info(insert_sql)
        try:
            spark.sql(insert_sql)
            print("total record count: %s" % (record_count))
            append_ds_success(append_ids_str)
            #update_sql = f"""update ds_append_log_his_bigtopics set is_deleted = 1 where id in ({append_ids_str})"""
        except Exception as e:
            logger.error("insert error: %s" % e)
            append_ds_failure(append_ids_str)
            #update_sql = f"""update ds_append_log_his_bigtopics set is_deleted = 1 where id in ({append_ids_str})"""
            #sync_write_ds_error(dict_prefix, 'big_topics')
    # 关闭游标
    #cursor.close()
    #conn.close()


# 获取mysql连接 = (
conn = pymysql.connect(host='xxx.xxx.xxx.xxx', port=3306, user='root', password='xxxxx', db='test', charset='utf8')


# 并发针对topic_id
def check_write_iceberg(paras_dict, topic_id):
    # 获取游标
    cursor = conn.cursor(DictCursor)
    # 执行sql
    check_sql = f"""
        select count(id) as cnt from ds_append_log_his_bigtopics 
         where mysql_id = '{paras_dict['mysql_id']}' and topic_id = '{topic_id}' and is_deleted = 0
    """
    print(check_sql)
    cursor.execute(check_sql)
    # 获取结果
    result = cursor.fetchone()
    flag_result_cnt = result['cnt']
    # 关闭游标
    cursor.close()

    if flag_result_cnt > 0:
        logger.info("mcap_data data already load to fy_ods_stg.fy_ad_big_topics_ods")
        return False
    else:
        # 将数据写入iceberg表中
        logger.info("start process mcap file")
        return True


def sync_df_ds_status(paras_dict, topic_id):
    insert_sql = f""" insert into ds_append_log_his_bigtopics(mysql_id, mcap_file, vin, topic_id, ds_table_name, date_str, cost_download_cos, record_count, status) 
                      values 
                       ('{paras_dict['mysql_id']}', '{paras_dict['mcap_file']}', '{paras_dict['vin']}','{topic_id}', 'fy_ad_dibo_ib_t003',  '{paras_dict['date_str']}', {paras_dict['cost_download_cos']}, {paras_dict['record_count']}, 1)
                  """
    # 获取游标
    cursor = conn.cursor(DictCursor)
    logger.info(insert_sql)
    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()


def sync_df_ds_empty(paras_dict, topic_id):
    insert_sql = f""" insert into ds_append_log_his_bigtopics(mysql_id, mcap_file, vin, topic_id, ds_table_name, date_str, record_count, status) 
                      values 
                       ('{paras_dict['mysql_id']}', '{paras_dict['mcap_file']}', '{paras_dict['vin']}','{topic_id}', 'fy_ad_dibo_ib_t003',  '{paras_dict['date_str']}', 0 , '-2')
                  """
    # 获取游标
    cursor = conn.cursor(DictCursor)
    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()


def sync_read_mcap_base(paras_dict, topic_id):
    insert_sql = f""" insert into ds_append_log_his_bigtopics(mysql_id, mcap_file, vin, topic_id, date_str, record_count, status) 
                      values 
                       ('{paras_dict['mysql_id']}', '{paras_dict['mcap_file']}', '{paras_dict['vin']}', '{topic_id}',  '{paras_dict['date_str']}', null , '{paras_dict['error_status']}')
                  """
    # 获取游标
    cursor = conn.cursor(DictCursor)
    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()

def append_ds_success(append_ids_str):
    update_sql = f"""update ds_append_log_his_bigtopics set status = 2 where status = 1 and mysql_id in ({append_ids_str})"""
    # 获取游标
    cursor = conn.cursor(DictCursor)
    cursor.execute(update_sql)
    conn.commit()
    cursor.close()

def append_ds_failure(append_ids_str):
    update_sql = f"""update ds_append_log_his_bigtopics set status = -4 where status = 1 and mysql_id in ({append_ids_str})"""
    # 获取游标
    cursor = conn.cursor(DictCursor)
    cursor.execute(update_sql)
    conn.commit()
    cursor.close()


def sync_read_mcap_error(paras_dict, topic_id):
    paras_dict['error_status'] = '-9'
    sync_read_mcap_base(paras_dict, topic_id)


def sync_write_ds_error(paras_dict, topic_id):
    paras_dict['error_status'] = '-4'
    sync_read_mcap_base(paras_dict, topic_id)


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