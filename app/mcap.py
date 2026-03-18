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
import io

from loguru import logger

import pymysql
from pymysql.cursors import DictCursor

from mcap_protobuf.decoder import DecoderFactory
from mcap.reader import make_reader
from google.protobuf.json_format import MessageToDict
import json
import requests

from pyspark.sql import Row
from pyspark.sql import SparkSession
# from pyspark.sql.functions import to_json, struct, col  # ,lit
from pyspark.sql.functions import col
import os

#os.environ['PYSPARK_PYTHON'] = '/opt/anaconda3/envs/python3_7/bin/python3.7'
#os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/anaconda3/envs/python3_7/bin/python3.7'

# 正常情况日志级别使用 INFO，需要定位时可以修改为 DEBUG，此时 SDK 会打印和服务端的通信信息
#logging.basicConfig(level=logging.INFO, stream=sys.stdout)
#logger.add('get_mcap_file_{time}.log', rotation='00:00', retention='300 days')

spark = SparkSession.builder.appName("stg_append_iceberg").enableHiveSupport().getOrCreate()
# 获取mysql连接 = (
# conn = pymysql.connect(host='10.161.160.79', port=3306, user='root', password='123456', db='test', charset='utf8')
conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='Root!1q2w', db='test', charset='utf8')

def concatenate_numbers(year, month, day, hour, minute):
    return str(f"{year:04d}{month:02d}{day:02d}{hour:02d}{minute:02d}")

def consumer_json_from_kafka(topic):
    BATCH_SIZE = 2
    ds_df_row = []
    uuid_list = []
    append_uuid_list = []

    uuids_list = ['8e6a8609-acee-4e74-9c9e-48afd62742bb','44b76b96-09e1-4d7f-9634-af2f43cb2d4a',
                  '81323c6b-f4e6-41ed-9747-347d45bff893','86748524-891f-4f2e-95be-a5a04267e6ec',
                  '547deb1e-8d01-4d1c-8734-d2f578ba4ab6','5b153ea7-41eb-46ec-ae99-4d607bc69a51'
                  ]
    # 消费模块
    try:
        for uuid in uuids_list:
            if uuid is not None:
                uuid_list.append(uuid)

                if len(uuid_list) >= BATCH_SIZE:
                    for i in range(len(uuid_list)):
                        download_url, dict_prefix = get_download_url(uuid_list[i])
                        #download_url = "https://bit.nioint.com/adw-stg/FyADWTableForSnapshot/" + rowkey
                        print("download_url ==> {}".format(download_url if download_url is not None else "None"))
                        tmp_row_message = parse_pb_from_url(download_url, dict_prefix=dict_prefix)
                        if tmp_row_message:
                            append_uuid_list.append(uuid_list[i])
                            ds_df_row.extend(tmp_row_message)

                    if len(ds_df_row) >= 1:
                        append_uuid_str = ','.join(map(str, append_uuid_list))
                        print("append_ids_str: %s" % (append_uuid_str))
                        record_count = len(ds_df_row)
                        df_source = spark.createDataFrame(ds_df_row)
                        df = df_source.select(col("vin").alias("vin"), col("sw_id").alias("sw_id"),
                                              col("mcap_file").alias("mcap_file"),
                                              col("mcap_file_info").alias("mcap_file_info"),
                                              col("publish_ts").alias("publish_ts"),
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
                        spark.sql("select * from t_tmp").show(10, truncate=False)
                        try:
                            # spark.sql(insert_sql)
                            print("total record count: %s" % (record_count))
                            #append_ds_success(append_uuid_str)
                            # update_sql = f"""update ds_append_log_his_bigtopics set is_deleted = 1 where id in ({append_ids_str})"""
                        except Exception as e:
                            logger.error("insert error: %s" % e)
                            #append_ds_failure(append_uuid_str)

                        uuid_list = []  # 清空消息列表
                        ds_df_row = []  # 清空消息列表
                        append_uuid_list = []  # 清空消息列表

    except KeyboardInterrupt as e:
            print (e)


def get_download_url(uuid):
    table_name = 'production/fy/snapshot/datafiles'
    #uuid = 'aa43c261-5e34-481f-8f97-4953838c65e5'
    where = "uuid='{uuid}' and file_type='mcap'".format(uuid=uuid)
    #scan = cmm.Scan(table_name=table_name, where=where, limit=10)
    #scan_result_iterator = adw_client.scan(scan)
    scan_result_iterator = []
    url = ''
    download_url = ''
    dict_prefix = {}
    for scan_row in scan_result_iterator:
        # adw 自动生成唯一键
        print(scan_row.rowkey)
        url = adw_client.generate_download_url(scan_row)
        download_url = "https://bit.nioint.com/adw-stg/FyADWTableForSnapshot/" + scan_row.rowkey
        print(url)
        # f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        date_str = str(scan_row.meta["date"])
        dict_prefix = {"rowkey": scan_row.rowkey, "obj_key": scan_row.meta["file_path"],  "vin": scan_row.meta["vehicle_id"],
                       "vehicle_uuid": scan_row.meta["vehicle_uuid"], "vid": scan_row.meta["vehicle_id"],
                       "date_str": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
                       'mcap_file': scan_row.meta["file_name"], 'uuid': scan_row.meta["uuid"],  'url': url,
                       'file_size': scan_row.meta["data_size"]}
    return download_url, dict_prefix


def parse_pb_from_url(url, dict_prefix):
    ####  获取文件到response
    try:
        start_time = time.time()
        dict_prefix["adventure_start_time"] = start_time

        res = requests.get(url)
        binary_io = io.BytesIO(res.content)

        # 记录cost download cos 时间
        cost_download_cos = time.time() - dict_prefix["adventure_start_time"]
        dict_prefix["cost_download_cos"] = cost_download_cos

        # Use the binary_io object in a with statement
        with binary_io as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])

            sw_id = '0'
            # date_str = '0'
            try:
                for schema, channel, message, proto_msg in reader.iter_decoded_messages(topics='/info/swid'):
                    sensor_data_dict2 = MessageToDict(proto_msg, preserving_proto_field_name=True)
                    publish_ts = proto_msg.publish_ts
                    if proto_msg.SwId[0].swid_source == 1:
                        year = proto_msg.SwId[0].year
                        month = proto_msg.SwId[0].month
                        day = proto_msg.SwId[0].day
                        hour = proto_msg.SwId[0].hour
                        minute = proto_msg.SwId[0].minute
                        sw_id = concatenate_numbers(year, month, day, hour, minute)
                        # date_str = f"""{year}-{month}-{day} {hour}:{minute}"""
                # print(sw_id)
            except Exception as e:
                sw_id = '0'

            # for topic_id in topic_ids:
            messages = []
            messages_rows = []
            sensor_data_dict = []
            # tmp_topic_ids = []
            # tmp_topic_ids.append(topic_id)
            # logger.info("tmp_topic_ids=%s" % (tmp_topic_ids))
            for schema, channel, message, proto_msg in reader.iter_decoded_messages(topics='foxglove/snapshot'):
                # if channel.topic in topic_id:
                sensor_data_dict = MessageToDict(proto_msg, preserving_proto_field_name=True)
                sensor_data_dict['vin'] = dict_prefix['vin']
                sensor_data_dict['vid'] = dict_prefix['vid']
                sensor_data_dict['uuid'] = dict_prefix['uuid']
                sensor_data_dict['sw_id'] = sw_id
                sensor_data_dict['date_str'] = dict_prefix['date_str']
                sensor_data_dict['mcap_file'] = dict_prefix['mcap_file']
                #sensor_data_dict['mysql_id'] = dict_prefix['mysql_id']
                sensor_data_dict['rowkey'] = dict_prefix['rowkey']
                sensor_data_dict['mcap_file_info'] = dict_prefix['obj_key']
                sensor_data_dict['topic_id'] = channel.topic
                messages.append(sensor_data_dict)
                messages_rows.append(Row(datetime=dict_prefix['date_str'], mcap_file=dict_prefix['mcap_file'],
                                         #mysql_id=dict_prefix['mysql_id'],
                                         mcap_file_info=dict_prefix['obj_key'],
                                         file_size=dict_prefix['file_size'], uuid=dict_prefix['uuid'],
                                         vin=dict_prefix['vin'], vid=dict_prefix['vid'],
                                         vehicle_uuid=dict_prefix['vehicle_uuid'],
                                         sw_id=sw_id,
                                         topic=channel.topic,
                                         publish_ts=proto_msg.publish_ts,  # topic=channel.topic.replace('/', '_'),
                                         publish_ptp_ts=proto_msg.publish_ptp_ts,
                                         json_str=json.dumps(sensor_data_dict, ensure_ascii=False)
                                         ))
            dict_prefix["record_count"] = len(messages_rows)
            print("file count ==> {}".format(dict_prefix["record_count"]))
            if messages:
                #sync_df_ds_status(dict_prefix, 'foxglove/snapshot')
                return messages_rows
            else:
                #sync_df_ds_empty(dict_prefix, 'foxglove/snapshot')
                logger.info("mcap file is empty ==> id=%s,vin=%s,obj_key=%s,topid_id=%s" % (
                dict_prefix['mysql_id'], dict_prefix['vin'], url, 'small_topics'))
                return messages_rows
    except Exception as e:
        # 异常写入数据库
        # dict_prefix['error_status'] = '-9'
        #sync_read_mcap_error(dict_prefix, 'foxglove/snapshot')
        logger.exception(e)
        return []

def sync_df_ds_status(paras_dict, topic_id):
    insert_sql = f""" insert into ds_append_log_his_snapshot(mysql_id, mcap_file, vin, topic_id, ds_table_name, date_str, cost_download_cos, record_count, status) 
                      values 
                       ('{paras_dict['mysql_id']}', '{paras_dict['mcap_file']}', '{paras_dict['vin']}','{topic_id}', 'fy_ad_dibo_ib_t003',  '{paras_dict['date_str']}', {paras_dict['cost_download_cos']}, {paras_dict['record_count']}, 1)
                  """
    # 获取游标
    cursor = conn.cursor(DictCursor)
    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()


def sync_df_ds_empty(paras_dict, topic_id):
    insert_sql = f""" insert into ds_append_log_his_snapshot(mysql_id, mcap_file, vin, topic_id, ds_table_name, date_str, record_count, status) 
                      values 
                       ('{paras_dict['mysql_id']}', '{paras_dict['mcap_file']}', '{paras_dict['vin']}','{topic_id}', 'fy_ad_dibo_ib_t003',  '{paras_dict['date_str']}', 0 , '-2')
                  """
    # 获取游标
    cursor = conn.cursor(DictCursor)
    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()


def sync_read_mcap_base(paras_dict, topic_id):
    insert_sql = f""" insert into ds_append_log_his_snapshot(mysql_id, mcap_file, vin, topic_id, date_str, record_count, status) 
                      values 
                       ('{paras_dict['mysql_id']}', '{paras_dict['mcap_file']}', '{paras_dict['vin']}', '{topic_id}',  '{paras_dict['date_str']}', null , '{paras_dict['error_status']}')
                  """
    # 获取游标
    cursor = conn.cursor(DictCursor)
    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()

def sync_read_mcap_error(paras_dict, topic_id):
    paras_dict['error_status'] = '-9'
    sync_read_mcap_base(paras_dict, topic_id)


def sync_write_ds_error(paras_dict, topic_id):
    paras_dict['error_status'] = '-4'
    sync_read_mcap_base(paras_dict, topic_id)

def append_ds_success(append_ids_str):
    update_sql = f"""update ds_append_log_his_bigtopics set status = 2 where id in ({append_ids_str})"""
    # 获取游标
    cursor = conn.cursor(DictCursor)
    cursor.execute(update_sql)
    conn.commit()
    cursor.close()

def append_ds_failure(append_ids_str):
    update_sql = f"""update ds_append_log_his_bigtopics set status = -4 where id in ({append_ids_str})"""
    # 获取游标
    cursor = conn.cursor(DictCursor)
    cursor.execute(update_sql)
    conn.commit()
    cursor.close()

def get_meta_data(start_time, end_time):
    table_name = 'production/fy/snapshot/datafiles'
    # table_name = 'production/fy/log/datafiles'
    #uuid = 'aa43c261-5e34-481f-8f97-4953838c65e5'
    #start_time = 1735354504
    #end_time = 1735358805
    where = "sys.update_time > {start_time} and sys.update_time <= {end_time} ".format(start_time=start_time, end_time=end_time)
    # where = "sys.create_time > {start_time} and sys.create_time <= {end_time} and meta.file_name >= 'calib-onl-camera' and meta.file_name < 'calib-onl-camerb' ".format(start_time=start_time, end_time=end_time)
    # where = "uuid='{uuid}' and file_type='mcap'".format(uuid=uuid)
    #scan = cmm.Scan(table_name=table_name, where=where, limit=100000)
    #scan_result_iterator = adw_client.scan(scan)
    scan_result_iterator = []
    url = ''
    download_url = ''
    dict_prefix = {}
    result = []
    for scan_row in scan_result_iterator:
        # adw 自动生成唯一键
        #print(scan_row.rowkey)
        url = adw_client.generate_download_url(scan_row)
        # download_url = "https://bit.nioint.com/adw-stg/FyADWTableForSnapshot/" + scan_row.rowkey
        #print(url)
        # f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        date_str = str(scan_row.meta["date"])
        original_path = "".join(["niofs://fy-cn-dlb-default-stg/", scan_row.sys["original_path"]])
        #print(original_path)
        dict_prefix = {"rowkey": scan_row.rowkey, "obj_key": scan_row.meta["file_path"],  "vin": scan_row.meta["vehicle_id"],
                       "vehicle_uuid": scan_row.meta["vehicle_uuid"], "vid": scan_row.meta["vehicle_id"],
                       "date_str": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
                       'file_name': scan_row.meta["file_name"], 'uuid': scan_row.meta["uuid"],  'url': url,
                       'file_size': scan_row.meta["data_size"], 'original_path':scan_row.sys["original_path"],
                       'adw_sys_update_time':scan_row.sys["update_time"]}
        result.append(dict_prefix)
    #return download_url, dict_prefix
    return result


from packages.mysql_utils import MySQLDB
# 初始化数据库连接

db = MySQLDB(
    host='d-qcsh4-common-mysql8-cluster-01-dev.nioint.com',
    user='fy_data_platform_dev_6fc16677_rw',
    password='zqHDQQyqabDUSukjf1I',
    database='fy_data_platform_dev'
)


# conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='Root!1q2w', db='test', charset='utf8')
def save_mysql(start_time, end_time):
    result_list = get_meta_data(start_time=start_time, end_time=end_time)
    affected_rows = db.insert_many('t_snapshot_metadata', result_list)
    print(f"Inserted {affected_rows} rows.")
    etl_dict = {'etl_task':'t_snapshot_metadata','end_time': end_time, 'start_time': start_time, 'affected_rows': affected_rows, 'status': 1 }
    db.insert('t_etl_track', etl_dict)
    """
    for result in result_list:
        db.insert('t_log_metadata', result)
        #print(result)
    """

def item_loop():
    import datetime
    import math
    batch_size = 3600
    #time_str_start = "2024-12-20 00:00:00"  #1732982400
    #time_str_end = "2024-12-01 02:00:00"    #1737103405
    #begin_date_second = 1734660000
    # SELECT 示例
    result = db.select(
        table='t_etl_track',
        conditions={'etl_task': 't_snapshot_metadata'},
        fields=['max(end_time) as max_end_time']
    )
    if result[0]['max_end_time'] is not None:
        begin_date_second = result[0]['max_end_time']
        # print(begin_date_second)
    else:
        begin_date_second = 0

    current_time_seconds = time.time()
    end_date_second = int(current_time_seconds) - 100
    # end_date_second = 1737396000
    # 转换为时间戳
    #timestamp_start = int(datetime.datetime.strptime(time_str_start, '%Y-%m-%d %H:%M:%S').timestamp())
    #timestamp_end = int(datetime.datetime.strptime(time_str_end, '%Y-%m-%d %H:%M:%S').timestamp())
    #batch_cnt = math.ceil((end_date_second - begin_date_second + 1) / batch_size) + 1
    #print(f"batch_cnt is {batch_cnt}")
    #for i in range(batch_cnt):
    start_time = begin_date_second
    end_time = end_date_second
    print(start_time)
    print(end_time)
    save_mysql(start_time, end_time)
    #print(f"batch {i} done")
    # time.sleep(1)

if __name__ == '__main__':
    topic_name = "aa-stg-production-fy-dlb-data"
    item_loop()
    #consumer_json_from_kafka(topic_name)
    #result1 = get_meta_data(start_time=1737338153, end_time=1737358271)
    #print(len(result1))
    # save_mysql()