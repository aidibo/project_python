#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0

import warnings
from loguru import logger
import json

warnings.filterwarnings('ignore')          # 最常用，全局忽略所有警告

# Log to a file
# logger.add("app.log")

# With rotation (create new file when size limit reached)
# logger.add("app.log", rotation="10 MB", compression="zip")

logger.add(
    "logs/app_log_{time:YYYY-MM-DD}.log",
    # "logs/app_log.log",
    #format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {extra} | {message}",
    rotation="00:00",  # 每天午夜轮转
    # 每小时轮转
    #logger.add("app.log", rotation="1 hour")
    # rotation="10 MB",
    retention="45 days",
    level="INFO",                       # usually INFO or higher in files
    encoding="utf-8",
    enqueue=True,
)


def cost_time():
    import time
    start_time = time.time()
    time.sleep(1)
    end_time = time.time()
    logger.info("cost time: = %s" % (str(end_time - start_time)))


def loguru():
    logger.info("dibo_test")
    # 3. Contextual information (very useful)
    logger.bind(user_id=3841, request_id="req_7f3k9p2m").info("User logged in")
    logger.bind(ip="45.79.123.45", endpoint="/api/v1/orders").warning("Slow endpoint")


def uuid_str():
    import uuid
    uuid_str = str(uuid.uuid4())
    print(uuid_str)
    return uuid_str


def date_format(before_ndays=1):
    import datetime
    before_day = datetime.date.today() - datetime.timedelta(days=before_ndays)
    return before_day.strftime('%Y-%m-%d')


def date_str():
    import  datetime
    current_datetime = datetime.datetime.now()
    datetime_str = current_datetime.strftime('%Y%m%d')
    return datetime_str


def local_dir():
    import os
    local_directory = 'logs'
    messages = {"key": "name"}
    topic_id = "msg.json"
    # Write the combined data to a single JSON file
    # local_directory = 'data'
    os.makedirs(local_directory, exist_ok=True)
    # DestFilePath = '{}/{}'.format(local_directory, filename)
    with open('{}/{}'.format(local_directory, topic_id.replace('/', '-')), "w") as json_file:
        json.dump(messages, json_file, indent=4)


def tqdm():
    """
    tqdm 是 Python 中最受欢迎的进度条库之一，它简单、高效、跨平台（终端、Jupyter Notebook 都支持），常用于显示循环、文件下载、训练模型等耗时任务的进度。
    """
    from tqdm import tqdm
    import time

    # 示例1：简单 for 循环
    for i in tqdm(range(2)):
        time.sleep(1)  # 模拟耗时操作

    for i in tqdm(range(10), desc="模拟训练", unit="batch", unit_scale=False, leave=True):
        time.sleep(1)


if __name__ == '__main__':
    # cost_time()
    # uuid_str()
    # print(date_format(1))
    # print(date_str())
    # local_dir()
    tqdm()
