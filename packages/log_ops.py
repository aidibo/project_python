#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0
"""
https://loguru.readthedocs.io/en/stable/index.html
https://www.cnblogs.com/longweiqiang/p/14100211.html
https://github.com/IMInterne/python-elasticsearch-ecs-logger
"""

from loguru import logger


def loguru():
    import time
    start_time = time.time()
    time.sleep(1)
    end_time = time.time()
    # 正常情况日志级别使用 INFO，需要定位时可以修改为 DEBUG，此时 SDK 会打印和服务端的通信信息
    logger.add('get_mcap_file_{time}.log', rotation='00:00', retention='300 days')
    logger.info("cost time: = %s" % (str(end_time - start_time)))


def get_logger(name=None):
    ### CMRESHandler code:
    import logging
    import sys
    from os import makedirs
    from os.path import dirname, exists

    from cmreslogging.handlers import CMRESHandler

    loggers = {}

    LOG_ENABLED = True  # 是否开启日志
    LOG_TO_CONSOLE = True  # 是否输出到控制台
    LOG_TO_FILE = True  # 是否输出到文件
    LOG_TO_ES = True  # 是否输出到 Elasticsearch

    LOG_PATH = './runtime.log'  # 日志文件路径
    LOG_LEVEL = 'DEBUG'  # 日志级别
    LOG_FORMAT = '%(levelname)s - %(asctime)s - process: %(process)d - %(filename)s - %(name)s - %(lineno)d - %(module)s - %(message)s'  # 每条日志输出格式
    ELASTIC_SEARCH_HOST = 'fyad-data-platform-cc3ef1e8-es-rw-stg.nioint.com'  # Elasticsearch Host
    ELASTIC_SEARCH_PORT = 9200  # Elasticsearch Port
    ELASTIC_SEARCH_INDEX = 'fyad_data_platform_stg_mcap_etl_ds_old'  # Elasticsearch Index Name
    APP_ENVIRONMENT = 'stg'  # 运行环境，如测试环境还是生产环境

    global loggers

    if not name: name = __name__

    if loggers.get(name):
        return loggers.get(name)

    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    # 输出到控制台
    if LOG_ENABLED and LOG_TO_CONSOLE:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level=LOG_LEVEL)
        formatter = logging.Formatter(LOG_FORMAT)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # 输出到 Elasticsearch
    if LOG_ENABLED and LOG_TO_ES:
        # 添加 CMRESHandler
        es_handler = CMRESHandler(hosts=[{'host': ELASTIC_SEARCH_HOST, 'port': ELASTIC_SEARCH_PORT}],
                                  # 可以配置对应的认证权限
                                  auth_type=CMRESHandler.AuthType.BASIC_AUTH,
                                  auth_details=('fyad_data_platform_stg_rw','dI2DDT2tsWP1RhiAxehJECbZovfJG6uM2Epzcw3U1TNkGx4FH9jVIOGTX3ETIHIJx5syHEjJgtcnBzaM14IzhinhkflM9WLNFTK3M4bU3regOle5kTfdOrPql3Ru2bQ7'),
                                  es_index_name=ELASTIC_SEARCH_INDEX,
                                  # 一个月分一个 Index
                                  index_name_frequency=CMRESHandler.IndexNameFrequency.MONTHLY,
                                  # 额外增加环境标识
                                  es_additional_fields={'environment': APP_ENVIRONMENT}
                                  )
        es_handler.setLevel(level=LOG_LEVEL)
        formatter = logging.Formatter(LOG_FORMAT)
        es_handler.setFormatter(formatter)
        logger.addHandler(es_handler)

    # 保存到全局 loggers
    loggers[name] = logger
    return logger


def es_logs():
    ### CMRESHandler code:
    logger = get_logger()
    dict_user = { "username":"zhangsan", "age":12, "info":"info2" }
    logger.info(dict_user)
    for i in range(1, 2):
        try:
            a = 2/0
        except Exception as e:
            #logger.exception("e, 'dibo2', {username},'vin1'".format(username="zhanghaohao"))
            logger.exception("{e}, 'dibo2', {dict_user},'vin1'".format(e=e, dict_user=dict_user))


def get_logger_es(name=None):
    ###  ElasticECSHandler code:
    import logging
    import sys
    from os import makedirs
    from os.path import dirname, exists

    from elasticecslogging.handlers import ElasticECSHandler

    loggers = {}

    LOG_ENABLED = True  # 是否开启日志
    LOG_TO_CONSOLE = True  # 是否输出到控制台
    LOG_TO_FILE = True  # 是否输出到文件
    LOG_TO_ES = True  # 是否输出到 Elasticsearch

    LOG_PATH = './runtime.log'  # 日志文件路径
    LOG_LEVEL = 'DEBUG'  # 日志级别
    LOG_FORMAT = '%(levelname)s - %(asctime)s - process: %(process)d - %(filename)s - %(name)s - %(lineno)d - %(module)s - %(message)s'  # 每条日志输出格式
    ELASTIC_SEARCH_HOST = 'fyad-data-platform-cc3ef1e8-es-rw-stg.nioint.com'  # Elasticsearch Host
    ELASTIC_SEARCH_PORT = 9200  # Elasticsearch Port
    ELASTIC_SEARCH_INDEX = 'fyad_data_platform_stg_mcap_etl_ds'  # Elasticsearch Index Name
    APP_ENVIRONMENT = 'stg'  # 运行环境，如测试环境还是生产环境

    global loggers

    if not name: name = __name__
    if loggers.get(name):
        return loggers.get(name)

    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    # 输出到控制台
    if LOG_ENABLED and LOG_TO_CONSOLE:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level=LOG_LEVEL)
        formatter = logging.Formatter(LOG_FORMAT)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # 输出到 Elasticsearch
    if LOG_ENABLED and LOG_TO_ES:
        # 添加 CMRESHandler
        es_handler = ElasticECSHandler(hosts=[{'host': ELASTIC_SEARCH_HOST, 'port': ELASTIC_SEARCH_PORT}],
                                       # 可以配置对应的认证权限
                                       auth_type=ElasticECSHandler.AuthType.BASIC_AUTH,
                                       auth_details=('fyad_data_platform_stg_rw',
                                                     'dI2DDT2tsWP1RhiAxehJECbZovfJG6uM2Epzcw3U1TNkGx4FH9jVIOGTX3ETIHIJx5syHEjJgtcnBzaM14IzhinhkflM9WLNFTK3M4bU3regOle5kTfdOrPql3Ru2bQ7'),
                                       es_index_name=ELASTIC_SEARCH_INDEX,
                                       # 一个月分一个 Index
                                       index_name_frequency=ElasticECSHandler.IndexNameFrequency.MONTHLY,
                                       # 额外增加环境标识
                                       es_additional_fields={'environment': APP_ENVIRONMENT}
                                       )
        es_handler.setLevel(level=LOG_LEVEL)
        formatter = logging.Formatter(LOG_FORMAT)
        es_handler.setFormatter(formatter)
        logger.addHandler(es_handler)

    # 保存到全局 loggers
    loggers[name] = logger
    return logger


def es_logs_es():
    logger = get_logger_es()
    # logger.debug('this is a message')
    dict_user = { "username":"zhangsan", "age":12, "info":"info2" }
    logger.info(dict_user)
    for i in range(1, 2):
        try:
            a = 2/0
        except Exception as e:
            #logger.exception("e, 'dibo2', {username},'vin1'".format(username="zhanghaohao"))
            logger.exception("{e}, 'dibo2', {dict_user},'vin1'".format(e=e, dict_user=dict_user))



