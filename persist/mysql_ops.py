#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0

# https://chat.deepseek.com/a/chat/s/46b49bdd-88ff-44cb-84ce-4128d2550af1

import pymysql
from typing import Dict, List, Union, Tuple


class MySQLDB:
    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        self.config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': port,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }

    def get_connection(self):
        return pymysql.connect(**self.config)

    def select(self, table: str, conditions: Dict = None, fields: List[str] = None) -> List[Dict]:
        fields_str = ', '.join(fields) if fields else '*'
        sql = f"SELECT {fields_str} FROM {table}"

        if conditions:
            where_clause = ' AND '.join([f"{k} = %s" for k in conditions.keys()])
            sql += f" WHERE {where_clause}"

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(conditions.values()) if conditions else None)
                return cursor.fetchall()

    def insert(self, table: str, data: Dict) -> int:
        fields = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        sql = f"INSERT INTO {table} ({fields}) VALUES ({values})"

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(data.values()))
                conn.commit()
                return cursor.lastrowid

    def insert_many(self, table: str, data_list: List[Dict]) -> int:
        if not data_list:
            return 0

        fields = ', '.join(data_list[0].keys())
        values = ', '.join(['%s'] * len(data_list[0]))
        sql = f"INSERT INTO {table} ({fields}) VALUES ({values})"

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                affected_rows = cursor.executemany(sql, [tuple(data.values()) for data in data_list])
                conn.commit()
                return affected_rows

    def update(self, table: str, data: Dict, conditions: Dict) -> int:
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        where_clause = ' AND '.join([f"{k} = %s" for k in conditions.keys()])
        sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

        params = tuple(list(data.values()) + list(conditions.values()))

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                affected_rows = cursor.execute(sql, params)
                conn.commit()
                return affected_rows


if __name__ == '__main__':
    import time
    # from mysql_utils import MySQLDB
    # 初始化数据库连接
    db = MySQLDB(
        host='d-qcsh4-common-mysql8-cluster-01-dev.nioint.com',
        user='fy_data_platform_dev_6fc16677_rw',
        password='zqHDQQyqabDUSukjf1I',
        database='fy_data_platform_dev'
    )

    start_time = time.time()
    time.sleep(1)
    end_time = time.time()
    etl_dict = {'etl_task':'t_snapshot_metadata','end_time': end_time, 'start_time': start_time, 'affected_rows': affected_rows, 'status': 1 }
    db.insert('t_etl_track', etl_dict)