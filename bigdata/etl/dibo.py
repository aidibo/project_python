#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0


def string_convert_list(json_path_str):
    json_paths = [p.strip() for p in json_path_str.split(',') if p.strip()]
    print(json_paths)
    return json_paths


def hadoop_list():
    base_path = '/path1/test02.json'
    result = []
    for i in range(1, 3):
        result.append(f'{base_path}')  # 每次添加一个带单引号的路径字符串
    return result


if __name__ == '__main__':
    json_str = '/path1/test02.json,/path2/test.json,/path3/test.json'
    json_path_list = string_convert_list(json_path_str=json_str)
    print(hadoop_list())
    #print(json_path_list)
