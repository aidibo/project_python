#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0


if __name__ == '__main__':
    import os

    cpu_count = os.cpu_count()  # 获取CPU核心数

    # 推荐设置
    max_workers = cpu_count * 2  # 通常为 16-32
    print(str(cpu_count))
    print(str(max_workers))
