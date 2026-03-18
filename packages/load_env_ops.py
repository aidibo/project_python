#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0


from dotenv import load_dotenv, find_dotenv
import os

_ = load_dotenv(find_dotenv())  # read local .env file

# 设置当前环境（可以通过命令行、环境变量或配置文件指定）
# 例如：export ENV=dev
env = os.getenv('ENV', 'dev')  # 默认使用开发环境

# 根据环境加载对应的 .env 文件
if env == 'dev':
    load_dotenv('.env.dev')
elif env == 'test':
    load_dotenv('.env.test')
elif env == 'prod':
    load_dotenv('.env.prod')
else:
    raise ValueError(f"Unknown environment: {env}")

# 读取环境变量
database_url = os.getenv('DATABASE_URL')
secret_key = os.getenv('SECRET_KEY')
debug= os.getenv('DEBUG')

# 打印读取到的环境变量
print(f"Environment: {env}")
print(f"DATABASE_URL: {database_url}")
print(f"SECRET_KEY: {secret_key}")
print(f"DEBUG: {debug}")

# 使用 dotenv_values 读取特定文件
# 如果你不想直接加载环境变量到 os.environ，而是想以字典形式读取特定文件的内容，可以使用 dotenv_values 函数。
from dotenv import dotenv_values

# 读取特定 .env 文件
env = 'dev'  # 指定环境
env_vars = dotenv_values(f'.env.{env}')

# 访问配置
database_url = env_vars.get('DATABASE_URL')
secret_key = env_vars.get('SECRET_KEY')
debug = env_vars.get('DEBUG', 'False').lower() == 'true'

print(f"Environment: {env}")
print(f"DATABASE_URL: {database_url}")
print(f"SECRET_KEY: {secret_key}")
print(f"DEBUG: {debug}")

# 结合 argparse 动态指定环境
import argparse
from dotenv import load_dotenv
import os

# 设置命令行参数
parser = argparse.ArgumentParser()
parser.add_argument('--env', type=str, default='dev', help='Environment to load (dev, test, prod)')
args = parser.parse_args()

# 根据命令行参数加载对应的 .env 文件
# load_dotenv(f'.env.{args.env}')
load_dotenv(f".env.{args.env}", override=True)  # override=True 覆盖已存在的

# 读取环境变量
database_url = os.getenv('DATABASE_URL')
secret_key = os.getenv('SECRET_KEY')
debug = os.getenv('DEBUG', 'False').lower() == 'true'

# 打印读取到的环境变量
print(f"Environment: {args.env}")
print(f"DATABASE_URL: {database_url}")
print(f"SECRET_KEY: {secret_key}")
print(f"DEBUG: {debug}")

# 执行python程序
# python your_script.py --env=test