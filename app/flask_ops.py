#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0


# 编写 Flask 应用
from flask import Flask, jsonify
from pyngrok import ngrok

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({"message": "Hello, Flask on Colab!"})

@app.route('/hello/<name>')
def hello(name):
    return jsonify({"message": f"Hello, {name}!"})

# 启动 ngrok 隧道
public_url = ngrok.connect(5000)
print(f"Public URL: {public_url}")

# 启动 Flask 应用
app.run(port=5000)

