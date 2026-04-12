#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0

# https://colab.research.google.com/drive/1Auw5ey-WHa9NRx1EFsridHjnDgoKaw0C#scrollTo=JE3wDApreD5Y

'''
ViT-L/14@336px CLIP 是 OpenAI 开发的一个视觉-语言预训练模型的特定配置。让我解释一下这个名称中各部分的含义和作用：
名称解析：

ViT-L：Vision Transformer - Large，表示使用大型视觉Transformer架构
14：patch size为14×14像素，即将输入图像分割成14×14的小块
336px：输入图像分辨率为336×336像素
'''

from google.colab import ai
response = ai.generate_text("What is the capital of France?")

print(response)

response = ai.generate_text("What is the capital of China?")
print(response)

# !pip uninstall clip          # 先移除錯誤的clip（很重要！）
# !pip install git+https://github.com/openai/CLIP.git

import os
import torch
import pickle
import numpy as np
from PIL import Image
from sklearn.linear_model import LogisticRegression
import clip


def load_data_and_extract_features(positive_dir, negative_dir, model, preprocess, device):
    """
    加载图片并提取特征
    """
    image_paths = []
    labels = []

    # 1. 读取正样本并分配标签 1
    if os.path.exists(positive_dir):
        for filename in os.listdir(positive_dir):
            if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.webp')):
                image_paths.append(os.path.join(positive_dir, filename))
                labels.append(1)
    else:
        print(f"警告: 正样本目录 {positive_dir} 不存在")

    # 2. 读取负样本并分配标签 0
    if os.path.exists(negative_dir):
        for filename in os.listdir(negative_dir):
            if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.webp')):
                image_paths.append(os.path.join(negative_dir, filename))
                labels.append(0)
    else:
        print(f"警告: 负样本目录 {negative_dir} 不存在")

    print(f"共加载 {len(image_paths)} 张图片 (正样本: {sum(labels)}, 负样本: {len(labels) - sum(labels)})")

    # 3. 批量提取特征
    all_features = []

    with torch.no_grad():
        for path in image_paths:
            # 加载并预处理图片
            image = preprocess(Image.open(path).convert("RGB")).unsqueeze(0).to(device)

            # 使用CLIP提取图像特征
            image_features = model.encode_image(image)

            # 转换为CPU numpy数组并添加到列表
            all_features.append(image_features.cpu().numpy())

    # 将列表转换为二维数组 shape: (N, 768) 对于 ViT-L/14
    features = np.concatenate(all_features, axis=0)
    labels = np.array(labels)

    return features, labels

def main():
    # 配置参数
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model_name = "ViT-L/14@336px"
    model_path = "clip_logistic_regression.pkl"

    # 设置你的图片目录
    positive_samples_dir = "./data/positive"  # 修改为你的正样本文件夹路径
    negative_samples_dir = "./data/negative"  # 修改为你的负样本文件夹路径

    print(f"正在加载 CLIP 模型 ({model_name}) 到 {device}...")
    model, preprocess = clip.load(model_name, device=device)
    model.eval() # 设置为评估模式

    # 1. 加载数据并提取特征
    print("正在提取图片特征...")
    features, labels = load_data_and_extract_features(
        positive_samples_dir,
        negative_samples_dir,
        model,
        preprocess,
        device
    )

    # 2. 训练分类器
    print("正在训练 Logistic Regression 分类器...")
    classifier = LogisticRegression(
        random_state=0,
        C=0.316,
        max_iter=1000,
        verbose=1, # 设置为1可以看到训练日志
        class_weight="balanced"
    )
    classifier.fit(features, labels)

    # 评估一下训练集准确率 (可选)
    score = classifier.score(features, labels)
    print(f"训练集准确率: {score * 100:.2f}%")

    # 3. 保存模型
    print(f"正在保存模型到 {model_path}...")
    with open(model_path, "wb") as f:
        pickle.dump(classifier, f)

    print("训练完成！")

if __name__ == "__main__":
    main()

### Prediction Function

# First, let's define a function that loads a new image, extracts its features using the CLIP model, and then uses the trained Logistic Regression classifier to predict its label (e.g., positive or negative).

def predict_image(image_path, model, preprocess, classifier, device):
    """
    使用训练好的分类器预测单张图片的标签。
    """
    if not os.path.exists(image_path):
        print(f"错误: 图片文件 {image_path} 不存在。")
        return None

    # 1. 加载并预处理图片
    image = preprocess(Image.open(image_path).convert("RGB")).unsqueeze(0).to(device)

    # 2. 使用CLIP提取图像特征
    with torch.no_grad():
        image_features = model.encode_image(image)

    # 3. 转换为CPU numpy数组
    features = image_features.cpu().numpy()

    # 4. 使用分类器进行预测
    prediction = classifier.predict(features)
    prediction_proba = classifier.predict_proba(features)

    # 假设标签 1 是 'positive', 0 是 'negative'
    label_map = {1: 'positive', 0: 'negative'}
    predicted_label = label_map.get(prediction[0], 'unknown')

    print(f"图片: {image_path}")
    print(f"预测标签: {predicted_label} (原始预测值: {prediction[0]})")
    print(f"预测概率 (负样本, 正样本): {prediction_proba[0]}")

    return predicted_label, prediction_proba[0]


# Test

#! pwd

### Load Model and Predict

#Now, let's load the trained Logistic Regression model and the CLIP model, then use our new function to predict an example image.

# 配置参数 (与训练时保持一致)
device = "cuda" if torch.cuda.is_available() else "cpu"
model_name = "ViT-L/14@336px"
model_path = "clip_logistic_regression.pkl"

# 1. 加载 CLIP 模型 (用于特征提取)
print(f"正在加载 CLIP 模型 ({model_name}) 到 {device}...")
clip_model, clip_preprocess = clip.load(model_name, device=device)
clip_model.eval() # 设置为评估模式

# 2. 加载训练好的 Logistic Regression 分类器
print(f"正在加载训练好的分类器模型 {model_path}...")
with open(model_path, "rb") as f:
    loaded_classifier = pickle.load(f)

print("模型加载完成。")

# 3. 准备一张待预测的图片 (你需要替换为你的图片路径)
# 这里假设 'data/positive' 中有一张图片，或者你可以上传一张新的图片
# 为了演示，我们随机选择一个现有图片，或者你可以创建一个新的测试图片

# 假设我们用一个正样本路径作为例子
example_image_path = './data/test/cat2.jpeg' # 请替换为实际的图片路径

# 如果你没有这个路径的图片，你可以创建一个虚拟的图片文件进行测试
if not os.path.exists(example_image_path):
    print(f"警告: 示例图片 {example_image_path} 不存在。请替换为你的图片路径或上传一张图片。")
    # 为了让代码能够运行，我们可以尝试使用训练数据中的第一张图片 (如果存在)
    positive_dir = './data/positive'
    if os.path.exists(positive_dir):
        first_positive_image = next((os.path.join(positive_dir, f) for f in os.listdir(positive_dir) if f.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))), None)
        if first_positive_image:
            example_image_path = first_positive_image
            print(f"使用 {first_positive_image} 作为示例图片。")
        else:
            print("没有找到任何正样本图片用于示例预测。请确保有图片在 './data/positive' 目录中。")
            example_image_path = None # Set to None if no image found
    else:
        print("正样本目录 './data/positive' 不存在，无法获取示例图片。")
        example_image_path = None

if example_image_path:
    # 4. 进行预测
    print("\n--- 进行预测 ---")
    predicted_label, prediction_proba = predict_image(
        example_image_path,
        clip_model,
        clip_preprocess,
        loaded_classifier,
        device
    )
else:
    print("无法进行预测，因为没有可用的示例图片。")


# 配置参数 (与训练时保持一致)
device = "cuda" if torch.cuda.is_available() else "cpu"
model_name = "ViT-L/14@336px"
model_path = "clip_logistic_regression.pkl"

# 1. 加载 CLIP 模型 (用于特征提取)
print(f"正在加载 CLIP 模型 ({model_name}) 到 {device}...")
clip_model, clip_preprocess = clip.load(model_name, device=device)
clip_model.eval() # 设置为评估模式

# 2. 加载训练好的 Logistic Regression 分类器
print(f"正在加载训练好的分类器模型 {model_path}...")
with open(model_path, "rb") as f:
    loaded_classifier = pickle.load(f)

print("模型加载完成。")

# 3. 准备一张待预测的图片 (你需要替换为你的图片路径)
# 这里假设 'data/positive' 中有一张图片，或者你可以上传一张新的图片
# 为了演示，我们随机选择一个现有图片，或者你可以创建一个新的测试图片

# 假设我们用一个正样本路径作为例子

folder = './data/test'
file_paths = [os.path.join(folder, f) for f in os.listdir(folder)]
for file_path in file_paths:
  #example_image_path = './data/test/cat2.jpeg' # 请替换为实际的图片路径
  example_image_path = file_path

  if example_image_path:
      # 4. 进行预测
      print("\n--- 进行预测 ---")
      predicted_label, prediction_proba = predict_image(
          example_image_path,
          clip_model,
          clip_preprocess,
          loaded_classifier,
          device
      )
  else:
      print("无法进行预测，因为没有可用的示例图片。")

folder = './data/test'
file_paths = [os.path.join(folder, f) for f in os.listdir(folder)]
for file_path in file_paths:
    print(file_path)

# 3. 准备一张待预测的图片 (你需要替换为你的图片路径)
# 这里假设 'data/positive' 中有一张图片，或者你可以上传一张新的图片
# 为了演示，我们随机选择一个现有图片，或者你可以创建一个新的测试图片

# 假设我们用一个正样本路径作为例子

folder = './data/test'
file_paths = [os.path.join(folder, f) for f in os.listdir(folder)]
for file_path in file_paths:
  #example_image_path = './data/test/cat2.jpeg' # 请替换为实际的图片路径
  example_image_path = file_path

  if example_image_path:
      # 4. 进行预测
      print("\n--- 进行预测 ---")
      predicted_label, prediction_proba = predict_image(
          example_image_path,
          clip_model,
          clip_preprocess,
          loaded_classifier,
          device
      )
  else:
      print("无法进行预测，因为没有可用的示例图片。")