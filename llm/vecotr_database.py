#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0

'''
专用向量数据库：

Pinecone - 完全托管的云服务，使用简单但仅云端部署
Milvus - 开源分布式向量数据库，支持多种索引算法，可本地或云端部署
Weaviate - 开源向量数据库，原生支持GraphQL，内置向量化模块
Qdrant - 开源向量数据库，用Rust编写，性能优异，提供丰富的过滤功能
Chroma - 轻量级开源向量数据库，专为AI应用设计，易于集成
MyScale - 的开源情况：
MyScale在GitHub上开源，它是基于ClickHouse开发的一个分支版本，专门优化了向量搜索和全文搜索功能 GitHub。你可以在GitHub上找到官方仓库 myscale/MyScaleDB。
MyScale的特点：
它的独特之处在于将SQL数据库、向量数据库和全文搜索引擎统一到一个系统中。MyScale基于ClickHouse这个流行的开源分析数据库构建，利用了其列式存储、高级压缩和SIMD处理等优势 GitHub。
传统数据库的向量扩展：

pgvector - PostgreSQL的向量扩展插件
Elasticsearch - 8.0+版本支持向量搜索
Redis - 通过RediSearch模块支持向量搜索
MongoDB Atlas - 提供向量搜索功能
'''