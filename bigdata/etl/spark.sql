-- https://claude.ai/chat/1999584a-72c7-4400-8133-265f75676dcc
-- https://blog.devops.dev/the-complete-practitioners-guide-to-pyspark-sql-functions-147dc85f4058
-- https://www.huaxiaozhuan.com/%E5%B7%A5%E5%85%B7/spark/chapters/03_dataframe.html

-- ❶ 提取单字段
SELECT get_json_object(data, '$.name') FROM table_name;

-- ❷ 提取多字段（推荐）
SELECT json_tuple(data, 'id', 'name', 'age') as (id, name, age) FROM table_name;

-- ❸ 完整解析
SELECT
    d.name, d.age
FROM (
    SELECT from_json(data, 'struct<name: string, age: int>') as d FROM table_name
);

-- ❹ 转为JSON
SELECT to_json(struct(id, name, age)) FROM table_name;

-- ❺ 数组展开
SELECT explode(from_json(json_array, 'array<struct<id: int, name: string>>')) FROM table_name;



