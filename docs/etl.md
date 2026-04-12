# spark
```
什么时候才需要 multiline=true？只有当你的 JSON 文件是下面这种结构时才需要（整个文件是一个大对象，内部有换行）：json

{
  "records": [
    {"name": "zhangsan", "age": 12},
    {"name": "zhangsan1", "age": 12222}
  ]
}
或者一个超大的单个 JSON 对象跨多行。你的文件明显是 每行一个对象，所以去掉 multiline 选项即可。
```

# docs
```
https://x.com/i/grok?conversation=2042826416224637271
```