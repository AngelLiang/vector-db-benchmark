# 42memo测试说明

安装环境

```
poetry install
```

测试

```
python run.py
```

## 测试

注意事项：

- pgvector需要手动创建数据库。

## 测试结果

- pgvector上传数据的速率很慢
- qdrant可以建立本地数据库，上传数据速率很慢，但一般也用不到，使用glove-25-angular数据集无法得到结果
- lancedb当数据量大了之后，搜索会很慢，可能是因为是用文件建立索引，也可能是我代码中没有建立索引，所以使用glove-25-angular数据集无法得到搜索结果

