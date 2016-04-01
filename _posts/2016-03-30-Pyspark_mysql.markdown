---
layout: post
title:  "Pyspark connect mysql"
date:   2016-03-31
category: 随手记
tagline: "Supporting tagline"
tags: [Pyspark,mysql,jdbc]
---

# pyspark 怎样将 rdd 写到 mysql

接上一篇 pyspark connect ES

### 1. 错误的尝试
开始，直接尝试用python 中torndb 自建函数写库，发现这种方法比较难，pyspark 在传递函数时，
现将函数转成字符串格式，然后在每个节点上解析函数，这就要求每个节点都要安装相应的库，
如果函数调用了别的自建的 class 或 function 就要求每个节点都能正确调用，这就比较麻烦，放弃。

### 2. jdbc to mysql
文档说明 [jdbc documentation](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=jdbc#pyspark.sql.DataFrame)
接下来尝试使用jdbc 写到 mysql， jdbc 的操作要求，所用的数据的格式是 dataframe， 因而，首先要将
rdd 转化成 dataframe

```python
from pyspark.sql import Row
out_df = out_rdd.map(lambda l: Row(**dict(l))).toDF()
out_df.show(4)
+-----+-------+-------+-------------+
|newid|request| target|    timestamp|
+-----+-------+-------+-------------+
|    0|1830004|2593438|1459267263513|
|    0|1352008|1898704|1459267343283|
|    0|  87201|  26893|1459267388806|
|    0|  87201|  26893|1459267380340|
+-----+-------+-------+-------------+
```

接下来 set mysql connect information, 写入就行了

```python
host_ip = '*.*.*.*'
port = '****'
user_name = 'user_name'
password = 'password'
# db information
dbname = 'dbname'
table = 'table_name'
url = "jdbc:mysql://%s:%s/%s"%(host_ip,port,dbname)
properties = {"user": user_name,"password": password}
out_df.write.jdbc(mode='append',url=url,table=table,properties=properties)

```

这一步也有可能会遇见问题：
问题1:

```python
py4j.protocol.Py4JJavaError: An error occurred while calling o87.jdbc.
: java.sql.SQLException: No suitable driver found for jdbc:mysql://*.*.*.*:****/****
```
这个错误和上一篇遇见的 error 相似，也是缺少一些包，需要加载

solution

```
pyspark --jars /bigdata/software/elasticsearch-hadoop-2.2.0/dist/elasticsearch-spark_2.10-2.2.0.jar,/bigdata/libs/mysql-connector-java-5.1.38.jar --conf spark.executor.extraClassPath=/bigdata/libs/mysql-connector-java-5.1.38.jar --driver-class-path /bigdata/libs/mysql-connector-java-5.1.38.jar

```

如果没有这些包，就需要下载一下，放到相应的路径，然后将上面的路径改为你的存放路径就行了。

问题2:

如果你的 dataframe 中列的顺序与mysql 中不对应，也会报错

```
out_df.show(2)
+-----+-------+-------+-------------+
|newid|request| target|    timestamp|
+-----+-------+-------+-------------+
|    0|1830004|2593438|1459267263513|
|    0|1352008|1898704|1459267343283|
+-----+-------+-------+-------------+

mysql table:
+-----+-------+----------+----------+
|newid|request| timestamp|    target|
+-----+-------+----------+----------+
|    0|1830004|4545454545|1459263313|
+-----+-------+----------+----------+

```

他将会按照列的先后顺序一一对应写：

out_df.newid     --> mysql table['newid']

out_df.request   --> mysql table['request']

out_df.target    --> mysql table['timestamp']

out_df.timestamp --> mysql table['target']

将会出现格式错误，数据混乱等，解决办法就是，调整out_df中列的顺序，将其变成和mysql中的一致

```python
columns = (u'newid',u'request',u'target',u'timestamp')
out_df.select(*columns).write.jdbc(mode='append',url=url,table=table,properties=properties)
```
就Ok 了。
