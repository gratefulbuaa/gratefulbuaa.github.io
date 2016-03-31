---
layout: post
title:  "Pyspark connect ES"
date:   2016-03-31
category: 随手记
tagline: "Supporting tagline"
tags: [Pyspark, elasticsearch,]
---


# pyspark 怎样从 elasticsearch 中取数据

遇见如下几个问题：

### 1. connect elasticserch Error

```
py4j.protocol.Py4JJavaError: An error occurred while calling z:org.apache.spark.api.python.PythonRDD.newAPIHadoopRDD.
: java.lang.ClassNotFoundException: org.elasticsearch.hadoop.mr.LinkedMapWritable
```

**solution:**
1. download elasticsearch-spark_2.10-2.2.0.jar [download](http://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark_2.10) or search on http://mvnrepository.com
2. put it on your server
3. start pyspark with the jar path: # 将路径改为自己的存放路径

```
pyspark --jars /bigdata/software/elasticsearch-hadoop-2.2.0/dist/elasticsearch-spark_2.10-2.2.0.jar
```

### 2. query 语法
先看看example message 长啥样：

```
{
  "took": 16,
  "timed_out": false,
  "_shards": {
    "total": 38,
    "successful": 38,
    "failed": 0
  },
  "hits": {
    "total": 7204,
    "max_score": 6.9001007,
    "hits": [
      {
        "_index": "logstash-2016.03.31",
        "_type": "tracklog",
        "_id": "AVPJ-*****-1u6LW",
        "_score": 6.9001007,
        "_source": {
            ...
            ...
            "@version": "1",
                      "@timestamp": "2016-03-31T00:01:04.576Z",
                      "type": "tracklog",
                      "host": "python1",
                      "path": "/mnt/ufo_logs/track_10007",
                      "tags": [
                        "multiline"
                      ],
             "RequestBody": "{\"timestamp\": 1459382464243, \"cmdid\": 10017, \"params\": {\"handle_type\": 4, \"pairid\": 402608}, \"common\": {\"timestamp\": 1459382464243, \"userid\": 500391, \"platform\": 1, \"cmdid\": 10017, \"version\": \"3.2.0a\", \"userkey\": \"******\"}}",
             ...
             ...
             "cmdid": "10017",


 ```
想获取的数据是：cmdid= 10017, 且"RequestBody"中params 中的handle_type ＝ 4，同时满足两个条件的数据。
在数据库中handle_type 有4个取值（1，2，3，4）,
先用query 和filter 尝试：

```
{
"query": {
        "filtered": {
            "query":  { "match": { "RequestBody": "'handle_type': 1" }},
            "filter": { "term": { "cmdid": "10017" }}
        }
     }
}
```

发现不对，不能正确的区别handle_type 的内容：
handle_type =1 ,handle_type =4, 数据的数量一样，说明query语句没有正确的取出

**solution**

```
{"query": {
            "filtered": {
                "query": {
                    "query_string": {
                        "query": "cmdid:10017  AND RequestBody:\"'handle_type': 4\"",
                        "analyze_wildcard": true
                    }
                }
              }}}
```

### pyspark code:
取出过去24小时的数据

```python
import json
import time
# elasticserch connect information  
es_ip = '*.*.*.*'
port = '****'
resource = 'logstash-*'

time_end = int(time.time()*1000)
time_start = int(time_end - 3600*24*1000)

query_dict = {"query": {
            "filtered": {
                "query": {
                    "query_string": {
                        "query": "cmdid:10017  AND RequestBody:\"'handle_type': 4\"",
                        "analyze_wildcard": True
                    }
                },
                "filter": {
                    "bool": {
                        "must": [
                            {"range": {
                                    "@timestamp": {
                                        "gte": time_start,
                                        "lte": time_end
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
}

query_str = json.dumps(query_dict)
read_conf = {
    'es.nodes': es_ip,
    'es.port': port,
    'es.resource':resource,
    'es.query':query_str
    }
es_rdd = sc.newAPIHadoopRDD(
    inputFormatClass = 'org.elasticsearch.hadoop.mr.EsInputFormat',
    keyClass = 'org.apache.hadoop.io.NullWritable',
    valueClass = 'org.elasticsearch.hadoop.mr.LinkedMapWritable',
    conf = read_conf)

```

es_rdd 就是取出的数据了，下面还需要从中取出，想要的信息，这里用函数 **c_str_dict(in_str)** 取出想要的信息，返回dict：


```python
def c_str_dict(in_str):
    # convert string to dict
    dict_tem = json.loads(in_str)
    request = dict_tem[u'common'][u'userid']
    target = dict_tem[u'params'][u'pairid']
    visit_time = dict_tem[u'timestamp']
    return {u'newid':0,u'request':request,u'target':target,u'timestamp':visit_time}

a = es_rdd.first()
print c_str_dict(a[1]['RequestBody'])
{u'request': 1830004, u'newid': 0, u'target': 2593438, u'timestamp': 1459267263513}
out_rdd = es_rdd.map(lambda x:c_str_dict(x[1]['RequestBody']))
```

out_rdd 就是结果了，下面可以存为文件，也可以写到 mysql table
下一篇将说明：pyspark 怎样将 rdd 写到 mysql
