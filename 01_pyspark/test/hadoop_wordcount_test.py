# coding=gbk
from pyspark import SparkContext, SparkConf
import os
import re

if __name__ == '__main__':
    # hadoop系统
    # 配置JDK的路径，就是前面解压的那个路径
    os.environ['JAVA_HOME'] = '/export/servers/jdk1.8.0_65'
    # 配置Hadoop的路径，就是前面解压的那个路径
    os.environ['HADOOP_HOME'] = '/export/servers/hadoop-3.3.0'
    # 配置base环境Python解析器的路径
    os.environ['PYSPARK_PYTHON'] = '/export/servers/anaconda3/bin/python3'
    # 配置base环境Python解析器的路径
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/export/servers/anaconda3/bin/python3'
    # todo:1-构建SparkContext
    conf = SparkConf().setMaster("spark://hadoop200:7077").setAppName("Remote Test APP")
    sc = SparkContext(conf=conf)

    # todo:2-数据处理：读取、转换、保存
    # step1: 读取数据
    input_rdd = sc.textFile("hdfs://hadoop200:8020/spark/wordcount/input")
    # step2: 处理数据
    # step3: 保存结果

    rs_rdd = (
               # 过滤空行前,都需要去除头尾空格因为在算长度时空格也会被算上长度
               input_rdd.filter(lambda line: len(line.strip()) > 0)
               # 一行单词，转换成多行单词,flatMap爆炸函数 先map再flat
               .flatMap(lambda line: re.split("\\s+", line))
               # 变多个行每行一个单词以后，使用map映射成二元组
               .map(lambda word: (word, 1))
               # 根据key分组, 再根据value聚合计算
               .reduceByKey(lambda item, tmp: item+tmp)
    )
    # 保存结果
    rs_rdd.saveAsTextFile("hdfs://hadoop200:8020/spark/wordcount/output3")
    # todo:3-关闭SparkContext
    sc.stop()
