#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	persist.py
   Software    :    PyCharm
   Author      :	CZP.Ronaldo
   Date	       :	2022/10/29
   Time        :    0:07
-------------------------------------------------
"""

from pyspark import SparkContext, SparkConf, StorageLevel
import os
import re
import time

if __name__ == '__main__':
    # todo:0-设置系统环境变量:全部换成Linux地址
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
    conf = SparkConf().setMaster("local[2]").setAppName("Remote Test APP")
    sc = SparkContext(conf=conf)

    # todo:2-数据处理：读取、转换、保存
    # step1: 读取数据
    input_rdd = sc.textFile("../datas/wordcount/word.txt")

    # step2: 处理数据
    rs_rdd = (input_rdd
              .filter(lambda line: len(line.strip()) > 0)
              .flatMap(lambda line: re.split("\\s+", line))
              .map(lambda word: (word, 1))
              .reduceByKey(lambda tmp, item: tmp + item)
              )

    # step3: 保存结果
    """cache：直接将RDD缓存在内存中，不允许指定缓存级别"""
    # rs_rdd.cache() # 本质上调用的还是persist
    """persist：将RDD缓存，允许用户自己缓存级别"""
    # rs_rdd.persist(StorageLevel.MEMORY_ONLY)  # 只缓存在内存，等同于cache
    rs_rdd.persist(StorageLevel.MEMORY_AND_DISK)

    # 第一次触发
    rs_rdd.foreach(lambda x: print(x))
    # 第二次触发
    print(rs_rdd.count())

    """unpersist：释放缓存，如果后续代码中，这个RDD明确不会再被使用，需要将数据从缓存中释放"""
    rs_rdd.unpersist(blocking=True)

    # todo:3-关闭SparkContext
    time.sleep(180)
    sc.stop()
