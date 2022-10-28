#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
import os
import re

"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	operator_groupByKey.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/23
   Time        :    15:30
-------------------------------------------------
"""

if __name__ == '__main__':
    # todo:0-设置系统环境变量
    # 配置JDK的路径，就是前面解压的那个路径
    os.environ['JAVA_HOME'] = 'C:\Program Files\Java\jdk1.8.0_172'
    # 配置Hadoop的路径，就是前面解压的那个路径
    os.environ['HADOOP_HOME'] = 'Z:\hadoop3_window\hadoop-3.3.0'
    # 配置base环境Python解析器的路径
    os.environ['PYSPARK_PYTHON'] = 'Z:\Miniconda3\python.exe'
    # 配置base环境Python解析器的路径
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'Z:\Miniconda3\python.exe'
    # 申明当前以root用户的身份来执行操作
    os.environ['HADOOP_USER_NAME'] = 'root'

    # todo:1-构建SparkContext
    conf = SparkConf().setMaster("local[2]").setAppName("App Name")
    sc = SparkContext(conf=conf)

    # todo:2-数据处理：读取、转换、保存
    # step1: 读取数据
    input_rdd = sc.textFile("../datas/word.txt")
    # step2: 处理数据,使用转换算子，返回的不是计算结果，而是新的分布式数据集rdd
    tuple_rdd = (
                 input_rdd.filter(lambda line: len(line.strip()) > 0)
                 # 扁平化操作，并且对其其中每个数据进行处理
                 .flatMap(lambda line: re.split("\\s+", line.strip()))
                 # 将转换出来的每一个单词，转换成二元组的形式
                 .map(lambda word: (word, 1))
    )
    # tuple_rdd.foreach(lambda x: print(x))

    # todo 使用groupByKey+map 先对key进行分组，在使用map对每一个元素的value进行聚合
    group_rdd = tuple_rdd.groupByKey()
    group_rdd.foreach(lambda x: print(x[0], "---->", *x[1]))

    # 使用map进行聚合计算
    # re_rdd = group_rdd.map(lambda x: (x[0], sum(x[1])))
    # re_rdd.foreach(lambda x: print(x))

    # todo:3-关闭SparkContext
    sc.stop()
