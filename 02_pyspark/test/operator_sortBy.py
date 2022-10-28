#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO: 对非kv类型的数据进行排序
   SourceFile  :	operator_sortBy.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/23
   Time        :    16:14
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
    input_rdd = sc.textFile("../datas/function_data/sort.txt")
    # step2: 处理数据
    # sortBy算子：数据量大，RDD非KV类型RDD
    # 先定义一个函数，对数据源的数据进行截取

    # def sub_age(line):
    #     age_rdd = line.strip().split(",")
    #     return age_rdd[1]
    #
    # # print(sub_age().collect())
    # sort_rdd = (input_rdd.filter(lambda line: len(line.strip()) > 0)
    #             .sortBy(keyfunc=lambda line: sub_age(line), ascending=False))
    # sort_rdd.foreach(lambda x: print(x))

    # step3: 保存结果
    # step2: 处理数据
    def split_line(line):
        arr = line.strip().split(",")
        return (arr[0], arr[1], arr[2])


    # sortByKey算子：数据量，RDD就是KV类型RDD
    # 手动将非KV类型转换成KV类型(年龄，名字+性别)
    kv_rdd = input_rdd.map(lambda line: split_line(line)).map(lambda tuple: (tuple[1], (tuple[0], tuple[2])))
    # 实现排序
    sort_by_key_rdd = kv_rdd.sortByKey(ascending=False)

    # step3: 保存结果
    sort_by_key_rdd.foreach(lambda x: print(x))
    # todo:3-关闭SparkContext
    sc.stop()
