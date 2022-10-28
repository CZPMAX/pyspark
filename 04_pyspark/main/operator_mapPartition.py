#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	operator_mapPartition.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/28
   Time        :    19:48
-------------------------------------------------
"""

from pyspark import SparkContext, SparkConf
import os, time


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
    input_rdd = sc.parallelize((1, 2, 3, 4, 5, 6, 7, 8, 9, 10), numSlices=2)
    # step2: 处理数据
    # 使用map每个元素处理一次
    # map_rdd = input_rdd.map(lambda x: x*2)
    # print(map_rdd.collect())

    # todo 使用mapPartition函数，让map函数作用于每个分区，接着分区内部对数据进行操作
    #  mapPartitions：每个分区处理一次
    def partition_value(part):
        rs = [i*2 for i in part]
        return rs

    map_par_rdd = input_rdd.mapPartitions(lambda part: partition_value(part))
    print(map_par_rdd.collect())
    # step3: 保存结果
    time.sleep(100)
    # todo:3-关闭SparkContext
    sc.stop()
