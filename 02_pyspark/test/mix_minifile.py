#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	mix_minifile.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/24
   Time        :    19:18
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
    # 在读取文件时直接指定具体文件下，那么就会读取这下面的所有文件数据
    input_rdd = sc.textFile("../datas/ratings100/", minPartitions=2)
    # 这里面的文件都是小文件，分区的形成是由数据块的个数决定的，所以100个小文件，它的单个文件大小都不满1m，
    # 所以会形成100个分区,不出意外输出的分区个数为100
    # print(input_rdd.getNumPartitions())
    # step2: 处理数据
    input_rdd.foreach(lambda file: print(file))
    # step3: 保存结果

    # todo:3-关闭SparkContext
    sc.stop()
