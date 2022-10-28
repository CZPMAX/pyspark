#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	wordcount.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/20
   Time        :    15:42
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

    # todo:1-构建SparkContext
    conf = SparkConf().setMaster("local[2]").setAppName("App Name")
    sc = SparkContext(conf=conf)

    # todo:2-数据处理：读取、转换、保存
    # step1: 读取数据
    # step2: 处理数据
    # step3: 保存结果

    # todo:3-关闭SparkContext
    sc.stop()
