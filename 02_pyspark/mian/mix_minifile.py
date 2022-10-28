#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	mix_minifile.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/24
   Time        :    19:49
-------------------------------------------------
"""

from pyspark import SparkContext, SparkConf
import os

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
    # step1:读取数据 使用textfIle读取文件，因为文件数据的大小只有1m左右,而一个块大小为128m
    # 所以100个文件会开启100个分区dd
    # input_rdd = sc.textFile("/root/pyspark_code/02_pyspark/datas/ratings100")
    # print(input_rdd.getNumPartitions())

    # 耗费资源所以换成合并小文件的读取方式并指定最小的分区数
    input_rdd = sc.wholeTextFiles("/root/pyspark_code/02_pyspark/datas/ratings100", minPartitions=2)
    # 再次读取分区个数
    # print(input_rdd.getNumPartitions()) 合并小文件后，分区数为2
    # print(input_rdd.first()) 打印第一条数据发现每个文件以kv二元组的形式存放在rdd弹性分布式数据集中 k --> 文件路径 v --> 文件内容
    # step2: 处理数据 这里匿名函数的操作是 获取每个元素二元组的第二个元素，进行以\n为分隔符的切割,返回的是一个列表
    # 再利用flat扁平化操作将列表去除,此时的rdd内存放的数据就是切割后的每一个元素
    line_rdd = input_rdd.flatMap(lambda content: content[1].split("\n"))
    # todo 利用for循环，循环去除此时新的rdd数据集的前三个元素，分别打印输出
    for item in line_rdd.take(5):
        print(item)
    # step3: 保存结果

    # todo:3-关闭SparkContext
    sc.stop()
