#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: 搜狗日志案例的需求分析
   SourceFile  :	operator_sogou_web_count.py
   Software    :    PyCharm
   Author      :	CZP.Ronaldo
   Date	       :	2022/10/28
   Time        :    20:12
-------------------------------------------------
"""

from pyspark import SparkContext, SparkConf
import os
import re, jieba

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
    input_rdd = sc.textFile("../datas/sogou")
    # print(input_rdd.first())

    # step2: 处理数据

    word_rdd = (
        # 将字符串切成列表
        input_rdd.map(lambda line: re.split("\\s+", line.strip()))
        # 过滤数据，字段元素不为6的过滤掉，过滤算子不会改变原来数据的格式
        .filter(lambda line: len(line) == 6)
        # 取出列表中需要的元素
        # jieba分词器对字符串进行截取以后
        # 返回的结果是一个迭代生成器，他不能直接被map作用
        # 要转换类型为元组或者列表等类型，才能使用map算子
        # 要是用list转换为一个列表的话，列表中的每个元素，每个词之间是用空格间隔
        # todo 字符串截取步长的形式 包左不包右 下标从0开始
        .flatMap(lambda line: jieba.cut(line[2][1:len(line[2])-1]))
        # 构建二元组
        # .flatMap(lambda line: (re.split("\\s+", line), 1))
        .map(lambda word: (word, 1))
        # 使用reduceByKey进行分组聚合计算结果
        .reduceByKey(lambda tmp, item: tmp+item)
        # 根据聚合的结果进行排序，排序的方式为降序
        .sortBy(keyfunc=lambda tuple: tuple[1], ascending=False)

    )
    print(word_rdd.take(10))
    # step3: 保存结果

    # todo:3-关闭SparkContext
    sc.stop()
