#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
import os

"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	operator_test.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/23
   Time        :    14:53
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
    list1 = [1, 2, 3, 4, 4, 4, 5, 6]
    list2 = [1, 6, 8, 9, 4, 6, 6, 9]
    # step1: 读取数据 能直接使rdd进行计算，collect是行动算子
    input_list_rdd = sc.parallelize(list1)
    # print(input_list_rdd.collect())
    input_list_rdd_se = sc.parallelize(list2)
    # print(input_list_rdd_se.collect())
    # step2: 处理数据

    # todo distinct操作 直接运行，不会得出算子的操作结果，这是转换算子
    distinct_rdd = input_list_rdd.distinct()
    # print(distinct_rdd.collect())

    # todo union操作 转换算子返回的结果一定是rdd 行动算子返回的结果一定是运算的结果
    union_rdd = input_list_rdd.union(input_list_rdd_se)

    # todo 要对rdd中的元素进行输出的话，一定得使用行动算子来，使用print直接打印rdd，打印的是rdd对象
    # 打印的是rdd弹性分布式集合中的元素
    # print(union_rdd.collect())

    # 打印的是对于
    print(union_rdd)
    # step3: 保存结果

    # todo:3-关闭SparkContext
    sc.stop()
