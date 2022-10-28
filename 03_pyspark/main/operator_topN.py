#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	operator_topN.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/25
   Time        :    20:05
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
    os.environ['HADOOP_CONF_DIR'] = '/export/servers/hadoop-3.3.0/etc/hadoop'
    os.environ['YARN_CONF_DIR'] = '/export/servers/hadoop-3.3.0/etc/hadoop'

    # todo:1-构建SparkContext
    conf = SparkConf().setMaster("local[2]").setAppName("Remote Test APP")
    sc = SparkContext(conf=conf)

    # todo:2-数据处理：读取、转换、保存
    # step1: 读取数据
    # todo top算子会将数据降序排序，即返回rdd中最大的前n个数据
    #  会将所有分区数据拉到内存中计算
    # TODO 因为topN算子会将所有数据放入内存中进行所有元素的排序，所以数据源的数据量要小
    input_rdd = sc.parallelize([num for num in range(1, 11)])
    # step2: 处理数据
    top_list = input_rdd.top(6)
    print(*top_list)
    # step3: 保存结果

    # todo takeOrdered算子 将rdd中的所有元素升序排序
    #  返回前n个元素，即返回最小的前n个元素
    ordered_rdd = sc.parallelize([1, 5, 2, 6, 9, 10, 4, 3, 8, 7])
    # todo top算子返回的都是列表，不是rdd，所以用*取出列表中的元素
    #  top算子为触发算子
    top_order_list = ordered_rdd.takeOrdered(5)
    print(*top_order_list)

    # todo:3-关闭SparkContext
    sc.stop()
