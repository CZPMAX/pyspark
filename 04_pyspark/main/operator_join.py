#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	operator_join.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/27
   Time        :    20:41
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
    # step1: 读取数据
    rdd_singer_age = sc.parallelize(
    [("周杰伦", 43), ("陈奕迅", 47), ("蔡依林", 41), ("林子祥", 74), ("陈升", 63)],
                                    numSlices=2)
    rdd_singer_music = sc.parallelize(
    [("周杰伦", "青花瓷"), ("陈奕迅", "孤勇者"), ("蔡依林", "日不落"), ("林子祥", "男儿当自强"),
    ("动力火车", "当")], numSlices=2)

    # todo =============================== join测试 =================================
    # 左接测试 rdd接rdd返回的时一个新的rdd,转换算子
    rdd_left_join = rdd_singer_age.leftOuterJoin(rdd_singer_music)
    rdd_left_join.foreach(lambda x: print(x))
    print(rdd_left_join.count())
    print(" ====================================================== ")
    # todo 结果返回一个二元组，第一个元素是左表的key，第二个元素是左表的value和右表的value组成的元组
    #  只有接上了右表的value才有值，没有接上的话，右表的value就为none

    # 内连接测试 转换算子
    inner_join = rdd_singer_age.join(rdd_singer_music)
    inner_join.foreach(lambda x: print(x))
    print(inner_join.count())
    print(" ====================================================== ")

    # 全连接测试 转换算子
    full_join = rdd_singer_age.fullOuterJoin(rdd_singer_music)
    full_join.foreach(lambda x: print(x))
    print(full_join.count())
    print(" ====================================================== ")

    # 右连接测试
    right_join_rdd = rdd_singer_age.rightOuterJoin(rdd_singer_music)
    right_join_rdd.foreach(lambda x: print(x))
    print(right_join_rdd.count())
    print(" ====================================================== ")

    # todo:3-关闭SparkContext
    sc.stop()
