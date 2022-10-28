#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	operator_kv_others.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/26
   Time        :    19:05
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
    rdd_kv = sc.parallelize([('laoda', 11), ('laoer', 22), ('laosan', 33), ('laosi', 44)], numSlices=2)

    # todo 将所有的key取出放入一个新的rdd中
    #    转换算子
    rdd_keys = rdd_kv.keys()
    # rdd_keys.foreach(lambda x: print(x))
    print(rdd_keys.collect())
    # =================================================================
    print(" ====================================================== ")

    # todo 将所有的value取出放入一个新的rdd中
    #    转换算子
    rdd_values = rdd_kv.values()
    print(rdd_values.collect())
    # =================================================================
    print(" ====================================================== ")

    # todo 使用map函数作用于每个value,转换为一个新的rdd
    #    转换算子
    rdd_map = rdd_kv.mapValues(lambda x: x*10)
    print(rdd_map.collect())
    # =================================================================
    print(" ====================================================== ")

    # todo 将RDD中元素的结果放入Driver内存中的一个字典中，数据量必须比较小
    #     因为需要整个字典的数据与结构一起输出所以直接print即可
    # collectAsMap: 这个Map叫做Map集合，等同于Python中的字典Dict
    # todo 触发算子
    rdd_dict = rdd_kv.collectAsMap()
    print(rdd_dict)

    # todo:3-关闭SparkContext
    sc.stop()
