#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	operator_rep_coalesce.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/26
   Time        :    9:58
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
    num_rdd = sc.parallelize(range(1, 11))
    # print(num_rdd.collect())
    print(f"num_rdd的分区个数为{num_rdd.getNumPartitions()}")

    num_rdd.foreach(lambda x: print(x))
    print(" ============================================ ")

    # todo 调用repartition将rdd分区数构建为4
    up_rdd = num_rdd.repartition(4)
    print(f"up_rdd的分区个数为{up_rdd.getNumPartitions()}")
    up_rdd.foreach(lambda x: print(x))

    print(" ============================================ ")
    # todo 构建coalesce来降低分区数,它一般就是用来降低分区数的
    #  本质上repartition与coalesce是一个东西
    #  repartition底层就是调用coalesce来实现分区数的增加
    #  但是前者的shuffler是写死的，也就是说增大分区时必须经过shuffler
    #  而coalesce可以自由选择是否要经过shuffler
    down_rdd = num_rdd.coalesce(numPartitions=1, shuffle=False)
    print(f"down_rdd的分区个数为{down_rdd.getNumPartitions()}")
    sort_list = down_rdd.takeOrdered(5)
    print(*sort_list)
    down_rdd.foreach(lambda x: print(x))

    # todo:3-关闭SparkContext
    sc.stop()
