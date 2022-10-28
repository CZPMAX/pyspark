#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	operator_fold.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/26
   Time        :    17:56
-------------------------------------------------
"""

from pyspark import SparkContext, SparkConf, TaskContext
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
    # 构建rdd numSlices指定分区个数
    input_rdd = sc.parallelize(range(1, 11), numSlices=2)

    def compute_sum(tmp, item):
        # 输出每个分区中的tmp和item
        print(f"part={TaskContext().partitionId()}, tmp={tmp}, item={item}")
        # 返回累加
        return tmp + item

    def compute_mix(tmp, item):
        # 输出每个分区中的tmp和item
        print(f"part={TaskContext().partitionId()}, tmp={tmp}, item={item}")
        # 返回累加
        return tmp * item
    # todo==========================  reduce fold aggregate =========================
    # 这三个都是触发算子,它们返回的是聚合的结果,也就是一个具体的数字
    # 触发算子因为返回的东西的类型不同,所以我们要视情况去将结果取出来
    # 例如 first() 有时候取的是一个列表 所以我们print时需要使用*将列表中的元素取出, 而reduce返回的就是一个数值,直接打印就好
    # ===============================================================================

    # ================================= reduce ======================================
    # reduce 没有初始值,分区内部与分区间的运算逻辑一致
    # reduce_rs = input_rdd.reduce(lambda tmp, item: compute_sum(tmp, item))
    # print(reduce_rs)
    # ===============================================================================

    # ================================= fold ========================================
    # fold 有初始值,但是分区内部与分区间的运算逻辑相同
    # foldRs = input_rdd.fold(1, op=lambda tmp, item: compute_sum(tmp, item))
    # print(foldRs)
    # ===============================================================================

    # ================================= aggregate ===================================
    # aggregate 有初始值,但是可以指定分区间的计算逻辑与分区内的计算逻辑
    agg_rs = input_rdd.aggregate(1, seqOp=lambda tmp, item: compute_mix(tmp, item),
                                 combOp=lambda tmp, item: compute_sum(tmp, item))
    print(agg_rs)
    # ===============================================================================
    # todo:3-关闭SparkContext
    sc.stop()

    # todo=================== reduceByKey,foldByKey,aggregateByKey =======================
    # 其他的两个转换算子foldByKey,aggregateByKey都是跟reduceByKey一样,根据key分组,然后再根据value聚合
    # 唯一不一样的是他们两个聚合计算的时候有初始值,而且aggregateByKey还能自定义分区内部的计算方式与分区间的计算方式
    # todo 解惑? 如何实现的?
    # 其实这个也就比前面的触发算子多了一步操作,就是把相同的key分到一组内,然后把它们的value放一块
    # 也就是这样("hadoop","1 1 1 1 1")
    # 然后我们就是接着对这个二元组第二个元素进行操作,与reduceByKey不同的是,在使用foldByKey,aggregateByKey时
    # 要指定一个初始值,如果有需要可以使用aggregateByKey自定义分区内部与分区间的聚合计算方式
    # ====================================================================================

