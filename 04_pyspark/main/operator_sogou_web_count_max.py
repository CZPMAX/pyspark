#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	operator_sogou_web_count_max.py
   Software    :    PyCharm
   Author      :	CZP.Ronaldo
   Date	       :	2022/10/28
   Time        :    23:01
-------------------------------------------------
"""

import os
import re, time

from pyspark import SparkContext, SparkConf

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

    # step2: 处理数据,过滤脏数据，将字符串转换为列表
    def to_tuple(line):
        rs = re.split("\\s+", line.strip())
        return (rs[0], rs[1], rs[2][1:len(rs[2])-1], rs[3], rs[4], rs[5])


    etl_rdd = (
        # 去除空行,和元素个数不为6的数据
        input_rdd.filter(lambda line: len(re.split("\\s+", line.strip())) == 6)
        # 将剩下的数据进行切割,返回一个元组
        .map(lambda line: to_tuple(line))

    )

    # 因为后续的rdd的转换还会用到这个rdd的数据，所以可以将它缓存起来
    etl_rdd.cache()

    # TODO 统计所有搜索中，用户在搜索时，最多、最少、平均搜索次数
    rs1_rdd = (
              # 从元组中取出我们需要的元素分别是用户id，搜索关键词,顺便构成二元组
              etl_rdd.map(lambda item: ((item[1], item[2]), 1))
              # 每个用户搜索一个搜索词分组，再聚合计算搜索这个搜索词用了几次
              .reduceByKey(lambda tmp, item: tmp+item)
              # 取出聚合结果的value值,返回的是列表，触发算子



    )

    rs1_rdd.cache()
    # step3: 保存结果
    # todo 第一次触发
    print(f"用户最大的搜索次数为{rs1_rdd.values().max()}")
    # todo 第二次触发
    print(f"用户最小的搜索次数为{rs1_rdd.values().min()}")
    # todo 第三次触发
    print(f"用户平均的搜索次数为{rs1_rdd.values().mean()}")
    print(" ====================================================== ")

    rs1_rdd.unpersist(blocking=True)
    # TODO 实统计每个小时的搜索量，按照搜索量降序排序
    #  这里的数据只有一天的所以不用考虑 不同天 小时的不同
    #  top takeOrder都是针对kv类型的数据的，根据key进行排序 top是降序 takeOrder是升序
    rs2 = (
            # 从etl_rdd中取出我们所需要的元素, 并且构建成一个二元组
            # key-时间(小时) value-1
            etl_rdd.map(lambda item: (item[0][0:2], 1))
            # 根据时间分组，因为小时一天最多只有24个小时，所以分组最多只有24个，它没必要放在两个分区内
            # 降低分区
            .coalesce(1)
            # 根据时间小时分组，再聚合计算
            .reduceByKey(lambda tmp, item: tmp+item)
            # 对调kv为了实现top的排序
            .map(lambda item: (item[1], item[0]))
            # 取出最高的24个数据，触发算子
            # todo 第二次触发 缓存生效，etl_rdd不用重新构建
            .top(24)
    )
    print(*rs2)
    time.sleep(180)
    # todo:3-关闭SparkContext
    sc.stop()
