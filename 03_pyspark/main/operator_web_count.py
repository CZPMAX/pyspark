#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	operator_web_count.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/26
   Time        :    16:15
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
    input_rdd = sc.textFile("/root/pyspark_code/03_pyspark/datas/function_data/tianchi_user.csv")
    # step2: 处理数据
    # todo 明确业务需要我们干什么？统计pv也就是页面的访问量
    #   怎么定义页面的访问量呢？用户打开一次页面也就是代表着页面的访问量加一，只要打开一次页面就算
    #   不考虑一个用户访问多次的情况，也就是记录产生一次pv就加一
    #   所以依照记录生成的时间做count统计就能得出pv的值
    """ sql实现
        select
             substr(time,1,10) as daycode,
             count(1) as pv
        from table
        group by substr(time,1,10)
    """

    # todo 使用spark算子实现计算pv
    # 自定义函数把数据的每一行转换为适合spark分析的元组
    def to_tuple(line):
        arr = line.strip().split(",")
        # 字符串的截取是现在见过唯一一个包括左边又包括右边
        # 因为时间包括小时，而我们只需要具体的日期
        return (arr[0], arr[1], arr[2], arr[3], arr[4], arr[5][0:10])


    etl_rdd = (
               # 因为每条数据都有6个元素，所以去空后，进行切片操作的返回的列表里面的元素没有6个就为脏数据，需要去除
               input_rdd.filter(lambda line: len(line.strip().split(",")) == 6)
               # 使用map函数，对rdd中的每一个元素,调用自定义的转元组函数，最后取第6个元素，接着与1
               # 形成一个类似于('时间', 1) 的二元组
               .map(lambda line: (to_tuple(line)[5], 1))
               # 根据key进行分组，对value进行sum累加聚合
               .reduceByKey(lambda tmp, item: tmp+item)
               # 经历过上面订单一系列运算后，不需要继续运算了，降低分区为1，使用coalesce来降分区，不需要经过shuffler过程
               .coalesce(1, shuffle=False)
               # 然后根据时间进升序排序
               .sortByKey()

    )
    # print(etl_rdd.collect()) 这个把整个rdd打印出来了
    etl_rdd.foreach(lambda x: print(x))

    print(" ====================================================== ")

    # todo 使用spark算子实现uv唯一用户访问量的计算
    rs_rdd = (
              # 因为每条数据都有6个元素，所以去空后，进行切片操作的返回的列表里面的元素没有6个就为脏数据，需要去除
              input_rdd.filter(lambda line: len(line.strip().split(",")) == 6)
              # 使用map函数对rdd中的每一个元素进行操作,把每行数据转换为二元组即('用户id','时间time')的形式
              .map(lambda line: (to_tuple(line)[0], to_tuple(line)[5]))
              # 因为需要的数据具有唯一性，即同一天内，一个用户id只能有一条数据，所以进行去重操作
              .distinct()
              # 去完重以后数据就是符合要求的数据，而且我们不需要用户id,因为id所对应的时间就能代表这个用户
              # 所以取出时间再与1映射成一个二元组即可
              .map(lambda tuple: (tuple[1], 1))
              # 接着进行reduceByKey的操作, 根据时间分组，根据value值累加聚合
              .reduceByKey(lambda tmp, item: tmp+item)
              # 降低分区数
              .coalesce(1, shuffle=False)
              # 根据uv排序
              .sortBy(keyfunc=lambda tuple: tuple[1], ascending=False)

    )
    rs_rdd.foreach(lambda x: print(x))
    # step3: 保存结果

    # todo:3-关闭SparkContext
    sc.stop()
