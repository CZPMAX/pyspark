#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	operator_jieba.py
   Software    :    PyCharm
   Author      :	CZP.Ronaldo
   Date	       :	2022/10/28
   Time        :    20:27
-------------------------------------------------
"""

import os

import jieba
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

    # 定义一个字符串
    line = '我来到北京清华大学'

    # TODO: 全模式分词
    #  将句子中所有可以组成词的词语都扫描出来,
    #  速度非常快，但可能会出现歧义
    seg_list = jieba.cut(line, cut_all=True)
    # 将分开的词用,号连接
    print(",".join(seg_list))
    print(" ====================================================== ")

    # todo ======================= 有依赖关系 ========================
    # TODO: 精确模式
    #  将句子最精确地按照语义切开
    #  适合文本分析，提取语义中存在的每个词
    seg_list_2 = jieba.cut(line, cut_all=False)
    # print(",".join(seg_list_2))
    print(type(seg_list_2))
    print(" ====================================================== ")

    # TODO: 搜索引擎模式
    #  在精确模式的基础上
    #  对长词再次切分，适合用于搜索引擎分词
    seg_list_3 = jieba.cut_for_search(line)
    print(','.join(seg_list_3))
    # todo:3-关闭SparkContext
    sc.stop()

    # TODO 结巴分词器在使用cut的时候，默认就是精确模式的
    #  在精确模式的基础上, 对它的结果的长词进行再次的分割，此时的关键词用到的不是cut了
    #  而是cut_for_search
