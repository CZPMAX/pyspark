# -*- coding = utf-8 -*-

# @time:2022/10/21 15:21

# Author:CZPMAX

# @File:

# @Software:PyCharm


# 导包
import os
from pyspark import SparkContext, SparkConf
import re

if __name__ == "__main__":

    # # hadoop系统
    # # 配置JDK的路径，就是前面解压的那个路径
    # os.environ['JAVA_HOME'] = '/export/servers/jdk1.8.0_65'
    # # 配置Hadoop的路径，就是前面解压的那个路径
    # os.environ['HADOOP_HOME'] = '/export/servers/hadoop-3.3.0'
    # # 配置base环境Python解析器的路径
    # os.environ['PYSPARK_PYTHON'] = '/export/servers/anaconda3/bin/python3'
    # # 配置base环境Python解析器的路径
    # os.environ['PYSPARK_DRIVER_PYTHON'] = '/export/servers/anaconda3/bin/python3'

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

    # 设置本地模式的spark
    conf = SparkConf().setMaster("local[2]").setAppName("Remote App Name")
    sc = SparkContext(conf=conf)

    # todo:2-数据处理：读取、转换、保存
    # step1: 读取数据
    # 通过驱动对象读取文件数据放入RDD中
    input_rdd = sc.textFile("hdfs://hadoop200:8020/spark/wordcount/input")
    # 打印RDD中内容
    # print(input_rdd.first())    # 打印第一个元素的内容
    # print(input_rdd.count())    # 打印RDD中元素的个数

    # 为了避免首尾空格的影响，进行操作时都要进行去空格的操作
    # step2: 处理数据
    # 1. 过滤空行
    # filter_Rdd = input_rdd.filter(lambda line:len(line.strip()) > 0)

    # 2. 切割元素
    # word_Rdd = filter_Rdd.map(lambda line:line.strip().split(" "))


    # 3. 爆炸函数，多行转多列
    # explode_Rdd = filter_Rdd.flatMap(lambda line : line.strip().split(" "))


    # 4. 将炸出来的列转映射成spark最喜欢的二元组
    # er_Rdd = explode_Rdd.map(lambda word:(word,1))
    # er_Rdd.foreach(lambda x: print(x))

    # 5. 按照二元组的key进行分组，再按照value进行聚合
    # result_Rdd = er_Rdd.reduceByKey(lambda item,tmp:item+tmp)
    # result_Rdd.foreach(lambda x: print(x))

    # todo 链式书写过程
    rs_rdd = (
               # 过滤空行
               input_rdd.filter(lambda line:len(line.strip()) > 0)
               # .map(lambda line:line.strip().split(" "))
               # 爆炸，切割后，使行转多列,一行多个单词转换为一行一个单词
               .flatMap(lambda line:re.split("\\s+",line))
               # 将每个元素映射成二元组
               .map(lambda word:(word,1))
               # 根据key分组，再根据value聚合
               .reduceByKey(lambda item,tmp:item+tmp)
    )
    # 这些对数据的处理其实是没有进行的，只有在下面rs_rdd.foreach()时使用这个rdd才会执行这些操作

    # rs_rdd = (  input_rdd
    #             # 过滤空行
    #             .filter(lambda line: len(line.strip()) > 0)
    #             # 一行多个单词转换为一行一个单词
    #             .flatMap(lambda line: line.strip().split(" "))
    #             # 将每个元素转换为二元组
    #             .map(lambda word: (word, 1))
    #             # 按照单词进行分组聚合
    #             .reduceByKey(lambda tmp,item: tmp+item)
    #          )

    # step3: 保存结果
    # 打印结果
    rs_rdd.foreach(lambda x: print(x))
    print(rs_rdd.collect())
    # 保存到文件中
    rs_rdd.saveAsTextFile(path="hdfs://hadoop200:8020/spark/wordcount/output6")

    # todo:3-关闭SparkContext
    sc.stop()

