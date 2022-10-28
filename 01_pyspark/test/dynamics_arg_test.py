# coding=gbk
import os
from pyspark import SparkContext, SparkConf
import re
import sys
import time


if __name__ == "__main__":


    # ����JDK��·��������ǰ���ѹ���Ǹ�·��
    os.environ['JAVA_HOME'] = 'C:\Program Files\Java\jdk1.8.0_172'
    # ����Hadoop��·��������ǰ���ѹ���Ǹ�·��
    os.environ['HADOOP_HOME'] = 'Z:\hadoop3_window\hadoop-3.3.0'
    # ����base����Python��������·��
    os.environ['PYSPARK_PYTHON'] = 'Z:\Miniconda3\python.exe'
    # ����base����Python��������·��
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'Z:\Miniconda3\python.exe'
    # ������ǰ��root�û��������ִ�в���
    os.environ['HADOOP_USER_NAME'] = 'root'

    # ���ñ���ģʽ��spark
    conf = SparkConf().setMaster(sys.argv[1]).setAppName("Remote App Name")
    sc = SparkContext(conf=conf)

    # todo:2-���ݴ�����ȡ��ת��������
    # step1: ��ȡ����
    # ͨ�����������ȡ�ļ����ݷ���RDD��
    # ��ȡ�ļ��ĵ�ַ hdfs://hadoop200:8020/spark/wordcount/input
    input_rdd = sc.textFile(sys.argv[2])
    # ��ӡRDD������
    # print(input_rdd.first())    # ��ӡ��һ��Ԫ�ص�����
    # print(input_rdd.count())    # ��ӡRDD��Ԫ�صĸ���

    # Ϊ�˱�����β�ո��Ӱ�죬���в���ʱ��Ҫ����ȥ�ո�Ĳ���
    # step2: ��������
    # 1. ���˿���
    # filter_Rdd = input_rdd.filter(lambda line:len(line.strip()) > 0)

    # 2. �и�Ԫ��
    # word_Rdd = filter_Rdd.map(lambda line:line.strip().split(" "))


    # 3. ��ը����������ת����
    # explode_Rdd = filter_Rdd.flatMap(lambda line : line.strip().split(" "))


    # 4. ��ը��������תӳ���spark��ϲ���Ķ�Ԫ��
    # er_Rdd = explode_Rdd.map(lambda word:(word,1))
    # er_Rdd.foreach(lambda x: print(x))

    # 5. ���ն�Ԫ���key���з��飬�ٰ���value���оۺ�
    # result_Rdd = er_Rdd.reduceByKey(lambda item,tmp:item+tmp)
    # result_Rdd.foreach(lambda x: print(x))

    # todo ��ʽ��д����
    rs_rdd = (
               # ���˿���
               input_rdd.filter(lambda line:len(line.strip()) > 0)
               # .map(lambda line:line.strip().split(" "))
               # ��ը���и��ʹ��ת����,һ�ж������ת��Ϊһ��һ������
               # .flatMap(lambda line:line.strip().split(" ")) ����Ҫ���������ո񣬻������հ׷������,����ֱ��ʹ�������տհ׷����и�
               .flatMap(lambda line: re.split("\\s+", line))
               # ��ÿ��Ԫ��ӳ��ɶ�Ԫ��
               .map(lambda word:(word,1))
               # ����key���飬�ٸ���value�ۺ�
               .reduceByKey(lambda item,tmp:item+tmp)
    )
    # ��Щ�����ݵĴ�����ʵ��û�н��еģ�ֻ��������rs_rdd.foreach()ʱʹ�����rdd�Ż�ִ����Щ����


    # step3: ������
    # ��ӡ���
    rs_rdd.foreach(lambda x: print(x))
    print(rs_rdd.collect())
    # ���浽�ļ���
    # �ļ����·�� hdfs://hadoop200:8020/spark/wordcount/output7
    rs_rdd.saveAsTextFile(sys.argv[3])
    time.sleep(100000)
    # todo:3-�ر�
    sc.stop()

