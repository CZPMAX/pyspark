# coding=gbk
from pyspark import SparkContext, SparkConf
import os
import re

if __name__ == '__main__':
    # hadoopϵͳ
    # ����JDK��·��������ǰ���ѹ���Ǹ�·��
    os.environ['JAVA_HOME'] = '/export/servers/jdk1.8.0_65'
    # ����Hadoop��·��������ǰ���ѹ���Ǹ�·��
    os.environ['HADOOP_HOME'] = '/export/servers/hadoop-3.3.0'
    # ����base����Python��������·��
    os.environ['PYSPARK_PYTHON'] = '/export/servers/anaconda3/bin/python3'
    # ����base����Python��������·��
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/export/servers/anaconda3/bin/python3'
    # todo:1-����SparkContext
    conf = SparkConf().setMaster("spark://hadoop200:7077").setAppName("Remote Test APP")
    sc = SparkContext(conf=conf)

    # todo:2-���ݴ�����ȡ��ת��������
    # step1: ��ȡ����
    input_rdd = sc.textFile("hdfs://hadoop200:8020/spark/wordcount/input")
    # step2: ��������
    # step3: ������

    rs_rdd = (
               # ���˿���ǰ,����Ҫȥ��ͷβ�ո���Ϊ���㳤��ʱ�ո�Ҳ�ᱻ���ϳ���
               input_rdd.filter(lambda line: len(line.strip()) > 0)
               # һ�е��ʣ�ת���ɶ��е���,flatMap��ը���� ��map��flat
               .flatMap(lambda line: re.split("\\s+", line))
               # ������ÿ��һ�������Ժ�ʹ��mapӳ��ɶ�Ԫ��
               .map(lambda word: (word, 1))
               # ����key����, �ٸ���value�ۺϼ���
               .reduceByKey(lambda item, tmp: item+tmp)
    )
    # ������
    rs_rdd.saveAsTextFile("hdfs://hadoop200:8020/spark/wordcount/output3")
    # todo:3-�ر�SparkContext
    sc.stop()
