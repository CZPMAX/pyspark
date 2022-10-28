#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	map_output_test.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/26
   Time        :    19:23
-------------------------------------------------
"""
list1 = [1, 2, 3, 4, 5]
print(list1)
print(*list1)
print(" ====================================================== ")

tuple1 = (1, 2, 3, 6, 7)
print(tuple1)
print(*tuple1)
print(" ====================================================== ")

dict1 = {1: 2, 3: 4}
print(dict1)
print(*dict1)
print(" ====================================================== ")

set1 = {1, 2, 3, 2}
print(set1)
print(*set1)
print(" ====================================================== ")

# todo 结论: python中的各种容器类型都可以直接print打印出来,打印出的结果是整个容器类型的数据
#    如果需要取出容器内部的值的话,则需要在print是在集合的前面写上一个*号,就能取出数据
#    注意: 字典类型的容器加*号取出来的是键的值
