#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Description :	TODO: pyspark代码
   SourceFile  :	range_test.py
   Software    :    PyCharm
   Author      :	CZPMAX
   Date	       :	2022/10/25
   Time        :    20:47
-------------------------------------------------
"""
# 错误示范，将range(1, 11) 赋值给num, 想输出所有的数值但是输出了他自己本身
num = range(1, 11)
print(num)
# todo 想要输出它的结果那
# 列表推导式 生成列表
list1 = [num for num in range(1, 11)]
print(list1)
