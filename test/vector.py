
# -*- coding:utf-8 -*-

import math
from ._global import EPSILON

class Vector:

    def __init__(self, lst):
        """
        初始化
        :param lst:
        """
        self._values = lst

    @classmethod
    def zero(cls, dim):
        """
        返回一个dim维向量
        :param dim:
        :return:
        """
        return cls([0] * dim)

    def norm(self):
        """
        返回向量的模
        :return:
        """
        return math.sqrt(sum(e**2 for e in self))

    def normalize(self):
        """返回向量的单位向量
        """
        if self.norm() < EPSILON:
            raise ZeroDivisionError("Normalize error!norm is zero.")
        return Vector(self._values) / self.norm()

    def __repr__(self):
        return "Vector({})".format(self._values)

    def __str__(self):
        return "({})".format(", ".join(str(e) for e in self._values))

    def __len__(self):
        """
        返回向量长度，显示元素数量
        :return:
        """
        return len(self._values)

    def __getitem__(self, index):
        """
        返回向量长度，显示元素数量
        """
        return self._values[index]

    def __truediv__(self, k):
        """返回数量除法的结果向量: slef / k
        """
        return (1 / k) * self

    def __add__(self, other):
        """
         向量加法，返回结果向量
        :param self:
        :param other:
        :return:
        """
        assert len(self) == len(other), "Error in adding. Length of vectors must be same."
        return Vector([a + b for a, b in zip(self, other)])

    def __sub__(self, other):
        """
        向量减法运算
        :param self:
        :param other:
        :return:
        """
        assert len(self) == len(other), "Error in adding. Length of vectors must be same."
        return Vector([a - b for a, b in zip(self, other)])

    def dot(self, another):
        assert len(self) == len(another), "Error in adding. Length of vectors must be same."

    def __mul__(self, k):
        """
        向量乘法运算
        """
        return Vector([k * e for e in self])

    def __rmul__(self, k):
        return self * k

    def __pos__(self):
        """
        向量取正运算
        :return:
        """
        return 1 * self

    def __neg__(self):
        """
        向量取负运算
        :return:
        """
        return -1 * self

    def __iter__(self):
        """
        返回向量迭代器
        :return:
       """
        return self._values.__iter__()
    














