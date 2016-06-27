#!/usr/bin/env python
# vim: set fileencoding=utf-8

from distutils.core import setup

setup(name='pythonLearn',  # 打包后的包文件名
      version='1.0',
      description='build python project',
      author='huang jing',
      author_email='hj3245459@163.com',
      packages=[
          'service',
          'controller',
          'dao',
          'util',
          'resources'
      ]
      )
