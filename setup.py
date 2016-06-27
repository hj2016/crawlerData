#!/usr/bin/env python
# vim: set fileencoding=utf-8

from distutils.core import setup

setup(name='crawlerData',  # 打包后的包文件名
      version='1.0',
      description='build python project',
      author='huang jing',
      author_email='hj3245459@163.com',
      packages=[
            'crawler',
            'crawler.util',
            'crawler.service',
            'crawler.dao',
            'crawler.controller'
      ],
      package_data = {
          'crawler': [
                'resources/conf/Log.conf',
                'resources/conf/Mysql.conf'
                      ],
      }
      )
