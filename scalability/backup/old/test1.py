# spark-basic.py
from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf()
conf.setMaster('spark://192.168.2.119:7077')
conf.setAppName('spark-basic')
sc = SparkContext(conf=conf)

