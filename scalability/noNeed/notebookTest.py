
# coding: utf-8

# In[7]:


from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import nltk
import json
#nltk.download('punkt')
#nltk.download('averaged_perceptron_tagger')

#confSpark = SparkConf().set("spark.driver.bindAddress", "localhost")
#sc = SparkContext("local[*]", "appname", conf = confSpark)


# In[8]:


# New API
spark_session = SparkSession        .builder        .master("spark://192.168.2.119:7077")         .appName("TestApp")        .config("spark.dynamicAllocation.enabled", True)        .config("spark.shuffle.service.enabled", True)        .getOrCreate()


# Old API (RDD)
spark_context = spark_session.sparkContext

spark_context.setLogLevel("INFO")


# In[3]:


