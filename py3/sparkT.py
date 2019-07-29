from pyspark import *
import os
import sys


if __name__ == '__main__':
    # Windows��Spark��װĿ¼
    # Path for spark source folder
    """
    os.environ['SPARK_HOME'] = "F:\spark-2.3.3-bin-hadoop2.7"

    # Append pyspark to Python Path
    sys.path.append("F:\spark-2.3.3-bin-hadoop2.7\python")
    
    """
    # Create SparkConf
    conf = SparkConf()\
        .setAppName("WordCount")\
        .setMaster("local")\
        # Create SparkContext
    sc = SparkContext(conf=conf)
    # �ӱ���ģ������
    datas = ["you,jump", "i,jump"]
    rdd = sc.parallelize(datas)
    print(rdd.count())  # 2
    print(rdd.first())  # you,jum

    # WordCount
    wordcount = rdd.flatMap(lambda line: line.split(",")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    for wc in wordcount.collect():
        print(wc[0] + "   " + str(wc[1]))