from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark,straming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf=SparkConf.setMaster('local[2]').setAppName('FBstream')
    sc=SparkContext(conf=conf)
    #streaming context with batch interval of 10 seconds
    ssc=StreamingContext(ss,10)
    ssc.checkpoint('checkpoint')

    sc.setLogLevel('WARN')

    posWords=get_wordlist('positive.txt')
    negWords=get_wordlist('negative.txt')

def get_wordlist(txtfile):
    


if __name__ == '__main__':
    main()