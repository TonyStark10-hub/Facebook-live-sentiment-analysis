from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
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

    counter=stream(ssc,posWords,negWords,100)
    plot_it(counter)


def get_wordlist(txtfile):
    words_file=open(txtfile,r).readlines()
    word_set=set()
    for word in word_file:
        #adding only words not \n in word_set
        word_set.add(word[:-1])
    return word_set

def stream(ssc,posWords,negWords,duration):
    kafkaStream=KafkaUtils.createDirectStream(
        ssc,topic=['facebookstream'],kafkaParams={'matadata.broker.list':'localhost:9092'})
    #each element of fb comment is text of comment
    comments=kafkaStream.map(lambda x: x[1])

    def word_catagory(word):
        if word in posWords:
            return ('positive',1)
        elif word in negWords:
            return ('negative',1)
        else:
            return ('nutral',1)
    def update_via_add(new,old):
        return sum(new,old or 0)

    #Spliting line by space to get words from each comment
    words=comments.flatmap(lambda line: line.split(' '))
    #catagorizing each word according to its nature positive/negative or nuteral
    words_to_catagories=words.map(word_catagory)
    #adding words by their catagory
    this_time_pairCount=words_to_catagories.reduceByKey(lambda x,y: x+y)
    #adding prvious time batch's counts to current ones
    upto_now_counts=this_time_pairCount.upadateStateByKey(update_via_add)

    upto_now_counts.pprint(2)

    counter=[]#counter array will contain word count for all time batches
    this_time_pairCount.foreachRDD(lambda t,rdd: counter.append(rdd.collect()))
    
    #start the computation
    ssc.start()
    ssc.awaitTerminationTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counter
def plot_it(counter):
    #plotting the counts for the positive and negative words for each time batch
    l_pos=[]
    l_neg=[]
    for list_ele in counter:
        f_pos,f_neg=0,0
        for el in list_ele:
            if 'positive' in el:
                l_pos.append(el[1])
                f_pos=1
            if 'negative' in el:
                l_neg.append(el[1])
                f_neg=1
        if f_pos==0:
            l_pos.append(0)
        if f_neg==0:
            l_neg.append(0)
    plt.plot(l_pos,label='positive',marker='.',color='blue')
    plt.plot(l_neg,label='negative',marker='.',color='green')
    plt.legend(loc='upper left')
    plt.xlabel('Time step')
    plt.ylabel('Word Count')
    plt.show()








if __name__ == '__main__':
    main()