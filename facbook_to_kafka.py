import json
import time
from kafka import SimpleProducer,KafkaClient
import configparser

class FacebookStreamProducer():
    ''' Class to read the facbook live stream and push it to Kafka topic '''

    def __init__(self):


if __name__ =='__main__':
    #to simulate facbook live stream ,we will load comments form a file in a streaming fashion
    
    f=open('16M.txt')
    stream=FacebookStreamProducer()
    i=0
    for data in f:
        stream.on_status(data.strip())
        i+=1
        if i%10000 == 0:
            print('pushed '+i+' elements')


