import json
import time
from kafka import SimpleProducer,KafkaClient
import configparser

class FacebookStreamProducer():
    ''' Class to read the facbook live stream and push it to Kafka topic '''

    def __init__(self):
        client= KafkaClient('localhost:9092')
        self.producer=SimpleProducer(client,async = True,
                                      batch_send_every_n=1000,
                                      batch_send_every_t=10)
    
    def on_message(self,status):
        # This method will take messages whenever new messages arrives from live comments stream.
        # here I have aynchronously pushed the messeges to topic called 'facebookstream'
        message=status
        try:
            self.producer.send_messages('facebookstream',message)
        except Exception as ex:
            print(ex)
            return False
        return True

    def on_error(self,status_code):
        print('error recived from kafka producer')
        return True #Dont kill the stream
    
    def on_timeout(self):
        return True #Dont kill the stream

if __name__ =='__main__':
    #to simulate facbook live stream ,we will load comments form a file in a streaming fashion
    
    f=open('16M.txt')
    stream=FacebookStreamProducer()
    i=0
    for data in f:
        stream.on_message(data.strip())
        i+=1
        if i%10000 == 0:
            print('pushed '+i+' elements')


