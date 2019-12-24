import zmq
import time
import sys, json
from datetime import datetime
from  multiprocessing import Process

class ExchangeDriver():
    def __init__(self, exchange, conf):
        

        self.exchange = exchange
        self.conf = conf
        self.message_nounce = 0

        #print(self.exchange, 'FE initialized on ipc://%s'%conf['zmq']['fe'])

        
    def info(self):
        return {'exchange': self.exchange}
    
    def tnow(self):
        return datetime.now().strftime('%H:%M:%S.%f')
    
    def mogrify(self, topic, msg):
        """ json encode the message and prepend the topic """
        return topic + ' ' + json.dumps(msg)

    def demogrify(self, topicmsg):
        """ Inverse of mogrify() """
        json0 = topicmsg.find('{')
        topic = topicmsg[0:json0].strip()
        msg = json.loads(topicmsg[json0:-1].replace('\\"','"') )        
        return topic, msg
    

    def build_packet(self, message):

        message['nonce'] = self.message_nounce
        self.message_nounce += 1
        
        return json.dumps(message)
        