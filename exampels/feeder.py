from multiprocessing import Process, Queue
from threading import Thread

from fundcrunch import Feeder


feder_conf = { 'port': [9001, 8010, 7001],
               'addr': '0.0.0.0',
               'exchanges': [{'name': 'binance',
                              'pairs': ['BTC/USDT'],
                              'mode': ['order_book', 'trades', 'ohlc']}]
             }

subscribe = ['ohlc-binance-BTC_USDT',
             'ob-binance-BTC_USDT',
             'trade-binance-BTC_USDT',
             ]
             
feeder = Feeder(config=feder_conf, subscribe=subscribe)
feeder.start()


while True:
    rcv = feeder.output.get()
    print(rcv)


if feeder.is_alive():
    feeder.join()







