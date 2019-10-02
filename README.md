# Fundcrunch library

* connect directly to various exchanges and bind they stream into single local socket endpoint
* connector to Fundcrunch.Tech data streaming socket and REST endpoints

## Installation

```bash
$ pip install fundcrunch
```

## Install from source

```bash
$ git clone git@github.com:fundcrunch-tech/fundcrunch_py.git
$ cd fundcrunch_py
```


```bash
$ python3.7 -m venv .venv
$ source .venv/bin/activate
$ python setup.py install
```

```bash
$ cd examples
$ python feeder.py
```

## Usage

```python
from fundcrunch import Feeder


feeder_conf = { 'port': [9001, 8010, 7001],
               'addr': '0.0.0.0',
               'exchanges': [{'name': 'binance',
                              'pairs': ['BTC/USDT'],
                              'mode': ['order_book', 'trades', 'ohlc']}]
             }

subscribe = ['ohlc-binance-BTC_USDT',
             'ob-binance-BTC_USDT',
             'trade-binance-BTC_USDT',]
             
feeder = Feeder(config=feder_conf, subscribe=subscribe)
feeder.start()

while True:
  rcv = feeder.output.get()
  print(rcv)

```
