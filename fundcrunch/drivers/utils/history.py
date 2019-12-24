import os, glob
import ccxt, pickle, time
import datetime
import pandas as pd

class History():
    def __init__(self, dir_path="./history"):
        self.dir_path = dir_path
        self.context = {}

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        for filepath in glob.iglob(f"{self.dir_path}/*.pkl"):
            df = pd.read_pickle(filepath)

            filepath = filepath.replace(f"{self.dir_path}/",'')
            filepath = filepath.replace('.pkl','')
            if 'ohlcv' in filepath:    
                exchange,symbol,ohlc,ohlc_t = filepath.split('_')
                exchange = exchange.lower()
                symbol = symbol.replace('-','/')

                self.init_symbol_context(exchange,symbol)
                self.context[exchange][symbol]['ohlc'][ohlc_t] = df


    def init_exchange_context(self, exchange):
        exchange = exchange.lower()
        if exchange not in self.context.keys():
            self.context[exchange] = { }

    def init_symbol_context(self, exchange, symbol):
        exchange = exchange.lower()
        self.init_exchange_context(exchange)

        if symbol not in self.context[exchange]:
            self.context[exchange][symbol] = { 'ohlc': {} }
    
    def get_symbol(self,exchange,symbol):
        if exchange in self.context:
            if symbol in self.context[exchange]:
                return self.context[exchange][symbol]
            else:
                print('no symbol in ', exchange)
                return None
        else:
            print('no', exchange, 'in context')
    
    def exchanges(self):
        return list(self.context.keys())
        
    def exchange_symbols(self, exchange):
        if exchange in self.context:
           
            return list(self.context[exchange].keys())
        else:
            print('no', exchange, 'in context', self.context.keys())
            return []
    
    def download_ohlc_pandas(self, exchange, symbol, ohlcv, since=None):
        df = pd.DataFrame(columns=['timestamp','o','h','l','c','v'])
        try:
            if not since:
                ohlcv_downloaded = exchange.fetch_ohlcv(symbol, ohlcv)
            else:
                ohlcv_downloaded = exchange.fetch_ohlcv(symbol=symbol, timeframe=ohlcv, since=since)
        except Exception as e:
            print(e)
            
        else:
            df = pd.DataFrame(ohlcv_downloaded, columns=['timestamp','o','h','l','c','v'])
            df['timestamp'] = df['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000))
            df = df.set_index('timestamp')

        return df
    
    def append_ohlc(self, exchange, symbol, ohlc):
        print(ohlc)
        i = ohlc['i']

        if exchange not in ccxt.exchanges:
            print(exchange, 'not in driver')
            return 
        
        file_name = f"{self.dir_path}/{exchange}_{symbol}_ohlcv_{i}.pkl"
        


    def update_ohlc(self, exchange, ohlcv, symbol_filter=[]):
        if exchange not in ccxt.exchanges:
            print(exchange, 'not in driver')
            return        

        ccxt_driver = eval(f"ccxt.{exchange}")
        exchange = ccxt_driver()

        self.init_exchange_context(exchange.id)

        if not exchange.has['fetchOHLCV']:
            print(exchange.id, 'has no method fetchOHLCV()')
            return

        markets = exchange.load_markets()
        symbols = [ s[0] for s in markets.items()]

        for s in symbols:
            
            if any( substr in s for substr in symbol_filter) and symbol_filter:

                file_name = f"{self.dir_path}/{exchange}_{s.replace('/','-')}_ohlcv_{ohlcv}.pkl"
                
                #-----------------------------
                # download full ohlc history
                #-----------------------------
                if not os.path.isfile(f"{file_name}"):  
                    print('downloading', exchange, s, ohlcv)
                    df = self.download_ohlc_pandas(exchange,s,ohlcv)
                    df.to_pickle(file_name)

                    time.sleep(exchange.rateLimit / 100)

                else:
                    
                    df = pd.read_pickle(file_name)

                    if df.shape[0] > 0:                   
                        last_date = df.index[-1]   
                        print(exchange, s, 'updating from', last_date )              
                        df_extend = self.download_ohlc_pandas(exchange, s, ohlcv, since=int(last_date.timestamp()*1000))
                    else:
                        print(exchange, s, 'updating' )
                        df_extend = self.download_ohlc_pandas(exchange, s, ohlcv)

                    
                    df = df.append(df_extend)
                    df.to_pickle(file_name)
                    time.sleep(exchange.rateLimit / 100)

                    #print(df.tail())
                
                self.init_symbol_context(exchange.id, s)
                self.context[exchange.id][s]['ohlc'][ohlcv] = df
            





                



       