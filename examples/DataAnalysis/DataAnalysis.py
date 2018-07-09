# encoding: UTF-8

import sys
import json
from pymongo import MongoClient
import pandas as pd
import numpy as np
import datetime as dt
from vnpy.trader.app.ctaStrategy.ctaBase import *
from CentralBase import CentralBaseSet,Centralbase
try:
    import cPickle as pickle
except ImportError:
    import pickle

# 加载配置
config = open('config.json')
setting = json.load(config)

MONGO_HOST = setting['MONGO_HOST']
MONGO_PORT = setting['MONGO_PORT']
SYMBOLS = setting['SYMBOLS']
START = setting["START"]
END = setting["END"]
FREQS = setting["FREQ"]

mc = MongoClient(MONGO_HOST, MONGO_PORT)        # Mongo连接


DB_NAME_DICT = {
'1MIN': MINUTE_DB_NAME,
'5MIN':MINUTE_5_DB_NAME,
'15MIN':MINUTE_15_DB_NAME,
'30MIN':MINUTE_30_DB_NAME,
'60MIN':MINUTE_60_DB_NAME,
'D':DAILY_DB_NAME,
'W':WEEKLY_DB_NAME
}

RELY_FRED_DICT ={
'1MIN': '1MIN',
'5MIN':"1MIN",
'15MIN':"5MIN",
'30MIN':"5MIN",
'60MIN':"30MIN",
'D':"30MIN",
'W':"D"
}

PICKLE_DATA=['1MIN', '5MIN','30MIN']

def dataToFile(data, filename='file'):
    #保存到CSV
    data.to_csv(filename + '.csv', index = True, header=True)
    
    #保存到JSON
    data_json = data
    data_json['datetime'] = data_json['datetime'].apply(lambda x:dt.datetime.strftime(x, '%Y-%m-%d  %H:%M:%S'))
    data_json.set_index(data_json['datetime'], inplace=True)
    data_json.to_json(filename + ".json", orient='columns')        

def addCentralbaseLine(src, dst):
    lower_list = src.centralbase_list
    for base in lower_list:
        dst.data.ix[(dst.data.index>=base.start) & (dst.data.index<=base.end), 'l_base_up'] = base.up
        dst.data.ix[(dst.data.index>=base.start) & (dst.data.index<=base.end), 'l_base_down'] = base.down

    
if __name__ == '__main__':
    data_dict = {}
    for symbol in SYMBOLS:
        data_dict[symbol] = {}
        for freq in FREQS:
            print u'开始分析合约%s 周期%s' %(symbol, freq)
            file_name = symbol+'_'+freq+'.txt'
            try:
                pickle_file = open(file_name, 'rb')
            except:
                pickle_file = None
            if (freq in PICKLE_DATA) and (pickle_file !=None):
                data_analysis = pickle.load(pickle_file)
                data = data_analysis.data
            else:   
                db = mc[DB_NAME_DICT[freq]]
                data =  pd.DataFrame(list(db[symbol].find()))
                #对日线数据进行处理
                if freq == "D":
                    data['datetime'] = data['datetime'] + dt.timedelta(hours=23, minutes=59, seconds=59)            
                data.set_index(data['datetime'], inplace=True)
                data.drop(['_id'], axis=1, inplace=True)
                data.sort_index(axis=0, ascending=True, inplace=True)

                if RELY_FRED_DICT[freq] in data_dict[symbol]:
                    data_analysis = CentralBaseSet(freq, data, data_dict[symbol][RELY_FRED_DICT[freq]])
                else:
                    data_analysis = CentralBaseSet(freq,data)
                    
                data_analysis.analyze_CB()
            
            data_dict[symbol][freq] = data_analysis
            
            if (freq in PICKLE_DATA) and (pickle_file ==None):
                try:
                    pickle_file = open(file_name, 'wb')
                except:
                    pickle_file = None
                if pickle_file != None:
                    pickle.dump(data_analysis, pickle_file)
                     
            if RELY_FRED_DICT[freq] in data_dict[symbol]:
                addCentralbaseLine(data_dict[symbol][RELY_FRED_DICT[freq]], data_dict[symbol][freq])        
            
            print u'完成分析合约%s 周期%s 长度%d' %(symbol, freq, np.size(data, axis=0))
        
          
        for freq in data_dict[symbol]:
            dataToFile(data_dict[symbol][freq].data, filename=symbol+'_'+freq)
            print u'写入文件合约%s 周期%s 长度%d' %(symbol, freq, np.size(data_dict[symbol][freq].data, axis=0))
            
    

       


