# encoding: UTF-8

import sys
import json
from pymongo import MongoClient
import pandas as pd
import numpy as np
import datetime as dt
from vnpy.trader.app.ctaStrategy.ctaBase import *
from CentralBase import CentralBaseSet

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

def dataToFile(data, filename='file'):
    #保存到CSV
    data.to_csv(filename + '.csv', index = True, header=True)
    
    #保存到JSON
    data_json = data
    data_json['datetime'] = data_json['datetime'].apply(lambda x:dt.datetime.strftime(x, '%Y-%m-%d  %H:%M:%S'))
    data_json.set_index(data_json['datetime'], inplace=True)
    data_json.to_json(filename + ".json", orient='columns')        

for symbol in SYMBOLS:
    for freq in FREQS:
        print u'开始分析合约%s 周期%s' %(symbol, freq)

        db = mc[DB_NAME_DICT[freq]]
        data =  pd.DataFrame(list(db[symbol].find()))
        data.set_index(data['datetime'], inplace=True)
        data.drop(['_id'], axis=1, inplace=True)
        data_analysis = CentralBaseSet(data)
        dataToFile(data_analysis.data, filename=symbol+'_'+freq)
        
        print u'完成分析合约%s 周期%s 长度%d' %(symbol, freq, np.size(data, axis=0))

       


