# encoding: UTF-8

import sys
import json
import time as tm
from pymongo import MongoClient
import pandas as pd
import numpy as np
import datetime as dt
from vnpy.trader.app.ctaStrategy.ctaBase import *
from CentralBase import CentralBaseSet,Centralbase
from TradeAnalysis import *
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


NODE_DB_NAME_DICT = {
'1MIN': '',
'5MIN':CHT_NODE_5_DB_NAME,
'15MIN':'',
'30MIN':CHT_NODE_30_DB_NAME,
'60MIN':'',
'D':CHT_NODE_D_DB_NAME,
'W':''
}

CB_DB_NAME_DICT = {
'1MIN': '',
'5MIN':CHT_CB_5_DB_NAME,
'15MIN':'',
'30MIN':CHT_CB_30_DB_NAME,
'60MIN':'',
'D':CHT_CB_D_DB_NAME,
'W':''
}

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
    
def resultToSource(data, node_list, centralbase_list, beichi_list, share_beichi_list, panzh_beichi_list, share_panzh_beichi_list):
    data['node'] = None
    data['base_up'] = None
    data['base_down'] = None
    data['beichi'] = None
    data['sharebeichi'] = None
    data['panzhbeichi'] = None
    data['sharepanzhbeichi'] = None
    data['sec_buy'] = None
    
    if node_list!=None:
        for node in node_list:
            time_seg = data.ix[data.index>=node['datetime'], 'close']
            if not time_seg.empty:
                time = time_seg.index[0]
                data.set_value(time, 'node', node['value'])
                
    if centralbase_list!=None:            
        for base in centralbase_list:
            data.ix[base['start']:base['end'],'base_up'] = base['up']
            data.ix[base['start']:base['end'],'base_down'] = base['down']
            data.ix[base['start']:base['end'],'base_type'] = base['ctype']
    
    if beichi_list!=None:
        for beichi in beichi_list:
            time_seg = data.ix[data.index>=beichi['datetime'], 'close']
            if not time_seg.empty:
                time = time_seg.index[0]
                data.set_value(time, 'beichi', beichi['value'])
    
    if share_beichi_list!=None:        
        for sharebeichi in share_beichi_list:
            time_seg = data.ix[data.index>=sharebeichi['datetime'], 'close']
            if not time_seg.empty:
                time = time_seg.index[0]
                data.set_value(time, 'sharebeichi',  sharebeichi['value']) 
                
    if panzh_beichi_list:       
        for panzhbeichi in panzh_beichi_list:
            time_seg = data.ix[data.index>=panzhbeichi['datetime'], 'close']
            if not time_seg.empty:
                time = time_seg.index[0]
                data.set_value(time, 'panzhbeichi', panzhbeichi['value']) 
                
    if share_panzh_beichi_list:       
        for sharepanzhbeichi in share_panzh_beichi_list:
            time_seg = data.ix[data.index>=sharepanzhbeichi['datetime'], 'close']
            if not time_seg.empty:
                time = time_seg.index[0]
                data.set_value(time, 'sharepanzhbeichi', sharepanzhbeichi['value'])    
    '''         
    for sec_buy in self.sec_buy_point_list:
        time_seg = self.data.ix[self.data.index>=sec_buy.time, 'close']
        time = time_seg.index[0]
        if time!=None:
            self.data.set_value(time, 'sec_buy', self.data.ix[time, 'close'])  
     '''       
    
def loadAnalysisData():    
    for symbol in SYMBOLS:
        for freq in FREQS:
            print u'开始加载合约%s 周期%s' %(symbol, freq)
            file_name = symbol+'_'+freq
            
            
            db = mc[DB_NAME_DICT[freq]]
            data =  pd.DataFrame(list(db[symbol].find()))
            
            db = mc[NODE_DB_NAME_DICT[freq]]
            flt = {'btype':{ '$eq': 0}}
            node_list = list(db[symbol].find(flt))
            
            db = mc[CB_DB_NAME_DICT[freq]]
            
            centralbase_list = list(db[symbol].find())
            
            db = mc[NODE_DB_NAME_DICT[freq]]
            flt = {'btype':{ '$ne': 0}, 'classfier':{ '$eq': 1}}
            beichi_list = list(db[symbol].find(flt))
            
            db = mc[NODE_DB_NAME_DICT[freq]]
            flt = {'btype':{ '$ne': 0}, 'classfier':{ '$eq': 2}}
            share_beichi_list = list(db[symbol].find(flt))    
            
            db = mc[NODE_DB_NAME_DICT[freq]]
            flt = {'btype':{ '$ne': 0}, 'classfier':{ '$eq': 3}}
            panzh_beichi_list = list(db[symbol].find(flt))  
            
            db = mc[NODE_DB_NAME_DICT[freq]]
            flt = {'btype':{ '$ne': 0}, 'classfier':{ '$eq': 4}}
            share_panzh_beichi_list = list(db[symbol].find(flt))             
            
            #对日线数据进行处理
            if freq == "D":
                data['datetime'] = data['datetime'] + dt.timedelta(hours=23, minutes=59, seconds=59)            
            data.set_index(data['datetime'], inplace=True)
            data.drop(['_id'], axis=1, inplace=True)
            data.sort_index(axis=0, ascending=True, inplace=True)
           
            print u'融合分析数据...'
            resultToSource(data, node_list, centralbase_list, beichi_list, share_beichi_list, panzh_beichi_list, share_panzh_beichi_list)
            
            print u'写入文件...'
            dataToFile(data, file_name)
            
if __name__ == '__main__':
    loadAnalysisData()