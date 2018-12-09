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

PICKLE_DATA=['1MIN', '5MIN']
#PICKLE_DATA=[]
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

def analyze_existing_data():
    data_dict = {}
    for symbol in SYMBOLS:
        data_dict = {}
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

                if RELY_FRED_DICT[freq] in data_dict:
                    data_analysis = CentralBaseSet(freq=freq, dataframe=data, low_CB_set=data_dict[RELY_FRED_DICT[freq]])
                else:
                    data_analysis = CentralBaseSet(freq=freq,dataframe=data)
                    
                data_analysis.analyze_CB()
            
            data_dict[freq] = data_analysis
            
            if (freq in PICKLE_DATA) and (pickle_file ==None):
                try:
                    pickle_file = open(file_name, 'wb')
                except:
                    pickle_file = None
                if pickle_file != None:
                    pickle.dump(data_analysis, pickle_file)
                     
            if RELY_FRED_DICT[freq] in data_dict:
                addCentralbaseLine(data_dict[RELY_FRED_DICT[freq]], data_dict[freq])        
            
            print u'完成分析合约%s 周期%s 长度%d' %(symbol, freq, np.size(data, axis=0))
        
          
        for freq in data_dict:
            dataToFile(data_dict[freq].data, filename=symbol+'_'+freq)
            print u'写入文件合约%s 周期%s 长度%d' %(symbol, freq, np.size(data_dict[freq].data, axis=0))
            
def analyze_realtime_data():
    data_dict = {}
    data_analysis_dict = {}
    cur_time_index = {}
    for symbol in SYMBOLS:
        data_analysis_dict = {}
        data_dict = {}
        cur_time_index[symbol] = {}
        for freq in FREQS:
            print u'开始分析合约%s 周期%s' %(symbol, freq)
            file_name = symbol+'_'+freq+'.txt'
            
            db = mc[DB_NAME_DICT[freq]]
            data =  pd.DataFrame(list(db[symbol].find()))
            #对日线数据进行处理
            if freq == "D":
                data['datetime'] = data['datetime'] + dt.timedelta(hours=23, minutes=59, seconds=59)            
            data.set_index(data['datetime'], inplace=True)
            data.drop(['_id'], axis=1, inplace=True)
            data.sort_index(axis=0, ascending=True, inplace=True)
            columns = data.columns
            
            
            data_dict[freq] = data
            start_time = dt.datetime.strptime(START, '%Y-%m-%d')

            data_seg = data.ix[data.index<start_time]
            #if data_seg == None:
             #   data_seg = pd.DataFrame(columns=columns)              
            if RELY_FRED_DICT[freq] in data_analysis_dict:
                data_analysis = CentralBaseSet(freq=freq, dataframe=data_seg, low_CB_set=data_analysis_dict[RELY_FRED_DICT[freq]],all_data=data)
            else:
                data_analysis = CentralBaseSet(freq=freq,dataframe=data_seg, all_data=data)           
                            
            data_analysis_dict[freq] = data_analysis
            
            data_seg = data.ix[data.index>=start_time]
            cur_time_index[symbol][freq] = data_seg.index[0]

        lowest_freq = FREQS[0]
        lowest_data = data_dict[lowest_freq]
        
        count = 0
        length = np.size(lowest_data, axis=0)
        print u'--START REALTIME ANALYSIS [%d]' %(length)
        timespend5 = 0
        for time in lowest_data.index:
            if(count % 100 == 0):
                print u'--STEP:%d/%d' %(count,length)
            count +=1            
            if time>cur_time_index[symbol][lowest_freq]:

                #data_analysis_dict[lowest_freq].update_data(low_data_frame=None, data_item_series=data_dict[lowest_freq].ix[time])
                data_analysis_dict[lowest_freq].update_data_time(low_data_frame=None, data_item_time=time)
                if time > cur_time_index[symbol][FREQS[1]]:
                    data = data_dict[FREQS[1]]
                    data_seg = data.ix[data.index < time] 
                    if data_seg.empty:
                        break
                    data_analysis_dict[FREQS[1]].update_data_time(low_data_frame=None, data_item_time=data_seg.index[-1])
                    
                    data_seg = data.ix[data.index >= time]
                    if data_seg.empty:
                        break                    
                    cur_time_index[symbol][FREQS[1]] = data_seg.index[0]
       
                if time > cur_time_index[symbol][FREQS[2]]:
                    data = data_dict[FREQS[2]]
                    data_seg = data.ix[data.index < time] 
                    if data_seg.empty:
                        break                    
                    data_analysis_dict[FREQS[2]].update_data_time(low_data_frame=None, data_item_time=data_seg.index[-1]) 
                    
                    data_seg = data.ix[data.index >= time]    
                    if data_seg.empty:
                        break                    
                    cur_time_index[symbol][FREQS[2]] = data_seg.index[0]
                    
                data_analysis_dict['5MIN'].analyze_CB_Step()
                data_analysis_dict['30MIN'].analyze_CB_Step()
                data_analysis_dict['D'].analyze_CB_Step()
                
                #data_analysis_dict['30MIN'].trade_strategy_step(data_analysis_dict['D'])
                    
        #print u'分析买卖点1'
        ##trade_analysis_2buy_allsell(data_analysis_dict['30MIN'].sec_buy_point_list, \
                                  ##data_analysis_dict['30MIN'].beichi_list, \
                                  ##data_analysis_dict['30MIN'].sec_sell_point_list, \
                                  ##data_dict['5MIN'])
        #trade_analysis_buy_oneseg_sell(data_analysis_dict['D'].first_buy_point_list,\
                                        #data_analysis_dict['D'].all_sell_point_list,\
                                        #data_dict['5MIN'])
        #print u'分析买卖点2'
        #trade_analysis_buy_oneseg_sell(data_analysis_dict['D'].sec_buy_point_list,\
                                        #data_analysis_dict['D'].all_sell_point_list,\
                                        #data_dict['5MIN'])
        #print u'分析买卖点3'
        #trade_analysis_buy_oneseg_sell(data_analysis_dict['D'].third_buy_point_list,\
                                        #data_analysis_dict['D'].all_sell_point_list,\
                                        #data_dict['5MIN'])
        print u'分析买卖点'
        trade_strategy_analysis(data_analysis_dict['D'].trade_strategy_list, \
                                  data_dict['5MIN'], symbol+'_analysis.txt')
        
        for freq in FREQS:    
            data_analysis_dict[freq].resultToSource()
            
            if RELY_FRED_DICT[freq] in data_analysis_dict:
                addCentralbaseLine(data_analysis_dict[RELY_FRED_DICT[freq]], data_analysis_dict[freq])        
            
            print u'完成分析合约%s 周期%s 长度%d' %(symbol, freq, np.size(data, axis=0))

            dataToFile(data_analysis_dict[freq].data, filename=symbol+'_'+freq)
            print u'写入文件合约%s 周期%s 长度%d' %(symbol, freq, np.size(data_analysis_dict[freq].data, axis=0))   

        #sec_buy_list = data_analysis_dict['30MIN'].sec_buy_point_list
        #beichi_list = data_analysis_dict['30MIN'].beichi_list
        #lowest_data = data_dict['5MIN']
        #lowest_data = lowest_data.set_index(lowest_data['datetime'])
        #for sec_buy in sec_buy_list:
            #time_seg = lowest_data.ix[lowest_data.index>=sec_buy.real_time]
            #buy_value = time_seg.ix[0, 'close']         
            #print("BUY: ", sec_buy.real_time, buy_value)
            #for beichi in beichi_list:
                #if beichi.btype>0 and beichi.real_time>sec_buy.real_time:
                    #time_seg = lowest_data.ix[lowest_data.index>=beichi.real_time, 'close']
                    #sell_value = time_seg.ix[0, 'close']                 
                    #print("SELL: ", beichi.real_time, sell_value, 100*(sell_value-buy_value)/buy_value)
                    #break
 
    
if __name__ == '__main__':
    
    analyze_realtime_data()
            
    

       


