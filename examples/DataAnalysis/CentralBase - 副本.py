# encoding: UTF-8

import sys
import json
from pymongo import MongoClient
from vnpy.trader.app.ctaStrategy.ctaBase import DATABASE_NAMES
import pandas as pd
import numpy as np
import datetime as dt
import talib as ta
from interval import Interval
import time

#方向        
M_TO_UP = True
M_TO_DOWN = False

#节点或中枢是否正式形成
M_FORMAL = True
M_TEMP = False

#顶点或底点
M_TOP = 1
M_BOTTOM = -1

#常量
M_MAX_VAL = 5000
M_MIN_VAL = -5000
M_INVALID_INDEX = -1

#背驰点或买卖点的判定
M_NODECIDE = 0
M_FALSE = -1
M_TRUE = 1


class Node:
    """ 
    趋势节点，包含（时间， 值）
    low_id:次级中枢编号
    low_count:当前点累计的次级中枢数目
    ntype: 顶点或低点
    isformal：正式或临时节点
    """
    def __init__(self, time, value, ntype, low_id=None, low_count=None, isformal=M_TEMP):
        self.datetime = time
        self.value = value
        self.ntype= ntype
        self.low_id = low_id
        self.low_count = low_count
        self.isformal = isformal
    
    
        
class Centralbase:
    """ 
    中枢定义
    start：开始时间
    end：结束时间
    up：上边界
    down: 下边界
    start_node_id: 开始ID
    end_node_id: 结束ID
    ctype:类型计数
    isformal：正式或临时节点
    """
    def __init__(self, start, end, up, down, start_node_id=None, end_node_id=None, isformal=M_TEMP):
        self.start = start
        self.end = end 
        self.up = up
        self.down = down
        self.start_node_id = start_node_id
        self.end_node_id = end_node_id
        self.ctype = 0
        self.isformal = isformal
        self.max_val = M_MIN_VAL
        self.min_val = M_MAX_VAL
        self.max_node_id = M_INVALID_INDEX
        self.min_node_id = M_INVALID_INDEX
        
        
    def setCType(self, ctype):
        self.ctype = ctype
        
    def getCBInterval(self):
        return Interval(lower_bound=self.down, upper_bound=self.up, 
                        lower_closed=False, upper_cloesed=False)        
        
class BeichiTime:
    '''
    time:背驰时间
    btype:背驰类型和中枢层级，正数为顶背驰 负数为底背驰
    real_beichi:是否为真背驰, M_NODECIDE为未判定，M_FALSE否 M_TRUE是
    '''
    def __init__(self, time, btype, node_id, real_time=None):
        self.time = time
        self.btype = btype
        self.node_id = node_id
        self.real_beichi = M_NODECIDE
        self.real_time = real_time

class BuyPoint:
    def __init__(self, time, node_id,real_time = None):
        self.time = time
        self.real_time = real_time
        self.node_id = node_id
        self.real_buy = M_NODECIDE
        
class SellPoint:
    def __init__(self, time,  node_id,real_time = None):
        self.time = time
        self.real_time = real_time
        self.node_id = node_id
        self.real_sell = M_NODECIDE
 
class KData:
    def __init__(self):
        self.data_dict={}
        self.rely_dict={}
    def addDataItem(self, name='D', item=None):
        if item==None:
            self.data_dict[name] = pd.DataFrame()
        else:
            self.data_dict[name] = self.data_dict[name].concat(item,ignore_index=False)
    def buildRelation(up_name='D', low_name='30MIN'):
        self.rely_dict[up_name] = low_name



        
class CentralBaseSet:
        
    def __init__(self, freq, dataframe, low_CB_set=None, all_data=None):
        self.data = dataframe.set_index(dataframe['datetime'])
        self.low_CB_set = low_CB_set
        self.node_list = []
        self.centralbase_list = []
        self.freq = freq
        self.beichi_list=[]
        self.first_sell_point_list = []
        self.sec_sell_point_list = []
        self.all_sell_point_list = []
        self.first_buy_point_list = []
        self.sec_buy_point_list = []
        self.third_buy_point_list = []
        self.beichi_processing = False
        self.upgrade_cnt=0
        self.seek_max=None
        self.temp_open = None
        self.temp_close = None
        self.data_columns = self.data.columns
        self.all_data = all_data
        self.data = all_data
        self.cur_time_index = None
        self.cur_min_value = M_MAX_VAL
        self.cur_min_node_id = 0
        self.cur_max_value = M_MIN_VAL
        self.cur_max_node_id = 0
        
        #中枢升级逻辑
        self.cur_cut_low_id = -1
        self.cur_cut_start_node_id = -1
        
        self.cur_low_beichi_time = None
        
        self.timespend1=0
        self.timespend2=0
        self.timespend3=0
        self.timespend4=0
        
        self.callcnt = 0

            
    def analyze_CB_Step(self):

        
        if self.low_CB_set == None:
            self.getNodeList_KLine_Step()
        else:
            self.getNodeList_Lower_Step()   

        self.get_Centralbase_Step()
        self.beichi_processing = self.getBeichi_LastTwo_Step()
        self.beichi_judge_step()
        self.sell_point_judge()

    
    def update_data(self, low_data_frame=None, data_item_series=None):
        if not data_item_series.empty:

            data = {}
            for column in self.data_columns:
                data[column] = []
                data[column].append(data_item_series[column])
            data_item = pd.DataFrame(data, columns=self.data_columns)
            self.data = pd.concat([self.data, data_item], ignore_index=True)

            self.data.set_index(self.data['datetime'], inplace=True)
            self.indexGenerator_step()

        '''
                else:
            if low_data_frame!=None:
                if self.low_CB_set==None:
                    self.data = low_data_frame
                else:
                    data_seg = low_data_frame.ix[low_data_frame.index>self.data.index[-1]]
                    if data_seg!=None:
                        open_p = data_seg.ix[0, 'open']
                        close_p = data_seg.ix[-1, 'close']
                        volumns = data_seg['volume'].sum()
        '''

    def update_data_time(self, low_data_frame=None, data_item_time=None):
        if data_item_time !=None:
            start = time.clock()
            self.cur_time_index = data_item_time
            self.indexGenerator_step()
            self.timespend4 = self.timespend4 + (time.clock() - start)
        '''
                else:
            if low_data_frame!=None:
                if self.low_CB_set==None:
                    self.data = low_data_frame
                else:
                    data_seg = low_data_frame.ix[low_data_frame.index>self.data.index[-1]]
                    if data_seg!=None:
                        open_p = data_seg.ix[0, 'open']
                        close_p = data_seg.ix[-1, 'close']
                        volumns = data_seg['volume'].sum()
        '''

                        
    
                
        
        
    def __getmax(self, a, b):
        return a if a>b else b
    
    def __getmin(self, a,b):
        return a if a<b else b
    
    def resultToSource(self):
        self.data['node'] = None
        self.data['base_up'] = None
        self.data['base_down'] = None
        self.data['beichi'] = None
        self.data['sec_buy'] = None
        for node in self.node_list:
            time_seg = self.data.ix[self.data.index>=node.datetime, 'close']
            time = time_seg.index[0]
            if time!=None:
                self.data.set_value(time, 'node', node.value)
        for base in self.centralbase_list:
            self.data.ix[base.start:base.end,'base_up'] = base.up
            self.data.ix[base.start:base.end,'base_down'] = base.down
            self.data.ix[base.start:base.end,'base_type'] = base.ctype
        
        for beichi in self.beichi_list:
            time_seg = self.data.ix[self.data.index>=beichi.time, 'close']
            time = time_seg.index[0]
            if time!=None:
                self.data.set_value(time, 'beichi', self.data.ix[time, 'close'])
                
        for sec_buy in self.sec_buy_point_list:
            time_seg = self.data.ix[self.data.index>=sec_buy.time, 'close']
            time = time_seg.index[0]
            if time!=None:
                self.data.set_value(time, 'sec_buy', self.data.ix[time, 'close'])        
                
            
    def indexGenerator_step(self):
        #length = np.size(self.data, axis=0)
        #if length<40:
            #self.data['SMA5'] = ta.SMA(self.data['close'].values, timeperiod = 5)  #5日均线
            #self.data['SMA10'] = ta.SMA(self.data['close'].values, timeperiod = 10)  #10日均线
        macd_talib, signal, hist = ta.MACD(self.data['close'].values,fastperiod=12,signalperiod=9)
        self.data['DIF'] = macd_talib #DIF
        self.data['DEA'] = signal #DEA
        self.data['MACD'] = hist #MACD 
        #else:
            ##self.data.ix[-40:,'SMA5'] = ta.SMA(self.data.ix[-40:,'close'].values, timeperiod = 5)  #5日均线
            ##self.data.ix[-40:,'SMA10'] = ta.SMA(self.data.ix[-40:,'close'].values, timeperiod = 10)  #10日均线
            #macd_talib, signal, hist = ta.MACD(self.data.ix[-40:,'close'].values,fastperiod=12,signalperiod=9)
            #self.data.ix[-1,'DIF'] = macd_talib[-1] #DIF
            #self.data.ix[-1:,'DEA'] = signal[-1] #DEA
            #self.data.ix[-1:,'MACD'] = hist[-1] #MACD     
        
            

    def getNodeList_KLine_Step(self):
        #if self.data.empty:
            #return
        #time = self.data.index[-1]
        if self.cur_time_index == None:
            return
        time = self.cur_time_index        
        
        open_price =  self.data.ix[time, 'open']
        close_price = self.data.ix[time, 'close']
        up_flag = open_price <= close_price
        
        if self.seek_max==None: #初始数据
            if up_flag:
                self.seek_max = M_TO_UP
                self.node_list.append(Node(time, close_price, M_TOP , isformal=M_TEMP))
            else:
                self.seek_max = M_TO_DOWN
                self.node_list.append(Node(time, close_price, M_BOTTOM, isformal=M_TEMP))
            
        go_judge = True
        if self.seek_max == M_TO_UP:
            if abs(close_price - open_price) <=0.001: #排查十字星的情况
                if close_price >= self.node_list[-1].value:
                    self.node_list[-1].datetime = time
                    self.node_list[-1].value = close_price
                else:
                    go_judge = False                    
            if up_flag and go_judge:
                if close_price >= self.node_list[-1].value:
                    self.node_list[-1].datetime = time
                    self.node_list[-1].value = close_price
            else:
                if close_price < self.node_list[-1].value:
                    self.node_list[-1].isformal = M_FORMAL
                    self.node_list.append(Node(time, close_price, M_BOTTOM, isformal=M_TEMP))
                    self.seek_max = M_TO_DOWN
        else:
            if abs(close_price - open_price) <=0.001: #排查十字星的情况
                if close_price <= self.node_list[-1].value:
                    self.node_list[-1].datetime = time
                    self.node_list[-1].value = close_price
                else:
                    go_judge = False
            if (not up_flag) and go_judge:
                if close_price <= self.node_list[-1].value:
                    self.node_list[-1].datetime = time
                    self.node_list[-1].value = close_price
            else:
                if close_price > self.node_list[-1].value:
                    self.node_list[-1].isformal = M_FORMAL
                    self.node_list.append(Node(time, close_price, M_TOP, isformal=M_TEMP))
                    self.seek_max = M_TO_UP

        
        
            
    def __getNodeListCross(self, start_id, end_id_include):
        cross_itval = Interval.none()
        i=start_id
        while(i<end_id_include):
            if cross_itval == Interval.none():
                cross_itval = self.__getSegment(i)
            else:
                cross_itval = cross_itval & self.__getSegment(i)
            i+=1
        return cross_itval
        

    def get_Centralbase_Step(self):
        '''
        有效逻辑时机：
        1.首个中枢；
        2.背驰处理；
        3.形成新的临时节点和正式节点
        '''
        seg_list=[]
        start = None
        end = None
        start_id = -1
        end_id = -1
        cross_itval = Interval.none
        
        if self.freq=='5MIN' and len(self.node_list)>2 \
           and abs(self.node_list[-2].value-5.29)<0.001\
           and abs(self.node_list[-1].value-5.28)<0.001:
            a=1
            
        if self.freq=='5MIN' :
            a=1       
            
        if self.freq=='30MIN' :
            a=1
            
        if self.freq=='D' :
            a=1        
             
        
        if len(self.centralbase_list) ==0:#首个中枢
            if len(self.node_list) > 3:
                cross_itval = self.__getSegment(0) & self.__getSegment(1)
                start = self.__getSegmentStart(0)
                end = self.__getSegmentEnd(1)
                newcbase = Centralbase(start, end, cross_itval.upper_bound, cross_itval.lower_bound, 0, 2, isformal=M_TEMP)
                newcbase.setCType(self.__getCBType(newcbase))
                newcbase.max_node_id, newcbase.max_val = self.__getMaxNode_Val(0, 2)
                newcbase.min_node_id, newcbase.min_val = self.__getMinNode_Val(0, 2)
                self.centralbase_list.append(newcbase)
        else:
            end_node_id = self.centralbase_list[-1].end_node_id
            start_node_id = self.centralbase_list[-1].start_node_id
            if len(self.node_list)-2 > end_node_id: #新临时NODE已经形成，新正式NODE形成
                cross_itval = self.centralbase_list[-1].getCBInterval() & self.__getSegment(end_node_id)
                if cross_itval != Interval.none():#新正式段与原中枢相交，更新中枢信息   
                    #if end_node_id-start_node_id >=4 :
                        ##切割中枢
                        #self.centralbase_list[-1].isformal = M_FORMAL
                        #cross_itval = self.__getSegment(start_node_id) & self.__getSegment(start_node_id+2)
                        #self.centralbase_list[-1].up = cross_itval.upper_bound
                        #self.centralbase_list[-1].down = cross_itval.lower_bound
                        #self.centralbase_list[-1].end_node_id = start_node_id+3
                        #self.centralbase_list[-1].end = self.node_list[start_node_id+3].datetime
                        #self.centralbase_list[-1].max_node_id, self.centralbase_list[-1].max_val = self.__getMaxNode_Val(start_node_id, start_node_id+3)
                        #self.centralbase_list[-1].min_node_id, self.centralbase_list[-1].min_val = self.__getMinNode_Val(start_node_id, start_node_id+3)                        
                        
                        ##添加新中枢
                        #cross_itval = self.centralbase_list[-1].getCBInterval() & self.__getSegment(start_node_id+3) & self.__getSegment(start_node_id+4)
                        #start = self.node_list[start_node_id+3].datetime
                        #end = self.node_list[end_node_id+1].datetime
                        #newcbase = Centralbase(start, end, cross_itval.upper_bound, cross_itval.lower_bound, start_node_id+3, end_node_id+1, isformal=M_TEMP)
                        #newcbase.setCType(self.__getCBType(newcbase))
                        #newcbase.max_node_id, newcbase.max_val = self.__getMaxNode_Val(start_node_id+3, end_node_id+1)
                        #newcbase.min_node_id, newcbase.min_val = self.__getMinNode_Val(start_node_id+3, end_node_id+1)                    
                        #self.centralbase_list.append(newcbase)                        
                    #else:
                    self.centralbase_list[-1].up = cross_itval.upper_bound
                    self.centralbase_list[-1].down = cross_itval.lower_bound
                    self.centralbase_list[-1].end_node_id = end_node_id+1
                    self.centralbase_list[-1].end = self.node_list[end_node_id+1].datetime
                    #self.centralbase_list[-1].setCType(self.__getCBType(newcbase=None, isnew=False, cb_id=len(self.centralbase_list)-1))
                    #更新极值信息
                    if self.node_list[end_node_id+1].value > self.centralbase_list[-1].max_val:
                        self.centralbase_list[-1].max_val = self.node_list[end_node_id+1].value
                        self.centralbase_list[-1].max_node_id = end_node_id+1
                    if self.node_list[end_node_id+1].value < self.centralbase_list[-1].min_val:
                        self.centralbase_list[-1].min_val = self.node_list[end_node_id+1].value
                        self.centralbase_list[-1].min_node_id = end_node_id+1
                        
                    
                    
                else:
                    self.centralbase_list[-1].isformal = M_FORMAL
                    #添加新中枢
                    cross_itval = self.__getSegment(end_node_id)
                    start = self.node_list[end_node_id].datetime
                    end = self.node_list[end_node_id+1].datetime
                    newcbase = Centralbase(start, end, cross_itval.upper_bound, cross_itval.lower_bound, end_node_id, end_node_id+1, isformal=M_TEMP)
                    newcbase.setCType(self.__getCBType(newcbase))
                    newcbase.max_node_id, newcbase.max_val = self.__getMaxNode_Val(end_node_id, end_node_id+1)
                    newcbase.min_node_id, newcbase.min_val = self.__getMinNode_Val(end_node_id, end_node_id+1)                    
                    self.centralbase_list.append(newcbase)
                    
                    if self.centralbase_list[-1].ctype < self.centralbase_list[-2].ctype:
                        self.sec_sell_point_list.append(SellPoint(self.node_list[-2].datetime,  len(self.node_list)-2,\
                                                           real_time=self.__get_lowest_current_time("5MIN"))) 
                    if (self.centralbase_list[-1].ctype >0) and (self.centralbase_list[-1].ctype*self.centralbase_list[-2].ctype<0):
                        self.third_buy_point_list.append(SellPoint(self.node_list[-2].datetime,  len(self.node_list)-2,\
                                                           real_time=self.__get_lowest_current_time("5MIN")))                    
                

    def getNodeList_Lower_Step(self):
        lower_CB_list = self.low_CB_set.centralbase_list
        length = len(lower_CB_list)
        index = length-1
        
        if length<2:        
            return
        
        pre_base = lower_CB_list[-2]
        base = lower_CB_list[-1] 
        
        if self.freq=='30MIN' and abs(base.up-4.35)<0.001:
            a=1 
        if self.freq=='30MIN':
            a=1           
        
        if (length==2) and len(self.node_list)==0:
            self.seek_max = M_TO_UP
            if 1==self.__get_CB_pos(pre_base, base):  
                self.seek_max = M_TO_DOWN
            else:
                self.seek_max = M_TO_UP
                
            #生成新临时节点
            self.__Make_New_Temp_Node_Lower(self.seek_max, base.start, base.end, index) 
            return
        
        
        if self.cur_cut_low_id != index:
            self.cur_cut_low_id = index
            self.cur_cut_start_node_id = base.start_node_id
            
        cur_base_start_node_id = self.cur_cut_start_node_id
        cur_base_end_node_id = base.end_node_id                 

        '''
        #中枢升级逻辑

        if (cur_base_end_node_id - cur_base_start_node_id)==9:
            
            if self.freq=='D':
                a=1               
            
            self.node_list.pop()
            self.__Make_New_Temp_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, cur_base_start_node_id+3, index)
            self.node_list[-1].isformal = M_FORMAL
            cur_base_start_node_id = cur_base_start_node_id+3

            self.seek_max=self.__reverse_direct(self.seek_max)
            
            self.__Make_New_Temp_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, cur_base_start_node_id+3, index)
            self.node_list[-1].isformal = M_FORMAL
            cur_base_start_node_id = cur_base_start_node_id+3
            
            #进行中枢计算
            self.get_Centralbase_Step()             
            
            self.seek_max=self.__reverse_direct(self.seek_max)
            self.__Make_New_Temp_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, cur_base_start_node_id+3, index)
            cur_base_start_node_id = cur_base_start_node_id+3
            self.cur_cut_start_node_id = cur_base_start_node_id
            return
            '''      
        if self.seek_max==M_TO_UP: #向上
            #当前中枢在前一中枢下或相交，当前趋势结束
            if((0<self.__get_CB_pos(pre_base, base)) and (index>self.node_list[-1].low_id)):
                #更新正式节点信息
                #self.__Update_Last_Node_Lower_WithID(self.seek_max, pre_base.start, pre_base.end, isformal=M_FORMAL)
                self.node_list[-1].isformal = M_FORMAL
                
                #生成新临时节点
                self.seek_max = M_TO_DOWN
                self.__Make_New_Temp_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, base.end_node_id, index)
                
            else:#趋势延续
                self.__Update_Last_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, base.end_node_id, low_id=index)                        
        else:
            #当前中枢在前一中枢上或相交，当前趋势结束
            if((0>self.__get_CB_pos(pre_base, base)) and (index>self.node_list[-1].low_id)):
                #更新正式节点信息
                #self.__Update_Last_Node_Lower(self.seek_max, pre_base.start, pre_base.end, isformal=M_FORMAL)
                self.node_list[-1].isformal = M_FORMAL
                
                #生成新临时节点
                self.seek_max = M_TO_UP
                self.__Make_New_Temp_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, base.end_node_id, index)
                
            else:#趋势延续
                self.__Update_Last_Node_Lower_WithID(self.seek_max, cur_base_start_node_id, base.end_node_id, low_id=index) 
                     

    def __Make_New_Temp_Node_Lower(self, seek_max, start_time, end_time, low_id=None):
        '''
        生成新的临时节点
        seek_max:该临时节点与上一节点的关系
        '''
        lower_data = self.low_CB_set.data
        if seek_max==M_TO_UP:
            time,value = self.__getMaxIndex_Val(lower_data, start_time, end_time)
        else:
            time,value = self.__getMinIndex_Val(lower_data, start_time, end_time)
        if time==None:
            time_seg = self.data.ix[self.data.index>end_time, 'close']
            time = time_seg.index[0]
            value = self.data.ix[0, 'close']                    
        self.node_list.append(Node(time, value, M_TOP, low_id=low_id, isformal=M_TEMP))
      
      
    def __Make_New_Temp_Node_Lower_WithID(self, seek_max, start_node_id, end_node_id, low_id=None):
        '''
        生成新的临时节点
        seek_max:该临时节点与上一节点的关系
        '''
        lower_node_list = self.low_CB_set.node_list
        if seek_max==M_TO_UP:
            node_id,value = self.__getMaxLowerNode_Val( start_node_id, end_node_id)
        else:
            node_id,value = self.__getMinLowerNode_Val( start_node_id, end_node_id)
                   
        self.node_list.append(Node(lower_node_list[node_id].datetime, value, M_TOP, low_id=low_id, isformal=M_TEMP))
        
    def __Update_Last_Node_Lower(self, seek_max, start_time, end_time, isformal=None, low_id = None) :
        '''
        更新最后节点信息
        seek_max:该临时节点与上一节点的关系
        '''
        lower_data = self.low_CB_set.data
        if seek_max==M_TO_UP:
            time,value = self.__getMaxIndex_Val(lower_data, start_time, end_time)
        else:
            time,value = self.__getMinIndex_Val(lower_data, start_time, end_time)
            
        if time==None:
            time_seg = self.data.ix[self.data.index>end_time, 'close']
            time = time_seg.index[0]
            value = self.data.ix[0, 'close'] 
        if ((seek_max==M_TO_UP) and (value>self.node_list[-1].value))\
           or ((seek_max==M_TO_DOWN) and (value<self.node_list[-1].value)):
            self.node_list[-1].datetime = time
            self.node_list[-1].value = value
            if low_id!=None:
                self.node_list[-1].low_id = low_id            
        if isformal!=None:
            self.node_list[-1].isformal = isformal
            
    def __Update_Last_Node_Lower_WithID(self, seek_max, start_node_id, end_node_id, isformal=None, low_id = None) :
        '''
        更新最后节点信息
        seek_max:该临时节点与上一节点的关系
        '''
        lower_node_list = self.low_CB_set.node_list
        if seek_max==M_TO_UP:
            node_id,value = self.__getMaxLowerNode_Val( start_node_id, end_node_id)
        else:
            node_id,value = self.__getMinLowerNode_Val( start_node_id, end_node_id)
            
        if ((seek_max==M_TO_UP) and (value>self.node_list[-1].value))\
           or ((seek_max==M_TO_DOWN) and (value<self.node_list[-1].value)):
            self.node_list[-1].datetime = lower_node_list[node_id].datetime
            self.node_list[-1].value = value
            if low_id!=None:
                self.node_list[-1].low_id = low_id            
        if isformal!=None:
            self.node_list[-1].isformal = isformal
            
    def __reverse_direct(self, seek_max):
        if seek_max == M_TO_UP:
            return M_TO_DOWN
        else:
            return M_TO_UP
        
    def __get_lowest_current_time(self, freq):
        low_cb_set = self.low_CB_set
        while(low_cb_set!=None):
            if low_cb_set.freq == freq:
                return low_cb_set.cur_time_index
            else:
                low_cb_set = low_cb_set.low_CB_set
        return self.cur_time_index
    
    def get_lower_beichi(self):
        if self.low_CB_set!=None:
            low_beichi_list = self.low_CB_set.beichi_list
            low_node_list = self.low_CB_set.node_list
            if len(low_beichi_list)<=0 \
               or len(low_node_list)<2 \
               or len(self.centralbase_list)<=0:
                return
            
            if self.freq=='30MIN' :
                if abs(low_node_list[-2].value-36)<0.001:
                    a=1            
            
            if (low_beichi_list[-1].time == low_node_list[-2].datetime) and (self.cur_low_beichi_time != low_node_list[-2].datetime):
                self.cur_low_beichi_time = low_node_list[-2].datetime
                base = self.centralbase_list[-1]
                if(base.ctype<=-2):
                    if low_node_list[-2].value <= self.cur_min_value:#创新低
                        pre_macd = self.__getMACD_Sum(self.node_list[self.cur_min_node_id-1].datetime, self.node_list[self.cur_min_node_id].datetime, seekMax=False)
                        cur_macd = self.__getMACD_Sum(low_node_list[-3].datetime, low_node_list[-2].datetime, seekMax=False)
                        
                        pre_vol = self.__getVolumn_Sum(self.node_list[self.cur_min_node_id-1].datetime, self.node_list[self.cur_min_node_id].datetime, seekMax=False)
                        cur_vol = self.__getVolumn_Sum(low_node_list[-3].datetime, low_node_list[-2].datetime, seekMax=False)
                                               
                        if (abs(cur_macd) < abs(pre_macd)) or (abs(cur_vol)<abs(pre_vol)):
                            self.beichi_list.append(BeichiTime(low_node_list[-2].datetime, base.ctype,  len(self.node_list)-1,\
                                                       real_time=self.__get_lowest_current_time("5MIN")))
                            self.first_buy_point_list.append(BuyPoint(low_node_list[-2].datetime, len(self.node_list)-1,\
                                                               real_time=self.__get_lowest_current_time("5MIN")))                                               
                elif (base.ctype>=2):
                    if low_node_list[-2].value >= self.cur_max_value:#创新高
                        pre_macd = self.__getMACD_Sum(self.node_list[self.cur_max_node_id-1].datetime, self.node_list[self.cur_max_node_id].datetime, seekMax=True)
                        cur_macd = self.__getMACD_Sum(low_node_list[-3].datetime, low_node_list[-2].datetime, seekMax=True)
                    
                        if abs(cur_macd) < abs(pre_macd):
                            self.beichi_list.append(BeichiTime(low_node_list[-2].datetime, base.ctype,  len(self.node_list)-1,\
                                                       real_time=self.__get_lowest_current_time("5MIN")))
                            self.first_sell_point_list.append(SellPoint(low_node_list[-2].datetime, len(self.node_list)-1,\
                                                               real_time=self.__get_lowest_current_time("5MIN")))                                                                         
    
    def getBeichi_LastTwo_Step(self):
        '''
        分步获取背驰节点
        返回当前中枢新加入节点是否为背驰点
        调用时机：
        新的正式节点加入中枢，并未更新此中枢的极值信息
        '''
        
        if(len(self.centralbase_list)<2):
            return False
        
        pre_base = self.centralbase_list[-2]
        base = self.centralbase_list[-1]
        
        if (self.cur_min_node_id == len(self.node_list)-2) \
           or (self.cur_max_node_id == len(self.node_list)-2):
            return self.beichi_processing
    
        
        if base.ctype==0 or pre_base.ctype*base.ctype<0:
            self.cur_max_node_id = base.max_node_id
            self.cur_max_value = base.max_val
            self.cur_min_node_id = base.min_node_id
            self.cur_min_value = base.min_val
        else:
            if (self.low_CB_set==None):
                cur_macd = 0
                pre_macd = 0
                base = self.centralbase_list[-1]
                if(base.ctype<=-2):
                    if self.node_list[-2].value <= self.cur_min_value:#创新低
                        pre_macd = self.__getMACD_Sum(self.node_list[self.cur_min_node_id-1].datetime, self.node_list[self.cur_min_node_id ].datetime, seekMax=False)
                        cur_macd = self.__getMACD_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=False)
                        self.cur_min_node_id = len(self.node_list)-2
                        self.cur_min_value = self.node_list[-2].value
                        if abs(cur_macd) < abs(pre_macd):
                            self.beichi_list.append(BeichiTime(self.node_list[-2].datetime,base.ctype, len(self.node_list)-2,\
                                                               real_time=self.__get_lowest_current_time("5MIN")))
                            self.first_buy_point_list.append(BuyPoint(self.node_list[-2].datetime,len(self.node_list)-2,\
                                                               real_time=self.__get_lowest_current_time("5MIN")))                            
                            return True                        
                elif (base.ctype>=2):
                    if self.node_list[-2].value >= self.cur_max_value:#创新高
                        pre_macd = self.__getMACD_Sum(self.node_list[self.cur_max_node_id-1].datetime, self.node_list[self.cur_max_node_id].datetime, seekMax=True)
                        cur_macd = self.__getMACD_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=True)
                        self.cur_max_node_id = len(self.node_list)-2
                        self.cur_max_value = self.node_list[-2].value 
                        if abs(cur_macd) < abs(pre_macd):
                            self.beichi_list.append(BeichiTime(self.node_list[-2].datetime,base.ctype, len(self.node_list)-2,\
                                                               real_time=self.__get_lowest_current_time("5MIN")))
                            self.first_sell_point_list.append(SellPoint(self.node_list[-2].datetime,  len(self.node_list)-2,\
                                                               real_time=self.__get_lowest_current_time("5MIN")))                             
                            return True                         
                else:
                    return self.beichi_processing
                
            else:
                cur_macd = 0
                pre_macd = 0
                base = self.centralbase_list[-1]
                if(base.ctype<=-2):
                    if self.node_list[-2].value <= self.cur_min_value:#创新低
                        pre_macd = self.__getMACD_Sum(self.node_list[self.cur_min_node_id-1].datetime, self.node_list[self.cur_min_node_id].datetime, seekMax=False)
                        cur_macd = self.__getMACD_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=False)
                        
                        pre_vol = self.__getVolumn_Sum(self.node_list[self.cur_min_node_id-1].datetime, self.node_list[self.cur_min_node_id].datetime, seekMax=False)
                        cur_vol = self.__getVolumn_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=False)
                        
                        self.cur_min_node_id = len(self.node_list)-2
                        self.cur_min_value = self.node_list[-2].value                         
                        #if (abs(cur_macd) < abs(pre_macd)) or (abs(cur_vol)<abs(pre_vol)):
                            #self.beichi_list.append(BeichiTime(self.node_list[-2].datetime, base.ctype,  len(self.node_list)-2,\
                                                       #real_time=self.__get_lowest_current_time("5MIN")))
                            #self.first_buy_point_list.append(BuyPoint(self.node_list[-2].datetime, len(self.node_list)-2,\
                                                               #real_time=self.__get_lowest_current_time("5MIN")))                            
                            #return True                     
                elif (base.ctype>=2):
                    if self.node_list[-2].value >= self.cur_max_value:#创新高
                        pre_macd = self.__getMACD_Sum(self.node_list[self.cur_max_node_id-1].datetime, self.node_list[self.cur_max_node_id].datetime, seekMax=True)
                        cur_macd = self.__getMACD_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=True)
                        self.cur_max_node_id = len(self.node_list)-2
                        self.cur_max_value = self.node_list[-2].value                        
                        #if abs(cur_macd) < abs(pre_macd):
                            #self.beichi_list.append(BeichiTime(self.node_list[-2].datetime, base.ctype,  len(self.node_list)-2,\
                                                       #real_time=self.__get_lowest_current_time("5MIN")))
                            #self.first_sell_point_list.append(SellPoint(self.node_list[-2].datetime, len(self.node_list)-2,\
                                                               #real_time=self.__get_lowest_current_time("5MIN")))                                                     
                            #return True                       
                else:
                    return self.beichi_processing
                    
        return self.beichi_processing    
    

    def getBeichi_LastOne_Step(self):
        '''
        分步获取背驰节点
        返回当前中枢新加入节点是否为背驰点
        调用时机：
        新的正式节点加入中枢，并未更新此中枢的极值信息
        '''
        
        if(len(self.centralbase_list)<2):
            return False
        
        pre_base = self.centralbase_list[-2]
        base = self.centralbase_list[-1]
        
        if (self.cur_min_node_id == len(self.node_list)-2) \
           or (self.cur_max_node_id == len(self.node_list)-2):
            return self.beichi_processing
    
        
        if base.ctype==0 or pre_base.ctype*base.ctype<0:
            self.cur_max_node_id = base.max_node_id
            self.cur_max_value = base.max_val
            self.cur_min_node_id = base.min_node_id
            self.cur_min_value = base.min_val
        else:
            if (self.low_CB_set==None):
                cur_macd = 0
                pre_macd = 0
                base = self.centralbase_list[-1]
                if(base.ctype<=-2):
                    if self.node_list[-2].value <= self.cur_min_value:#创新低
                        pre_macd = self.__getMACD_Sum(self.node_list[self.cur_min_node_id-1].datetime, self.node_list[self.cur_min_node_id ].datetime, seekMax=False)
                        cur_macd = self.__getMACD_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=False)
                        self.cur_min_node_id = len(self.node_list)-2
                        self.cur_min_value = self.node_list[-2].value
                        if abs(cur_macd) < abs(pre_macd):
                            self.beichi_list.append(BeichiTime(self.node_list[-2].datetime,base.ctype, len(self.node_list)-2,\
                                                               real_time=self.__get_lowest_current_time("5MIN")))
                            self.first_buy_point_list.append(BuyPoint(self.node_list[-2].datetime,len(self.node_list)-2,\
                                                               real_time=self.__get_lowest_current_time("5MIN")))                            
                            return True                        
                elif (base.ctype>=2):
                    if self.node_list[-2].value >= self.cur_max_value:#创新高
                        pre_macd = self.__getMACD_Sum(self.node_list[self.cur_max_node_id-1].datetime, self.node_list[self.cur_max_node_id].datetime, seekMax=True)
                        cur_macd = self.__getMACD_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=True)
                        self.cur_max_node_id = len(self.node_list)-2
                        self.cur_max_value = self.node_list[-2].value 
                        if abs(cur_macd) < abs(pre_macd):
                            self.beichi_list.append(BeichiTime(self.node_list[-2].datetime,base.ctype, len(self.node_list)-2,\
                                                               real_time=self.__get_lowest_current_time("5MIN")))
                            self.first_sell_point_list.append(SellPoint(self.node_list[-2].datetime,  len(self.node_list)-2,\
                                                               real_time=self.__get_lowest_current_time("5MIN")))                             
                            return True                         
                else:
                    return self.beichi_processing
                
            else:
                cur_macd = 0
                pre_macd = 0
                base = self.centralbase_list[-1]
                if(base.ctype<=-2):
                    if self.node_list[-2].value <= self.cur_min_value:#创新低
                        pre_macd = self.__getMACD_Sum(self.node_list[self.cur_min_node_id-1].datetime, self.node_list[self.cur_min_node_id].datetime, seekMax=False)
                        cur_macd = self.__getMACD_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=False)
                        
                        pre_vol = self.__getVolumn_Sum(self.node_list[self.cur_min_node_id-1].datetime, self.node_list[self.cur_min_node_id].datetime, seekMax=False)
                        cur_vol = self.__getVolumn_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=False)
                        
                        self.cur_min_node_id = len(self.node_list)-2
                        self.cur_min_value = self.node_list[-2].value                         
                        #if (abs(cur_macd) < abs(pre_macd)) or (abs(cur_vol)<abs(pre_vol)):
                            #self.beichi_list.append(BeichiTime(self.node_list[-2].datetime, base.ctype,  len(self.node_list)-2,\
                                                       #real_time=self.__get_lowest_current_time("5MIN")))
                            #self.first_buy_point_list.append(BuyPoint(self.node_list[-2].datetime, len(self.node_list)-2,\
                                                               #real_time=self.__get_lowest_current_time("5MIN")))                            
                            #return True                     
                elif (base.ctype>=2):
                    if self.node_list[-2].value >= self.cur_max_value:#创新高
                        pre_macd = self.__getMACD_Sum(self.node_list[self.cur_max_node_id-1].datetime, self.node_list[self.cur_max_node_id].datetime, seekMax=True)
                        cur_macd = self.__getMACD_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=True)
                        self.cur_max_node_id = len(self.node_list)-2
                        self.cur_max_value = self.node_list[-2].value                        
                        #if abs(cur_macd) < abs(pre_macd):
                            #self.beichi_list.append(BeichiTime(self.node_list[-2].datetime, base.ctype,  len(self.node_list)-2,\
                                                       #real_time=self.__get_lowest_current_time("5MIN")))
                            #self.first_sell_point_list.append(SellPoint(self.node_list[-2].datetime, len(self.node_list)-2,\
                                                               #real_time=self.__get_lowest_current_time("5MIN")))                                                     
                            #return True                       
                else:
                    return self.beichi_processing
                    
        return self.beichi_processing

            

    def beichi_judge_step(self):
        for beichi in self.beichi_list:
            if beichi.real_beichi == M_NODECIDE:
                if beichi.node_id + 4 == len(self.node_list):
                    if beichi.btype >0 and self.node_list[beichi.node_id].value>=self.node_list[-2].value: #顶背驰判断
                        beichi.real_beichi = M_TRUE
                    elif beichi.btype <0 and self.node_list[beichi.node_id].value<=self.node_list[-2].value: #低背驰判断
                        beichi.real_beichi = M_TRUE
                        self.sec_buy_point_list.append(BuyPoint(self.node_list[-2].datetime, len(self.node_list)-2,\
                                                           real_time=self.__get_lowest_current_time("5MIN")))                        
                    else:
                        beichi.real_beichi = M_FALSE   
            
    def trade_strategy_step(self, high_cb_set):
        for sec_buy_point in self.sec_buy_point_list:
            if sec_buy_point.real_buy==M_NODECIDE:
                if len(high_cb_set.centralbase_list)>0 and high_cb_set.centralbase_list[-1].ctype>=-2:
                    sec_buy_point.real_buy = M_TRUE
                else:
                    sec_buy_point.real_buy = M_FALSE
        
    def sell_point_judge(self):
        if len(self.node_list)<3:
            return
        if len(self.first_buy_point_list)>0:
            if len(self.node_list)-2 == (self.first_buy_point_list[-1].node_id+1):
                self.all_sell_point_list.append(SellPoint(self.node_list[-2].datetime,  len(self.node_list)-2,\
                                                   real_time=self.__get_lowest_current_time("5MIN")))
        if len(self.sec_buy_point_list)>0:
            if len(self.node_list)-2 == (self.sec_buy_point_list[-1].node_id+1):
                self.all_sell_point_list.append(SellPoint(self.node_list[-2].datetime,  len(self.node_list)-2,\
                                                   real_time=self.__get_lowest_current_time("5MIN")))  
        if len(self.third_buy_point_list)>0:        
            if len(self.node_list)-2 == (self.third_buy_point_list[-1].node_id+1):
                self.all_sell_point_list.append(SellPoint(self.node_list[-2].datetime,  len(self.node_list)-2,\
                                                   real_time=self.__get_lowest_current_time("5MIN")))          
        
    def __getMACD_Sum(self, start_time, end_time, seekMax=True):
        if self.low_CB_set!= None:
            data_seg = self.low_CB_set.data.ix[(self.low_CB_set.data.index>=start_time) & (self.low_CB_set.data.index<=end_time) & (self.low_CB_set.data.index <= self.low_CB_set.cur_time_index), 'MACD']
        else:
            data_seg = self.data.ix[(self.data.index>=start_time) & (self.data.index<=end_time) & (self.data.index <= self.cur_time_index), 'MACD']
        #data_seg = self.data.ix[(self.data.index>=start_time) & (self.data.index<=end_time) & (self.data.index <= self.cur_time_index), 'MACD']
        if seekMax:
            data_seg = data_seg[data_seg>0]
        else:
            data_seg = data_seg[data_seg<0]
        return data_seg.sum()
    
    def __getVolumn_Sum(self, start_time, end_time, seekMax=True):
        if self.low_CB_set!= None:
            data_seg = self.low_CB_set.data.ix[(self.low_CB_set.data.index>=start_time) & (self.low_CB_set.data.index<=end_time) & (self.low_CB_set.data.index <= self.low_CB_set.cur_time_index), 'volume']
        else:
            data_seg = self.data.ix[(self.data.index>=start_time) & (self.data.index<=end_time) & (self.data.index <= self.cur_time_index), 'volume']

        return data_seg.sum()    
         
    def __get_CB_pos(self, first, second):
        """
        获取两个中枢的相对位置:1前在后上，-1前在后下，0相交
        """
        #if (first.up <=second.down):
            #return -1
        #elif (first.down >=second.up) :
            #return 1
        #else:
            #return 0
        
        if (first.up <second.up) and (first.down <=second.down):
            return -1
        elif (first.down >second.down) and (first.up >= second.up) :
            return 1
        else:
            return 0
        
    def __getMaxIndex_Val(self, data, start, end):
        data_seg = data.ix[(data.index>=start)&(data.index<=end), 'close']
        if data_seg.any():
            return (data_seg.idxmax(), data_seg.max())
        else:
            return (None, None)
        
    def __getMinIndex_Val(self, data, start, end):
        data_seg = data.ix[(data.index>=start)&(data.index<=end), 'close']
        if data_seg.any():
            return (data_seg.idxmin(), data_seg.min())
        else:
            return (None, None)
        
    def __getMaxNode_Val(self, start_in, end_in):
        val = 0.0
        val_index = -1
        for index in range(start_in, end_in+1):
            if self.node_list[index].value > val:
                val = self.node_list[index].value
                val_index = index
        return (val_index, val)
    
    def __getMaxLowerNode_Val(self, start_in, end_in):
        val = 0.0
        val_index = -1
        for index in range(start_in, end_in+1):
            if self.low_CB_set.node_list[index].value > val:
                val = self.low_CB_set.node_list[index].value
                val_index = index
        return (val_index, val)    
    
    def __getMinNode_Val(self, start_in, end_in):
        val = 5000
        val_index = -1
        for index in range(start_in, end_in+1):
            if self.node_list[index].value < val:
                val = self.node_list[index].value
                val_index = index
        return (val_index, val)    
            
    def __getMinLowerNode_Val(self, start_in, end_in):
        val = 5000
        val_index = -1
        for index in range(start_in, end_in+1):
            if self.low_CB_set.node_list[index].value < val:
                val = self.low_CB_set.node_list[index].value
                val_index = index
        return (val_index, val)             
            
    def __getSegment(self, i):
        """
        i from 0
        """
        if i<0 or i>np.size(self.node_list)-1:
            return None
        return Interval(lower_bound=self.node_list[i].value, upper_bound=self.node_list[i+1].value, 
                        lower_closed=False, upper_cloesed=False)
    
    def __getSegmentStart(self, i):
        """
        i from 0
        """
        if i<0 or i>np.size(self.node_list)-1:
            return None
        return self.node_list[i].datetime       
            
    def __getSegmentEnd(self, i):
        """
        i from 0
        """
        if i<0 or i>np.size(self.node_list)-1:
            return None
        return self.node_list[i+1].datetime
    
    def __getCBType(self, newcbase, isnew=True, cb_id=None):
        if isnew:
            if(np.size(self.centralbase_list)<1):
                return 0            
            r_pos = self.__get_CB_pos(self.centralbase_list[-1], newcbase)
            pre_ctype = self.centralbase_list[-1].ctype
            #if pre_ctype==0:#前一个是起点或背驰形成的第一中枢
            #    return (-2*r_pos)
        else:
            if cb_id-1<0:
                return 0
            r_pos = self.__get_CB_pos(self.centralbase_list[cb_id-1], self.centralbase_list[cb_id])
            pre_ctype = self.centralbase_list[cb_id-1].ctype
            if self.centralbase_list[cb_id].ctype==0:#前一个是起点或背驰形成的第一中枢
                return 0            
            
        if(0==r_pos):
            return pre_ctype
        else:
            if((r_pos*pre_ctype) > 0):#转折
                if abs(pre_ctype) >=3:
                    return (-2*r_pos)
                else:
                    return (-2*r_pos)
            elif((r_pos*pre_ctype) < 0):#延续
                return (pre_ctype-r_pos)
            else:
                return(-1*r_pos)         
        
                            
                        

            