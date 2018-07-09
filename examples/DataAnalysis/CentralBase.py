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


class Node:
    """ 
    趋势节点，包含（时间， 值）
    low_id:次级中枢编号
    low_count:当前点累计的次级中枢数目
    """
    def __init__(self, time, value, ntype, low_id=None, low_count=None):
        self.datetime = time
        self.value = value
        self.ntype= ntype
        self.low_id = low_id
        self.low_count = low_count
        
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
    """
    def __init__(self, start, end, up, down, start_node_id=None, end_node_id=None):
        self.start = start
        self.end = end 
        self.up = up
        self.down = down
        self.start_node_id = start_node_id
        self.end_node_id = end_node_id
        self.ctype = 0
    def setCType(self, ctype):
        self.ctype = ctype
        
class BeichiTime:
    '''
    time:背驰时间
    btype:背驰类型，1顶背驰 -1底背驰
    '''
    def __init__(self, time, btype):
        self.time = time
        self.btype = btype
        
class CentralBaseSet:
        
    def __init__(self, freq, dataframe, low_CB_set=None):
        self.data = dataframe.set_index(dataframe['datetime'])
        self.low_CB_set = low_CB_set
        self.node_list = []
        self.centralbase_list = []
        self.freq = freq
        self.beichi_list=[]

        
    def analyze_CB(self):
        self.getNodeList()
        self.get_Centralbase()
        self.indexGenerator()
        if self.freq=='30MIN':
            self.getBeiChi()
        self.resultToSource()
        
        
    def __getmax(self, a, b):
        return a if a>b else b
    
    def __getmin(self, a,b):
        return a if a<b else b
    
    def resultToSource(self):
        self.data['node'] = None
        self.data['base_up'] = None
        self.data['base_down'] = None
        self.data['beichi'] = None
        for node in self.node_list:
            self.data.set_value(node.datetime, 'node', node.value)
        for base in self.centralbase_list:
            self.data.ix[base.start:base.end,'base_up'] = base.up
            self.data.ix[base.start:base.end,'base_down'] = base.down
            self.data.ix[base.start:base.end,'base_type'] = base.ctype
        for beichi in self.beichi_list:
            self.data.ix[beichi.time, 'beichi'] = self.data.ix[beichi.time, 'close']

        
    def indexGenerator(self):
        self.data['SMA5'] = ta.SMA(self.data['close'].values, timeperiod = 5)  #5日均线
        self.data['SMA10'] = ta.SMA(self.data['close'].values, timeperiod = 10)  #10日均线
        macd_talib, signal, hist = ta.MACD(self.data['close'].values,fastperiod=12,signalperiod=9)
        self.data['DIF'] = macd_talib #DIF
        self.data['DEA'] = signal #DEA
        self.data['MACD'] = hist #MACD        
            
    def getNodeList_KLine(self):
        try:
            self.data['up'] = self.data['open'] <= self.data['close']
            
            #初始变量
            time = self.data.index[0]
            value = 0
            seek_max = 	True
            if self.data.get_value(time, 'up'):
                value = self.data.get_value(time, 'close')
                seek_max = True
            else:
                value = self.data.get_value(time, 'close')
                seek_max = False
            
            #开始搜索 
            count = 0
            print u'--getNodeList: TOTAL %d' %(np.size(self.data, axis=0))
            for index in self.data.index:
                if(count % 100 == 0):
                    print u'--getNodeList:%d' %(count)
                count +=1
                if seek_max:
                    if self.data.get_value(index, 'up'):
                        if self.data.get_value(index, 'close') >=value:
                            time = index
                            value = self.data.get_value(index, 'close')
                    else:
                        if self.data.get_value(index, 'close') < value:
                            self.node_list.append(Node(time, value, 1))
                            time = index
                            value = self.data.get_value(index, 'close')
                            seek_max = False
                else:
                    if not self.data.get_value(index, 'up'):
                        if self.data.get_value(index, 'close') <= value:
                            time = index
                            value = self.data.get_value(index, 'close')
                    else:
                        if self.data.get_value(index, 'close') > value:
                            self.node_list.append(Node(time, value, -1))
                            time = index
                            value = self.data.get_value(index, 'close')
                            seek_max = True
                        if abs(self.data.get_value(index, 'close') - self.data.get_value(index, 'open')) <=0.001: #排查十字星的情况
                            if self.data.get_value(index, 'close') <= value:
                                time = index
                                value = self.data.get_value(index, 'close')                            
                            
        except Exception as e:
            print("getNodeList_KLine EEROR.", e)
            
    def getNodeList_Lower(self):
        try:
            lower_CB_list = self.low_CB_set.centralbase_list
            lower_data = self.low_CB_set.data
            length = len(lower_CB_list)
            if length<2:
                return
            seek_max = True
            pre_base = lower_CB_list[0]
            base = lower_CB_list[1]
            if 1==self.__get_CB_pos(pre_base, base):  
                seek_max = False
            else:
                seek_max = True
            
            pre_base=None
            low_count=0
            for index in range(length):
                base = lower_CB_list[index]
                if (self.freq == "D") and abs(base.up-4.6)<0.001:
                    a = 1                     
                if seek_max:
                    if (pre_base != None) and (0<=self.__get_CB_pos(pre_base, base)):
                        low_time,low_value = self.__getMaxIndex_Val(lower_data, pre_base.start, pre_base.end)
                        if (len(self.node_list)>0) and (low_time<=self.node_list[-1].datetime):
                            if pre_base.end>self.node_list[-1].datetime:
                                low_time = self.node_list[-1].datetime
                            else:
                                low_time = None
                        if low_time != None:
                            time_seg = self.data.ix[self.data.index>low_time, 'close']
                            if time_seg.any():
                                time = time_seg.index[0]
                                value = low_value
                                self.node_list.append(Node(time, value, 1, index-1, low_count))
                                seek_max = False
                                low_count = 1
                        else:
                                seek_max = False
                                pre_base = lower_CB_list[self.node_list[-1].low_id]   
                                low_count = self.node_list[-1].low_count
                                self.node_list.pop(-1)
                                index = index - 1
                    else:
                        if (pre_base != None):
                            low_count += 1
                else:
                    if (pre_base != None) and (0>=self.__get_CB_pos(pre_base, base)):
                        low_time,low_value = self.__getMinIndex_Val(lower_data, pre_base.start, pre_base.end)
                        if (len(self.node_list)>0) and (low_time<=self.node_list[-1].datetime):
                            if pre_base.end>self.node_list[-1].datetime:
                                low_time = self.node_list[-1].datetime
                            else:
                                low_time = None
                        if low_time != None:
                            time_seg = self.data.ix[self.data.index>low_time, 'close']
                            if time_seg.any():
                                time = time_seg.index[0]
                                value = low_value
                                self.node_list.append(Node(time, value, -1, index-1, low_count))
                                seek_max = True
                                low_count = 1
                        else:
                                seek_max = True
                                pre_base = lower_CB_list[self.node_list[-1].low_id] 
                                low_count = self.node_list[-1].low_count
                                self.node_list.pop(-1)
                                index = index - 1
                    else:
                        if (pre_base != None):
                            low_count += 1                    
                pre_base = base
            
        except Exception as e:
            print("getNodeList_Lower EEROR.", e)
            
    
    def getNodeList(self):
        try:
            if self.low_CB_set==None:
                print "getNodeList使用K线数据"
                self.getNodeList_KLine()
            else:
                print "getNodeList使用次级中枢数据"
                self.getNodeList_Lower()
        except Exception as e:
            print("getNodeList EEROR.", e)
            
    def get_Centralbase(self):
        size = np.size(self.node_list)
        seg_list=[]
        start = None
        end = None
        start_id = -1
        end_id = -1
        cross_itval = Interval.none()
        pre_cross_itval = Interval.none()
        
        if self.freq == 'D':
            a=1
        
        i=0 #段索引
        print u'--get_Centralbase: TOTAL NODE %d' %(size)
        count=0
        pre_cut = -1
        while(i<size-1):
            if(count % 50 == 0):
                print u'--get_Centralbase:%d' %(count)
            count +=1
            seg = self.__getSegment(i)           
            if np.size(seg_list) < 3:
                if (pre_cross_itval == Interval.none()) or (not seg.overlaps(pre_cross_itval)): 
                    seg_list.append(seg)
                    if np.size(seg_list) ==1:
                        start = self.__getSegmentStart(i)
                        start_id = i
                    end = self.__getSegmentEnd(i) 
                    end_id = i+1
                else:
                    seg_list=[]
                    cross_itval = Interval.none()
                    start = None
                    start_id = -1
                    end = None
                    end_id = -1
                i += 1
                    
                if (np.size(seg_list) >1) :
                    cross_itval = seg_list[0]
                    for t_seg in seg_list:
                        cross_itval = cross_itval & t_seg
                    if cross_itval == Interval.none():
                        seg_list.pop(0)
                        start = self.__getSegmentStart(i-2)
                        start_id = i-2
            else:
                if (pre_cross_itval == Interval.none()) or (not seg.overlaps(pre_cross_itval)):
                    if seg.overlaps(cross_itval):
                        #cross_itval = cross_itval & seg
                        end = self.__getSegmentEnd(i)
                        end_id = i+1
                        i += 1
                    else:
                        newcbase = Centralbase(start, end, cross_itval.upper_bound, cross_itval.lower_bound, start_id, end_id)
                        newcbase.setCType(self.__getCBType(newcbase))
                        self.centralbase_list.append(newcbase)
                        seg_list=[]
                        seg_list.append( self.__getSegment(i-1))
                        #pre_cross_itval = cross_itval
                        cross_itval = Interval.none()
                        start =  self.__getSegmentEnd(i-1)
                        start_id = i-1
                        end = None
                        end_id = -1
                else:
                    seg_list=[]
                    cross_itval = Interval.none()
                    start = None
                    start_id = -1
                    end = None
                    end_id = -1
                    i += 1
                    
            #根据趋势分割中枢
            #if (self.node_list[i].low_count>2):
                #if (np.size(seg_list) > 1): 
                    #cross_itval =  seg_list[0]
                    #for t_seg in seg_list:
                        #cross_itval = cross_itval & t_seg
                    #self.centralbase_list.append(Centralbase(start, end, cross_itval.upper_bound, cross_itval.lower_bound))
                    #seg_list=[]
                    ##pre_cross_itval = cross_itval
                    #cross_itval = Interval.none()
                    #start = None
                    #end = None  
                #else:
                    #if (np.size(seg_list) > 0):
                        ##self.centralbase_list[-1].end = end
                        #t_seg = seg_list[-1]
                        #seg_list=[]
                        #seg_list.append(t_seg)
                        #start = self.__getSegmentStart(i-1)
                        #end = self.__getSegmentEnd(i-1)
                        ##pre_cross_itval = cross_itval
                        #cross_itval = Interval.none()
                        ##start = None
                        ##end = None                      
                
            if (i==size-1) and (cross_itval != Interval.none()):
                newcbase=Centralbase(start, end, cross_itval.upper_bound, cross_itval.lower_bound, start_id, end_id)
                newcbase.setCType(self.__getCBType(newcbase))
                self.centralbase_list.append(newcbase)
                
    def getBeiChi(self):
        if (self.low_CB_set==None):
            return 
        
        low_cb_list = self.low_CB_set.centralbase_list
        trend_start=-1
        trend_end=-1
        length = len(self.centralbase_list)
        for index in range(length):
            base = self.centralbase_list[index]
            if((trend_start==-1) and ((base.ctype<=-2) or (base.ctype>=2))):
                trend_start = index
                
            if((trend_start>=0) and ((base.ctype>-2) and( base.ctype<2))):
                trend_end = index
                
            if ((trend_start>=0) and (trend_end>=0)):
                start_node_id = self.centralbase_list[trend_start].start_node_id
                end_node_id = self.centralbase_list[trend_end-1].end_node_id
                
                
                limit_val=0 
                if (base.ctype>0):#下背驰
                    seek_max = False
                    limit_val=5000.0
                else:#上背驰
                    seek_max = True
                    limit_val=0.0
                    
                pre_limit_node = None
                limit_node = None
                pre_macd_sum = 0
                macd_sum=0
                for n_index in range(start_node_id-1,end_node_id+1):
                    node = self.node_list[n_index]
                    calc = False
                    if (seek_max):#上背驰 
                        if(node.ntype==1) and (node.value>=limit_val):
                            limit_val = node.value
                            calc = True
                    else:
                        if(node.ntype==-1) and (node.value<=limit_val):
                            limit_val = node.value
                            calc = True
                    if calc:
                        if pre_limit_node == None:
                            pre_limit_node = node
                            start_time = self.node_list[n_index-1].datetime
                            end_time = low_cb_list[self.node_list[n_index].low_id].end
                            pre_macd_sum = abs(self.__getMACD_Sum(start_time, end_time, seek_max))
                        else:
                            limit_node = node
                            start_time = self.node_list[n_index-1].datetime
                            end_time = low_cb_list[self.node_list[n_index].low_id].end
                            macd_sum = abs(self.__getMACD_Sum(start_time, end_time, seek_max))
                            
                        if (pre_limit_node!=None) and (limit_node!=None):
                            if macd_sum<pre_macd_sum:
                                self.beichi_list.append(BeichiTime(self.node_list[n_index].datetime,seek_max))
                            pre_limit_node = limit_node
                            pre_macd_sum = macd_sum
                            limit_node = None
                
                trend_start = -1
                trend_end = -1
               
    
    def __getMACD_Sum(self, start, end, seekMax=True):
        data_seg = self.data.ix[(self.data.index>=start) & (self.data.index<end), 'MACD']
        if seekMax:
            data_seg = data_seg[data_seg>0]
        else:
            data_seg = data_seg[data_seg<0]
        return data_seg.sum()
         
    def __get_CB_pos(self, first, second):
        """
        获取两个中枢的相对位置:1前在后上，-1前在后下，0相交
        """
        if (first.up <=second.down):
            return -1
        elif (first.down >=second.up) :
            return 1
        else:
            return 0
        
        #if (first.up <=second.up) and (first.down <=second.down):
            #return -1
        #elif (first.down >=second.down) and (first.up >= second.up) :
            #return 1
        #else:
            #return 0
        
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
    
    def __getCBType(self, newcbase):
        if(np.size(self.centralbase_list)<=1):
            return 0
        else:
            r_pos = self.__get_CB_pos(self.centralbase_list[-1], newcbase)
            if(0==r_pos):
                return self.centralbase_list[-1].ctype
            else:
                if((r_pos*self.centralbase_list[-1].ctype) > 0):#转折
                    return (-1*r_pos) 
                elif((r_pos*self.centralbase_list[-1].ctype) < 0):#延续
                    return (self.centralbase_list[-1].ctype-r_pos)
                else:
                    return(-1*r_pos)         
        
                            
                        
                    
            