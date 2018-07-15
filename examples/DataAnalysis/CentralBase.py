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
    real_beichi:是否为真背驰
    '''
    def __init__(self, time, btype, graph_time=None):
        self.time = time
        self.btype = btype
        self.real_beichi = False
        self.graph_time = graph_time
 
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
        
    def __init__(self, freq, dataframe, low_CB_set=None):
        self.data = dataframe.set_index(dataframe['datetime'])
        self.low_CB_set = low_CB_set
        self.node_list = []
        self.centralbase_list = []
        self.freq = freq
        self.beichi_list=[]
        self.beichi_processing = False

        
    def analyze_CB(self):
        '''
        self.getNodeList()
        self.get_Centralbase()
        self.indexGenerator()
        if self.freq=='30MIN':
            self.getBeiChi()
        self.resultToSource()
        '''
        self.indexGenerator()
        
        if self.low_CB_set == None:
            self.getNodeList_KLine()
        else:
            if self.freq=="D":
                a=1
            self.getNodeList_Lower()
        
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
        #for node in self.node_list:
        #    self.data.set_value(node.datetime, 'node', node.value)
        for base in self.centralbase_list:
            self.data.ix[base.start:base.end,'base_up'] = base.up
            self.data.ix[base.start:base.end,'base_down'] = base.down
            self.data.ix[base.start:base.end,'base_type'] = base.ctype
        
        for beichi in self.beichi_list:
            if self.low_CB_set==None:
                self.data.ix[beichi.time, 'beichi'] = self.data.ix[beichi.time, 'close']
            else:
                self.data.ix[beichi.graph_time, 'beichi'] = self.data.ix[beichi.graph_time, 'close']

        
    def indexGenerator(self):
        self.data['SMA5'] = ta.SMA(self.data['close'].values, timeperiod = 5)  #5日均线
        self.data['SMA10'] = ta.SMA(self.data['close'].values, timeperiod = 10)  #10日均线
        macd_talib, signal, hist = ta.MACD(self.data['close'].values,fastperiod=12,signalperiod=9)
        self.data['DIF'] = macd_talib #DIF
        self.data['DEA'] = signal #DEA
        self.data['MACD'] = hist #MACD 
        
            
    def getNodeList_KLine(self):
        self.data['up'] = self.data['open'] <= self.data['close']
        
        #初始变量
        time = self.data.index[0]
        value = 0
        seek_max = M_TO_UP
        if self.data.get_value(time, 'up'):
            value = self.data.get_value(time, 'close')
            seek_max = M_TO_UP
            self.node_list.append(Node(time, value,M_TOP , isformal=M_TEMP))
        else:
            value = self.data.get_value(time, 'close')
            seek_max = M_TO_DOWN
            self.node_list.append(Node(time, value, M_BOTTOM, isformal=M_TEMP))
       
        #开始搜索 
        count = 0
        print u'--getNodeList: TOTAL %d' %(np.size(self.data, axis=0))
        for index in self.data.index:
            if(count % 100 == 0):
                print u'--getNodeList:%d' %(count)
            count +=1
            
            go_judge = True
            if seek_max == M_TO_UP:
                if abs(self.data.get_value(index, 'close') - self.data.get_value(index, 'open')) <=0.001: #排查十字星的情况
                    if self.data.get_value(index, 'close') >= self.node_list[-1].value:
                        self.node_list[-1].datetime = index
                        self.node_list[-1].value = self.data.get_value(index, 'close')
                    else:
                        go_judge = False                    
                if self.data.get_value(index, 'up') and go_judge:
                    if self.data.get_value(index, 'close') >= self.node_list[-1].value:
                        self.node_list[-1].datetime = index
                        self.node_list[-1].value = self.data.get_value(index, 'close')
                else:
                    if self.data.get_value(index, 'close') < self.node_list[-1].value:
                        self.node_list[-1].isformal = M_FORMAL
                        self.node_list.append(Node(index, self.data.get_value(index, 'close'), M_BOTTOM, isformal=M_TEMP))
                        seek_max = False
            else:
                if abs(self.data.get_value(index, 'close') - self.data.get_value(index, 'open')) <=0.001: #排查十字星的情况
                    if self.data.get_value(index, 'close') <= self.node_list[-1].value:
                        self.node_list[-1].datetime = index
                        self.node_list[-1].value = self.data.get_value(index, 'close')
                    else:
                        go_judge = False
                if (not self.data.get_value(index, 'up')) and go_judge:
                    if self.data.get_value(index, 'close') <= self.node_list[-1].value:
                        self.node_list[-1].datetime = index
                        self.node_list[-1].value = self.data.get_value(index, 'close')
                else:
                    if self.data.get_value(index, 'close') > self.node_list[-1].value:
                        self.node_list[-1].isformal = M_FORMAL
                        self.node_list.append(Node(index, self.data.get_value(index, 'close'), M_TOP, isformal=M_TEMP))
                        seek_max = True

            if abs(self.node_list[-1].value - 3.83) < 0.001:
                a = 1
            #进行中枢计算
            self.get_Centralbase_Step()   
                            

            
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
        seg_list=[]
        start = None
        end = None
        start_id = -1
        end_id = -1
        cross_itval = Interval.none()        
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
        elif self.beichi_processing:#背驰处理过程中
            node_list_len = len(self.node_list)
            if  node_list_len == self.centralbase_list[-1].end_node_id+4:#背驰后形成了三段走势
                self.beichi_processing = False
                if self.beichi_list[-1].btype<0 and self.node_list[-2].value > self.node_list[-4].value: #底背弛成立
                    self.beichi_list[-1].real_beichi = True
                elif self.beichi_list[-1].btype>0 and self.node_list[-2].value < self.node_list[-4].value: #顶背弛成立
                    self.beichi_list[-1].real_beichi = True
                else:
                    self.beichi_list[-1].real_beichi = False
                
                cross_itval = self.centralbase_list[-1].getCBInterval() & self.__getSegment(node_list_len-3)
                if (not self.beichi_list[-1].real_beichi) and (cross_itval != Interval.none()): #背弛不成立且有交点,更新中枢信息
                    if cross_itval != Interval.none():#新正式段与原中枢相交，更新中枢信息
                        self.centralbase_list[-1].up = cross_itval.upper_bound
                        self.centralbase_list[-1].down = cross_itval.lower_bound
                        self.centralbase_list[-1].end_node_id = node_list_len-2
                        self.centralbase_list[-1].end = self.node_list[node_list_len-2].datetime
                        #self.centralbase_list[-1].setCType(self.__getCBType(newcbase=None, isnew=False, cb_id=len(self.centralbase_list)-1))
                        
                        self.beichi_processing = self.getBeichi_Step()
                        
                        #更新极值信息
                        if self.node_list[-2].value > self.centralbase_list[-1].max_val:
                            self.centralbase_list[-1].max_val = self.node_list[-2].value
                            self.centralbase_list[-1].max_node_id = node_list_len-2
                        if self.node_list[-2].value < self.centralbase_list[-1].min_val:
                            self.centralbase_list[-1].min_val = self.node_list[-2].value
                            self.centralbase_list[-1].min_node_id = node_list_len-2                     
                else: #背弛成立或没有交点,生成新中枢
                    cross_itval = self.__getSegment(node_list_len-4) & self.__getSegment(node_list_len-3)
                    start = self.__getSegmentStart(node_list_len-4)
                    end = self.__getSegmentEnd(node_list_len-3)
                    newcbase = Centralbase(start, end, cross_itval.upper_bound, cross_itval.lower_bound, node_list_len-4, node_list_len-2, isformal=M_TEMP)
                    if self.beichi_list[-1].real_beichi:
                        newcbase.setCType(0)
                    else:
                        newcbase.setCType(self.__getCBType(newcbase))
                    newcbase.max_node_id, newcbase.max_val = self.__getMaxNode_Val(0, 2)
                    newcbase.min_node_id, newcbase.min_val = self.__getMinNode_Val(0, 2)
                    self.centralbase_list.append(newcbase)                    
                
        else:
            end_node_id = self.centralbase_list[-1].end_node_id
            if len(self.node_list)-2 > end_node_id: #新临时NODE已经形成，新正式NODE形成
                cross_itval = self.centralbase_list[-1].getCBInterval() & self.__getSegment(end_node_id)
                if cross_itval != Interval.none():#新正式段与原中枢相交，更新中枢信息
                    self.centralbase_list[-1].up = cross_itval.upper_bound
                    self.centralbase_list[-1].down = cross_itval.lower_bound
                    self.centralbase_list[-1].end_node_id = end_node_id+1
                    self.centralbase_list[-1].end = self.node_list[end_node_id+1].datetime
                    #self.centralbase_list[-1].setCType(self.__getCBType(newcbase=None, isnew=False, cb_id=len(self.centralbase_list)-1))
                    
                    self.beichi_processing = self.getBeichi_Step()
                    
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
                

    def getNodeList_Lower(self):
        lower_CB_list = self.low_CB_set.centralbase_list
        lower_data = self.low_CB_set.data
        length = len(lower_CB_list)
        if length<2:
            return
        seek_max = M_TO_UP
        pre_base = lower_CB_list[0]
        base = lower_CB_list[1]
        if 1==self.__get_CB_pos(pre_base, base):  
            seek_max = M_TO_DOWN
        else:
            seek_max = M_TO_UP
            
        #开始搜索 
        count = 0
        print u'--getNodeList: TOTAL %d' %(length)            
        for index in range(length):
            
            if(count % 100 == 0):
                print u'--getNodeList:%d' %(count)
            count +=1
            
            base = lower_CB_list[index]
            if index>0:
                pre_base = lower_CB_list[index-1]
                if seek_max==M_TO_UP: #向上
                    if(0<=self.__get_CB_pos(pre_base, base)): #当前中枢在前一中枢下或相交，当前趋势结束
                        #更新正式节点信息
                        time,value = self.__getMaxIndex_Val(lower_data, pre_base.start, pre_base.end)
                        if time==None:
                            time_seg = self.data.ix[self.data.index>pre_base.end, 'close']
                            time = time_seg.index[0]
                            value = self.data.ix[0, 'close']
                        self.node_list[-1].isformal = M_FORMAL
                        self.node_list[-1].datetime = time
                        self.node_list[-1].value = value
                        
                        #生成新临时节点
                        time,value = self.__getMinIndex_Val(lower_data, base.start, base.end)
                        if time==None:
                            time_seg = self.data.ix[self.data.index>base.end, 'close']
                            time = time_seg.index[0]
                            value = self.data.ix[0, 'close']                        
                        self.node_list.append(Node(time, value, M_TOP, low_id=index, isformal=M_TEMP))
                        seek_max = M_TO_DOWN                        
                        
                    else:#趋势延续
                        time,value = self.__getMaxIndex_Val(lower_data, base.start, base.end)
                        if time==None:
                            time_seg = self.data.ix[self.data.index>base.end, 'close']
                            time = time_seg.index[0]
                            value = self.data.ix[0, 'close']   
                        self.node_list[-1].datetime = time
                        self.node_list[-1].value = value                        
                else:
                    if(0>=self.__get_CB_pos(pre_base, base)): #当前中枢在前一中枢上或相交，当前趋势结束
                        #更新正式节点信息
                        time,value = self.__getMinIndex_Val(lower_data, pre_base.start, pre_base.end)
                        if time==None:
                            time_seg = self.data.ix[self.data.index>pre_base.end, 'close']
                            time = time_seg.index[0]
                            value = self.data.ix[0, 'close']
                        self.node_list[-1].isformal = M_FORMAL
                        self.node_list[-1].datetime = time
                        self.node_list[-1].value = value
                        
                        #生成新临时节点
                        time,value = self.__getMaxIndex_Val(lower_data, base.start, base.end)
                        if time==None:
                            time_seg = self.data.ix[self.data.index>base.end, 'close']
                            time = time_seg.index[0]
                            value = self.data.ix[0, 'close']                        
                        self.node_list.append(Node(time, value, M_TOP, low_id=index, isformal=M_TEMP))
                        seek_max = M_TO_UP                       
                        
                    else:#趋势延续
                        time,value = self.__getMinIndex_Val(lower_data, base.start, base.end)
                        if time==None:
                            time_seg = self.data.ix[self.data.index>base.end, 'close']
                            time = time_seg.index[0]
                            value = self.data.ix[0, 'close']   
                        self.node_list[-1].datetime = time
                        self.node_list[-1].value = value
                #进行中枢计算
                self.get_Centralbase_Step()                
            else:
                #生成新临时节点
                if seek_max==M_TO_UP:
                    time,value = self.__getMaxIndex_Val(lower_data, base.start, base.end)
                    if time==None:
                        time_seg = self.data.ix[self.data.index>base.end, 'close']
                        time = time_seg.index[0]
                        value = self.data.ix[0, 'close']
                else:
                    time,value = self.__getMinIndex_Val(lower_data, base.start, base.end)
                    if time==None:
                        time_seg = self.data.ix[self.data.index>base.end, 'close']
                        time = time_seg.index[0]
                        value = self.data.ix[0, 'close']                    
                self.node_list.append(Node(time, value, M_TOP, low_id=index, isformal=M_TEMP))
                
                      
    
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
                
                
    
    def getBeichi_Step(self):
        '''
        分步获取背驰节点
        返回当前中枢新加入节点是否为背驰点
        '''
        if (self.low_CB_set==None):
            cur_macd = 0
            pre_macd = 0
            base = self.centralbase_list[-1]
            if(base.ctype<=-2):
                if self.node_list[-2].value < base.min_val:#创新低
                    pre_macd = self.__getMACD_Sum(self.node_list[base.min_node_id-1].datetime, self.node_list[base.min_node_id].datetime, seekMax=False)
                    cur_macd = self.__getMACD_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=False)
            elif (base.ctype>=2):
                if self.node_list[-2].value > base.max_val:#创新高
                    pre_macd = self.__getMACD_Sum(self.node_list[base.max_node_id-1].datetime, self.node_list[base.max_node_id].datetime, seekMax=True)
                    cur_macd = self.__getMACD_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=True)
            else:
                return False
            if abs(cur_macd) < abs(pre_macd):
                self.beichi_list.append(BeichiTime(self.node_list[-2].datetime, base.ctype))
                return True
        else:
            cur_macd = 0
            pre_macd = 0
            base = self.centralbase_list[-1]
            if(base.ctype<=-2):
                if self.node_list[-2].value < base.min_val:#创新低
                    pre_macd = self.__getMACD_Sum(self.node_list[base.min_node_id-1].datetime, self.node_list[base.min_node_id].datetime, seekMax=False)
                    cur_macd = self.__getMACD_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=False)
            elif (base.ctype>=2):
                if self.node_list[-2].value > base.max_val:#创新高
                    pre_macd = self.__getMACD_Sum(self.node_list[base.max_node_id-1].datetime, self.node_list[base.max_node_id].datetime, seekMax=True)
                    cur_macd = self.__getMACD_Sum(self.node_list[-3].datetime, self.node_list[-2].datetime, seekMax=True)
            else:
                return False
            if abs(cur_macd) < abs(pre_macd):
                time_seg = self.data.ix[self.data.index>=self.node_list[-2].datetime, 'close']               
                graph_time =  time_seg.index[0]
                self.beichi_list.append(BeichiTime(self.node_list[-2].datetime, base.ctype, graph_time=graph_time))
                return True            
        return False

            
                
    def getBeiChi(self):
        if (self.low_CB_set!=None):
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
               
    
    def __getMACD_Sum(self, start_time, end_time, seekMax=True):
        data_seg = self.data.ix[(self.data.index>=start_time) & (self.data.index<=end_time), 'MACD']
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
        
    def __getMaxNode_Val(self, start_in, end_in):
        val = 0.0
        val_index = -1
        for index in range(start_in, end_in+1):
            if self.node_list[index].value > val:
                val = self.node_list[index].value
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
            if(np.size(self.centralbase_list)<=1):
                return 0            
            r_pos = self.__get_CB_pos(self.centralbase_list[-1], newcbase)
            pre_ctype = self.centralbase_list[-1].ctype
            if pre_ctype==0:#前一个是起点或背驰形成的第一中枢
                return (-2*r_pos)
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
                if abs(pre_ctype) >=2:
                    return (-2*r_pos)
                else:
                    return (-1*r_pos)
            elif((r_pos*pre_ctype) < 0):#延续
                return (pre_ctype-r_pos)
            else:
                return(-1*r_pos)         
        
                            
                        
                    
            