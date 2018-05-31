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
    """
    def __init__(self, time, value):
        self.datetime = time
        self.value = value
        
class Centralbase:
    """ 
    中枢定义
    start：开始时间
    end：结束时间
    up：上边界
    down: 下边界
    """
    def __init__(self, start, end, up, down):
        self.start = start
        self.end = end 
        self.up = up
        self.down = down
        

class CentralBaseSet:
    def __init__(self, dataframe):
        self.data = dataframe.set_index(dataframe['datetime'])
        self.node_list = []
        self.centralbase_list = []
        self.getNodeList()
        self.get_Centralbase()
        self.resultToSource()
        
        
    def __getmax(self, a, b):
        return a if a>b else b
    
    def __getmin(self, a,b):
        return a if a<b else b
    
    def resultToSource(self):
        self.data['node'] = None
        self.data['base_up'] = None
        self.data['base_down'] = None
        for node in self.node_list:
            self.data.set_value(node.datetime, 'node', node.value)
        for base in self.centralbase_list:
            self.data.ix[base.start:base.end,'base_up'] = base.up
            self.data.ix[base.start:base.end,'base_down'] = base.down
        self.data['SMA5'] = ta.SMA(self.data['close'].values, timeperiod = 5)  #5日均线
        self.data['SMA10'] = ta.SMA(self.data['close'].values, timeperiod = 10)  #10日均线
        macd_talib, signal, hist = ta.MACD(self.data['close'].values,fastperiod=12,signalperiod=9)
        self.data['DIF'] = macd_talib #DIF
        self.data['DEA'] = signal #DEA
        self.data['MACD'] = hist #MACD
        
    
        
    def getNodeList(self):
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
            for index in self.data.index:
                if seek_max:
                    if self.data.get_value(index, 'up'):
                        if self.data.get_value(index, 'close') >=value:
                            time = index
                            value = self.data.get_value(index, 'close')
                    else:
                        if self.data.get_value(index, 'close') < value:
                            self.node_list.append(Node(time, value))
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
                            self.node_list.append(Node(time, value))
                            time = index
                            value = self.data.get_value(index, 'close')
                            seek_max = True
                        if abs(self.data.get_value(index, 'close') - self.data.get_value(index, 'open')) <=0.001: #排查十字星的情况
                            if self.data.get_value(index, 'close') <= value:
                                time = index
                                value = self.data.get_value(index, 'close')                            
                            
        except Exception as e:
            print("getNodeList EEROR.", e)
            
            
    def get_Centralbase(self):
        try:
            size = np.size(self.node_list)
            if size < 5:
                return
            
            seg_list=[]
            start = None
            end = None
            cross_itval = Interval.none()
            pre_cross_itval = Interval.none()
            
            i=1
            while(i<size-1):
                if np.size(seg_list) < 3:
                    while((i<size-1) and (np.size(seg_list) < 3)):
                        seg = self.__getSegment(i)
                        if (pre_cross_itval != Interval.none()) and (seg.overlaps(pre_cross_itval)): 
                            seg_list=[]
                            cross_itval = Interval.none()
                            start = None
                            end = None                            
                        else:
                            seg_list.append(seg)
                            if np.size(seg_list) ==1:
                                start = self.__getSegmentStart(i)
                            end = self.__getSegmentEnd(i)
                        i += 1
                        
                    if i<size-1:
                        cross_itval = seg_list[0] & seg_list[1] & seg_list[2]
                        if cross_itval == Interval.none():
                            seg_list.pop(0)
                            
                else:        
                    seg = self.__getSegment(i)
                    if (pre_cross_itval != Interval.none()) and (seg.overlaps(pre_cross_itval)):
                        seg_list=[]
                        cross_itval = Interval.none()
                        start = None
                        end = None
                        i += 1
                    else:
                        if seg.overlaps(cross_itval):
                            cross_itval = cross_itval & seg
                            end = self.__getSegmentEnd(i)
                            i += 1
                        else:
                            self.centralbase_list.append(Centralbase(start, end, cross_itval.upper_bound, cross_itval.lower_bound ))
                            seg_list=[]
                            pre_cross_itval = cross_itval
                            cross_itval = Interval.none()
                            start = None
                            end = None     
                
        except Exception as e:
            print("get_Centralbase EEROR.", e)
            
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
                            
                        
                    
            