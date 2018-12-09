# encoding: UTF-8

'''
tushare的gateway接入


'''
import os
import json
from copy import copy
from datetime import datetime, timedelta

from vnpy.trader.vtGateway import *
from vnpy.trader.vtFunction import getJsonPath, getTempPath
from vnpy.trader.vtConstant import GATEWAYTYPE_FUTURES
import tushare as ts
import pandas as pd
from pymongo import MongoClient, ASCENDING

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.app.ctaStrategy.ctaBase import *

#from .language import text


# 以下为一些VT类型和CTP类型的映射字典
# 价格类型映射
#priceTypeMap = {}
#priceTypeMap[PRICETYPE_LIMITPRICE] = defineDict["THOST_FTDC_OPT_LimitPrice"]
#priceTypeMap[PRICETYPE_MARKETPRICE] = defineDict["THOST_FTDC_OPT_AnyPrice"]
#priceTypeMapReverse = {v: k for k, v in priceTypeMap.items()} 

## 方向类型映射
#directionMap = {}
#directionMap[DIRECTION_LONG] = defineDict['THOST_FTDC_D_Buy']
#directionMap[DIRECTION_SHORT] = defineDict['THOST_FTDC_D_Sell']
#directionMapReverse = {v: k for k, v in directionMap.items()}

## 开平类型映射
#offsetMap = {}
#offsetMap[OFFSET_OPEN] = defineDict['THOST_FTDC_OF_Open']
#offsetMap[OFFSET_CLOSE] = defineDict['THOST_FTDC_OF_Close']
#offsetMap[OFFSET_CLOSETODAY] = defineDict['THOST_FTDC_OF_CloseToday']
#offsetMap[OFFSET_CLOSEYESTERDAY] = defineDict['THOST_FTDC_OF_CloseYesterday']
#offsetMapReverse = {v:k for k,v in offsetMap.items()}

## 交易所类型映射
#exchangeMap = {}
#exchangeMap[EXCHANGE_CFFEX] = 'CFFEX'
#exchangeMap[EXCHANGE_SHFE] = 'SHFE'
#exchangeMap[EXCHANGE_CZCE] = 'CZCE'
#exchangeMap[EXCHANGE_DCE] = 'DCE'
#exchangeMap[EXCHANGE_SSE] = 'SSE'
#exchangeMap[EXCHANGE_SZSE] = 'SZSE'
#exchangeMap[EXCHANGE_INE] = 'INE'
#exchangeMap[EXCHANGE_UNKNOWN] = ''
#exchangeMapReverse = {v:k for k,v in exchangeMap.items()}

## 持仓类型映射
#posiDirectionMap = {}
#posiDirectionMap[DIRECTION_NET] = defineDict["THOST_FTDC_PD_Net"]
#posiDirectionMap[DIRECTION_LONG] = defineDict["THOST_FTDC_PD_Long"]
#posiDirectionMap[DIRECTION_SHORT] = defineDict["THOST_FTDC_PD_Short"]
#posiDirectionMapReverse = {v:k for k,v in posiDirectionMap.items()}

## 产品类型映射
#productClassMap = {}
#productClassMap[PRODUCT_FUTURES] = defineDict["THOST_FTDC_PC_Futures"]
#productClassMap[PRODUCT_OPTION] = defineDict["THOST_FTDC_PC_Options"]
#productClassMap[PRODUCT_COMBINATION] = defineDict["THOST_FTDC_PC_Combination"]
#productClassMapReverse = {v:k for k,v in productClassMap.items()}
#productClassMapReverse[defineDict["THOST_FTDC_PC_ETFOption"]] = PRODUCT_OPTION
#productClassMapReverse[defineDict["THOST_FTDC_PC_Stock"]] = PRODUCT_EQUITY

## 委托状态映射
#statusMap = {}
#statusMap[STATUS_ALLTRADED] = defineDict["THOST_FTDC_OST_AllTraded"]
#statusMap[STATUS_PARTTRADED] = defineDict["THOST_FTDC_OST_PartTradedQueueing"]
#statusMap[STATUS_NOTTRADED] = defineDict["THOST_FTDC_OST_NoTradeQueueing"]
#statusMap[STATUS_CANCELLED] = defineDict["THOST_FTDC_OST_Canceled"]
#statusMapReverse = {v:k for k,v in statusMap.items()}

## 全局字典, key:symbol, value:exchange
#symbolExchangeDict = {}

#tick数据与tushare返回数据映射
tick_tushare_map = {
    # 代码相关
    "symbol" :  "code" ,            # 合约代码
    #"exchange" : EMPTY_STRING            # 交易所代码
    "vtSymbol" : "code" ,           # 合约在vt系统中的唯一代码，通常是 合约代码.交易所代码
    
    # 成交数据
    "lastPrice" : "price",            # 最新成交价
    "lastVolume" : "volume" ,            # 最新成交量
    #"volume" : EMPTY_INT                 # 今天总成交量
    #openInterest = EMPTY_INT           # 持仓量
    "time" : "time" ,               # 时间 11:20:56.5
    "date" : "date" ,               # 日期 20151009
    #self.datetime = None                    # python的datetime时间对象
    
    # 常规行情
    "openPrice" : "open" ,           # 今日开盘价
    "highPrice" : "high" ,           # 今日最高价
    "lowPrice" : "low" ,            # 今日最低价
    "preClosePrice" : "pre_close",
    
    #self.upperLimit = EMPTY_FLOAT           # 涨停价
    #self.lowerLimit = EMPTY_FLOAT           # 跌停价
    
    # 五档行情
    "bidPrice1" : "b1_p" ,
    "bidPrice2" : "b2_p" ,
    "bidPrice3" : "b3_p" ,
    "bidPrice4" : "b4_p" ,
    "bidPrice5" : "b5_p" ,
    
    "askPrice1" : "a1_p" ,
    "askPrice2" : "a2_p" ,
    "askPrice3" : "a3_p" ,
    "askPrice4" : "a4_p" ,
    "askPrice5" : "a5_p" ,        
    
    "bidVolume1" : "b1_v", 
    "bidVolume2" : "b2_v", 
    "bidVolume3" : "b3_v", 
    "bidVolume4" : "b4_v", 
    "bidVolume5" : "b5_v", 
    
    "askVolume1" : "a1_v" ,
    "askVolume2" : "a2_v" ,
    "askVolume3" : "a3_v" ,
    "askVolume4" : "a4_v" ,
    "askVolume5" : "a5_v" , 

}

# 夜盘交易时间段分隔判断
NIGHT_TRADING = datetime(1900, 1, 1, 20).time()


########################################################################
class TushareGateway(VtGateway):
    """CTP接口"""

    #----------------------------------------------------------------------
    def __init__(self, eventEngine, gatewayName='Tushare'):
        """Constructor"""
        super(TushareGateway, self).__init__(eventEngine, gatewayName)
        
        
        self.mdConnected = False        # 行情API连接状态，登录完成后为True
        self.tdConnected = False        # 交易API连接状态
        
        self.qryEnabled = False         # 循环查询
        
        self.subSymbolList = []
        
        self.fileName = self.gatewayName + '_connect.json'
        self.filePath = getJsonPath(self.fileName, __file__)    
        
        
        
    #----------------------------------------------------------------------
    def connect(self):
        """连接"""

        # 初始化并启动查询
        self.initQuery()
    
    #----------------------------------------------------------------------
    def subscribe(self, subscribeReq):
        """订阅行情"""
        self.subSymbolList.append(subscribeReq)
        
        
    #----------------------------------------------------------------------
    def sendOrder(self, orderReq):
        """发单"""
        pass
        
    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """撤单"""
        pass
        
    #----------------------------------------------------------------------
    def qryAccount(self):
        """查询账户资金"""
        pass
        
    #----------------------------------------------------------------------
    def qryPosition(self):
        """查询持仓"""
        pass
        
    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        pass
    
    #----------------------------------------------------------------------
    def qryRealtimeData(self):
        """获取实时数据"""
        for req in self.subSymbolList:
            df = ts.get_realtime_quotes(req.symbol)
            
            if not df.empty:
                tick = VtTickData()
                
                for key in tick_tushare_map.keys():
                    tick.__dict__[key] = df.ix[0, tick_tushare_map[key]]
                
                tick.datetime = datetime.strptime(df.ix[0, 'date']+ ' ' + df.ix[0, 'time'], "%Y-%m-%d %H:%M:%S")
                
                self.onTick(tick)
        
    #----------------------------------------------------------------------
    def initQuery(self):
        """初始化连续查询"""
        if self.qryEnabled:
            # 需要循环的查询函数列表
            self.qryFunctionList = [self.qryRealtimeData]
            
            self.qryCount = 0           # 查询触发倒计时
            self.qryTrigger = 2         # 查询触发点
            self.qryNextFunction = 0    # 上次运行的查询函数索引
            
            self.startQuery()
    
    #----------------------------------------------------------------------
    def query(self, event):
        """注册到事件处理引擎上的查询函数"""
        self.qryCount += 1
        
        if self.qryCount > self.qryTrigger:
            # 清空倒计时
            self.qryCount = 0
            
            # 执行查询函数
            function = self.qryFunctionList[self.qryNextFunction]
            function()
            
            # 计算下次查询函数的索引，如果超过了列表长度，则重新设为0
            self.qryNextFunction += 1
            if self.qryNextFunction == len(self.qryFunctionList):
                self.qryNextFunction = 0
    
    #----------------------------------------------------------------------
    def startQuery(self):
        """启动连续查询"""
        self.eventEngine.register(EVENT_TIMER, self.query)
    
    #----------------------------------------------------------------------
    def setQryEnabled(self, qryEnabled):
        """设置是否要启动循环查询"""
        self.qryEnabled = qryEnabled
    




    