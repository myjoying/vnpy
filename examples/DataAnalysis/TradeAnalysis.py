import sys
import json
import time as tm
import pandas as pd
import numpy as np
import datetime as dt
from vnpy.trader.app.ctaStrategy.ctaBase import *
from CentralBase import *
  
def trade_analysis_2buy_1sell(sec_buy_list, beichi_list, lowest_data):
    for sec_buy in sec_buy_list:
        if sec_buy.real_buy==M_TRUE:
            buy_value = lowest_data.ix[sec_buy.real_time, 'close']         
            print("BUY: ", sec_buy.real_time, buy_value)
            for beichi in beichi_list:
                if beichi.btype>0 and beichi.real_time>sec_buy.real_time:
                    sell_value = lowest_data.ix[beichi.real_time, 'close']                 
                    print("SELL: ", beichi.real_time, sell_value, 100*(sell_value-buy_value)/buy_value)
                    break
            
                
def trade_analysis_2buy_allsell(sec_buy_list, beichi_list, sec_sell_list, lowest_data):
    for sec_buy in sec_buy_list:
        if sec_buy.real_buy==M_TRUE:
            buy_value = lowest_data.ix[sec_buy.real_time, 'close']         
            print("BUY: ", sec_buy.real_time, buy_value)
            sell_time = None
            for beichi in beichi_list:
                if beichi.btype>0 and beichi.real_time>sec_buy.real_time:
                    if sell_time!=None:
                        if beichi.real_time<=sell_time:
                            sell_time = beichi.real_time 
                    else:
                        sell_time = beichi.real_time
                    break
            for sec_sell in sec_sell_list:
                if beichi.btype>0 and sec_sell.real_time>sec_buy.real_time:
                    if sell_time!=None:
                        if sec_sell.real_time<=sell_time:
                            sell_time = sec_sell.real_time 
                    else:
                        sell_time = sec_sell.real_time
                    break
                
            if sell_time!=None:
                sell_value = lowest_data.ix[sell_time, 'close']                 
                print("SELL: ", sell_time, sell_value, 100*(sell_value-buy_value)/buy_value)
              
                
def trade_strategy_analysis(trade_strategy_list, lowest_data, filename="file"):
    f = open(filename, 'w')
    for trade_strategy in trade_strategy_list:
        for trade in trade_strategy.trade_point_list:
            if trade.trade_direct == M_BUY:
                buy_value = lowest_data.ix[trade.real_time, 'close'] 
                f_str = "BUY: " + str(trade.real_time) + " %f \n"%(buy_value)
                f.write(f_str) 
            else:
                sell_value = lowest_data.ix[trade.real_time, 'close'] 
                f_str = "SELL: " + str(trade.real_time) + " %f  %f\n"%(sell_value,100*(sell_value-buy_value)/buy_value)
                f.write(f_str)                
    f.close()

            
            
def trade_analysis_buy_oneseg_sell(buy_list, all_sell_list, lowest_data):
    for buy in buy_list:
        buy_value = lowest_data.ix[buy.real_time, 'close']         
        print("BUY: ", buy.real_time, buy_value)
        sell_time = None
        for sell in all_sell_list:
            if sell.real_time>buy.real_time:
                if sell_time!=None:
                    if sell.real_time<=sell_time:
                        sell_time = sell.real_time 
                else:
                    sell_time = sell.real_time
                break
            
        if sell_time!=None:
            sell_value = lowest_data.ix[sell_time, 'close']                 
            print("SELL: ", sell_time, sell_value, 100*(sell_value-buy_value)/buy_value)