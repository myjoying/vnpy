# encoding: UTF-8

"""
展示如何执行策略回测。
"""

from __future__ import division


from vnpy.trader.app.ctaStrategy.ictaBacktesting import *


if __name__ == '__main__':
    from vnpy.trader.app.ctaStrategy.strategy.strategyCT import CtStrategy
    
    # 创建回测引擎
    engine = CtBacktestingEngine()
    
    # 设置引擎的回测模式为K线
    engine.setBacktestingMode(engine.BAR_MODE)

    # 设置回测用的数据起始日期
    engine.setStartDate('20160820')
    
    # 设置产品相关参数
    engine.setSlippage(0.0)     # 股指1跳
    engine.setRate(0.0)   # 万0.3
    engine.setSize(300)         # 股指合约大小 
    engine.setPriceTick(0.2)    # 股指最小价格变动
    
    # 设置使用的历史数据库'300467'
    engine.setDatabase(MINUTE_5_DB_NAME, '000001',isbase = True, dbkey='5MIN')
    engine.setDatabase(MINUTE_30_DB_NAME, '000001',dbkey='30MIN')
    engine.setDatabase(DAILY_DB_NAME, '000001',dbkey='D')
    
    
    # 在引擎中创建策略对象
    d = {'vtSymbol': '000001'}
    engine.initStrategy(CtStrategy, d)
    
    # 开始跑回测
    engine.runBacktesting()
    
    # 显示回测结果
    engine.showBacktestingResult()