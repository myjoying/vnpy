# encoding: UTF-8

'''
本文件中包含了CTA模块中用到的一些基础设置、类和常量等。
'''

# CTA引擎中涉及的数据类定义
from vnpy.trader.vtConstant import EMPTY_UNICODE, EMPTY_STRING, EMPTY_FLOAT, EMPTY_INT

# 常量定义
# CTA引擎中涉及到的交易方向类型
CTAORDER_BUY = u'买开'
CTAORDER_SELL = u'卖平'
CTAORDER_SHORT = u'卖开'
CTAORDER_COVER = u'买平'

# 本地停止单状态
STOPORDER_WAITING = u'等待中'
STOPORDER_CANCELLED = u'已撤销'
STOPORDER_TRIGGERED = u'已触发'

# 本地停止单前缀
STOPORDERPREFIX = 'CtaStopOrder.'

# 数据库名称
SETTING_DB_NAME = 'VnTrader_Setting_Db'
POSITION_DB_NAME = 'VnTrader_Position_Db'

TICK_DB_NAME = 'VnTrader_Tick_Db'
MINUTE_DB_NAME = 'VnTrader_1Min_Db'
MINUTE_5_DB_NAME = 'VnTrader_5Min_Db'
MINUTE_15_DB_NAME = 'VnTrader_15Min_Db'
MINUTE_30_DB_NAME = 'VnTrader_30Min_Db'
MINUTE_60_DB_NAME = 'VnTrader_60Min_Db'
DAILY_DB_NAME = 'VnTrader_Daily_Db'
WEEKLY_DB_NAME = 'VnTrader_Weekly_Db'

CHT_NODE_5_DB_NAME = 'VnTrader_ChtNode_5Min_Db'
CHT_NODE_30_DB_NAME = 'VnTrader_ChtNode_30Min_Db'
CHT_NODE_D_DB_NAME = 'VnTrader_ChtNode_Daily_Db'
CHT_CB_5_DB_NAME = 'VnTrader_ChtCb_5Min_Db'
CHT_CB_30_DB_NAME = 'VnTrader_ChtCb_30Min_Db'
CHT_CB_D_DB_NAME = 'VnTrader_ChtCb_Daily_Db'

DATABASE_NAMES = [TICK_DB_NAME, MINUTE_DB_NAME,
                  MINUTE_5_DB_NAME,MINUTE_15_DB_NAME,
                  MINUTE_30_DB_NAME, MINUTE_60_DB_NAME, 
                  DAILY_DB_NAME,WEEKLY_DB_NAME]

# 引擎类型，用于区分当前策略的运行环境
ENGINETYPE_BACKTESTING = 'backtesting'  # 回测
ENGINETYPE_TRADING = 'trading'          # 实盘

# CTA模块事件
EVENT_CTA_LOG = 'eCtaLog'               # CTA相关的日志事件
EVENT_CTA_STRATEGY = 'eCtaStrategy.'    # CTA策略状态变化事件


########################################################################
class StopOrder(object):
    """本地停止单"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.vtSymbol = EMPTY_STRING
        self.orderType = EMPTY_UNICODE
        self.direction = EMPTY_UNICODE
        self.offset = EMPTY_UNICODE
        self.price = EMPTY_FLOAT
        self.volume = EMPTY_INT
        
        self.strategy = None             # 下停止单的策略对象
        self.stopOrderID = EMPTY_STRING  # 停止单的本地编号 
        self.status = EMPTY_STRING       # 停止单状态