# encoding: UTF-8

import sys
import json
from pymongo import MongoClient
import pandas as pd

from vnpy.trader.app.ctaStrategy.ctaBase import DATABASE_NAMES
from CentralBase import centralbase

# 加载配置
config = open('config.json')
setting = json.load(config)

MONGO_HOST = setting['MONGO_HOST']
MONGO_PORT = setting['MONGO_PORT']


mc = MongoClient(MONGO_HOST, MONGO_PORT)        # Mongo连接



for database in DATABASE_NAMES:
    print('DATABASE [' + database + ']' + ' INFO:')
    db = mc[database]                         # 数据库
    print("COLLECTION:" )
    print(db.collection_names())
    for collection in db.collection_names():
        print('  '+collection+': %d'%(db[collection].find().count()))
        for u in db[collection].find(): 
            print(u)
            break
        df = pd.DataFrame(list(db[collection].find()))
        print(df)


