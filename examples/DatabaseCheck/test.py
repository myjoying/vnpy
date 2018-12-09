# encoding: UTF-8

import sys
import json
from pymongo import MongoClient
import pandas as pd
import datetime as dt



time = dt.datetime(2018, 5, 10, 10, 40)
time = time.replace(minute=(30 if time.minute>=30 else 0)) + dt.timedelta(minutes=30)

time = time.replace(hour=0, minute=0)

def roundTimeForward(time, freq):
    if freq == '30MIN':
        return time.replace(minute=(30 if time.minute>=30 else 0))
    elif freq == 'D':
        return time.replace(hour=0, minute=0)
    else:
        return time
        
def roundTime(time, freq):
    if freq == '30MIN':
        return time.replace(minute=(30 if time.minute>=30 else 0)) + dt.timedelta(minutes=30)
    elif freq == 'D':
        return time.replace(hour=0, minute=0)
    else:
        return time







