import datetime
import pandas as pd
from influxdb import DataFrameClient

dbclient = DataFrameClient(host='127.0.0.1', port=8086, username='root', password='root', database='savina')

def onemin():
    upper = dbclient.query('select * from current order by time desc limit 1')['current'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
    if len(dbclient.query('select count(*) from onemin').keys()) == 0:
        dbclient.query('select first(value),min(value),mean(value),max(value),last(value) into onemin from current where time <= $upper group by *,time(1m)', bind_params={'upper':upper})
    else:
        lower = dbclient.query('select * from onemin order by time desc limit 1')['onemin'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
        dbclient.query('select first(value),min(value),mean(value),max(value),last(value) into onemin from current where time >= $lower and time <= $upper group by *,time(1m)', bind_params={'lower':lower, 'upper':upper})

def fifteenmins():
    upper = dbclient.query('select * from onemin order by time desc limit 1')['onemin'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
    if len(dbclient.query('select count(*) from fifteenmins').keys()) == 0:
        dbclient.query('select first(first),min(min),mean(mean),max(max),last(last) into fifteenmins from onemin where time <= $upper group by *,time(15m)', bind_params={'upper':upper})
    else:
        lower = dbclient.query('select * from fifteenmins order by time desc limit 1')['fifteenmins'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
        dbclient.query('select first(first),min(min),mean(mean),max(max),last(last) into fifteenmins from onemin where time >= $lower and time <= $upper group by *,time(15m)', bind_params={'lower':lower, 'upper':upper})

def onehour():
    upper = dbclient.query('select * from fifteenmins order by time desc limit 1')['fifteenmins'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
    if len(dbclient.query('select count(*) from onehour').keys()) == 0:
        dbclient.query('select first(first),min(min),mean(mean),max(max),last(last) into onehour from fifteenmins where time <= $upper group by *,time(1h)', bind_params={'upper':upper})
    else:
        lower = dbclient.query('select * from onehour order by time desc limit 1')['onehour'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
        dbclient.query('select first(first),min(min),mean(mean),max(max),last(last) into onehour from fifteenmins where time >= $lower and time <= $upper group by *,time(1h)', bind_params={'lower':lower, 'upper':upper})

def oneday():
    upper = dbclient.query('select * from onehour order by time desc limit 1')['onehour'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
    if len(dbclient.query('select count(*) from oneday').keys()) == 0:
        dbclient.query('select first(first),min(min),mean(mean),max(max),last(last) into oneday from onehour where time <= $upper group by *,time(1d)', bind_params={'upper':upper})
    else:
        lower = dbclient.query('select * from oneday order by time desc limit 1')['oneday'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
        dbclient.query('select first(first),min(min),mean(mean),max(max),last(last) into oneday from onehour where time >= $lower and time <= $upper group by *,time(1d)', bind_params={'lower':lower, 'upper':upper})

import time
while True:
    x = datetime.datetime.now()
    onemin()
    print(str(datetime.datetime.now() - x), 'done 1min')
    x = datetime.datetime.utcnow()
    fifteenmins()
    print(str(datetime.datetime.utcnow() - x), 'done 15mins')
    x = datetime.datetime.utcnow()
    onehour()
    print(str(datetime.datetime.utcnow() - x), 'done 1hour')
    x = datetime.datetime.utcnow()
    oneday()
    print(str(datetime.datetime.utcnow() - x), 'done 1day')
    time.sleep(5)