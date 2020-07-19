from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

import pandas as pd
import datetime
import time

# create influx client
token = "3qZ0imF1FlhHU4Y3PY-X32KswSKB8-odwwcvA-aOePGxLxxJsuagXZ3-Psdm_Uf1uT5EF3slmUBTeCw2Ji7ySw=="
org = "savina"
client = InfluxDBClient(url="http://localhost:9999", token=token, org=org)
query_api = client.query_api()

# create update function
def update(from_measurement, to_measurement, interval):
      # get the last update time and the present time
      present = str(datetime.datetime.now())[0:20].replace(" ", "T").replace(".", "Z")
      last_record = query_api.query_data_frame('from(bucket: "maindata")\
                                          |> range(start: 0, stop: present)\
                                          |> filter(fn: (r) => r._measurement == "to_measurement")\
                                          |> last()'.replace("to_measurement", to_measurement).replace("present", present))          
      if len(last_record) == 0:
            last_update = '0'
      else:
            last_update = str(last_record._time[0]).replace(" ", "T")
      # make the query
      if from_measurement == 'realtime':
            query = 'x = from(bucket: "maindata")\
                              |> range(start: last_update, stop: present)\
                              |> filter(fn: (r) => r._measurement == "from_measurement")\
                              |> group(columns: ["_measurment", "org", "group", "station", "device", "parameter", "_field"], mode: "by")\
                              |> window(every: interval)\
                        x     |> first()\
                              |> map(fn: (r) => ({r with _measurement: "to_measurement", _time: r._start, _field: "first"}))\
                              |> to(bucket: "maindata", org: "savina")\
                        x     |> min()\
                              |> map(fn: (r) => ({r with _measurement: "to_measurement", _time: r._start, _field: "min"}))\
                              |> to(bucket: "maindata", org: "savina")\
                        x     |> mean()\
                              |> map(fn: (r) => ({r with _measurement: "to_measurement", _time: r._start, _field: "mean"}))\
                              |> to(bucket: "maindata", org: "savina")\
                        x     |> max()\
                              |> map(fn: (r) => ({r with _measurement: "to_measurement", _time: r._start, _field: "max"}))\
                              |> to(bucket: "maindata", org: "savina")\
                        x     |> last()\
                              |> map(fn: (r) => ({r with _measurement: "to_measurement", _time: r._start, _field: "last"}))\
                              |> to(bucket: "maindata", org: "savina")'.replace("last_update", last_update,).replace("from_measurement", from_measurement).replace("to_measurement", to_measurement).replace("interval", interval).replace("present", present)
      else:
            query = 'x = from(bucket: "maindata")\
                              |> range(start: last_update, stop: present)\
                              |> filter(fn: (r) => r._measurement == "from_measurement")\
                              |> group(columns: ["_measurment", "org", "group", "station", "device", "parameter", "_field"], mode: "by")\
                              |> window(every: interval)\
                        x     |> filter(fn: (r) => r._field == "first")\
                              |> first()\
                              |> map(fn: (r) => ({r with _measurement: "to_measurement", _time: r._start, _field: "first"}))\
                              |> to(bucket: "maindata", org: "savina")\
                        x     |> filter(fn: (r) => r._field == "min")\
                              |> min()\
                              |> map(fn: (r) => ({r with _measurement: "to_measurement", _time: r._start, _field: "min"}))\
                              |> to(bucket: "maindata", org: "savina")\
                        x     |> filter(fn: (r) => r._field == "mean")\
                              |> mean()\
                              |> map(fn: (r) => ({r with _measurement: "to_measurement", _time: r._start, _field: "mean"}))\
                              |> to(bucket: "maindata", org: "savina")\
                        x     |> filter(fn: (r) => r._field == "max")\
                              |> max()\
                              |> map(fn: (r) => ({r with _measurement: "to_measurement", _time: r._start, _field: "max"}))\
                              |> to(bucket: "maindata", org: "savina")\
                        x     |> filter(fn: (r) => r._field == "last")\
                              |> last()\
                              |> map(fn: (r) => ({r with _measurement: "to_measurement", _time: r._start, _field: "last"}))\
                              |> to(bucket: "maindata", org: "savina")'.replace("last_update", last_update,).replace("from_measurement", from_measurement).replace("to_measurement", to_measurement).replace("interval", interval).replace("present", present)
      
      # excute the query
      query_api.query_data_frame(query)

# update
while True:
   x = datetime.datetime.now() 
   update(from_measurement='realtime', to_measurement='onemin', interval='1m')
   update(from_measurement='onemin', to_measurement='fifteenmins', interval='15m')
   update(from_measurement='fifteenmins', to_measurement='onehour', interval='1h')
   update(from_measurement='onehour', to_measurement='oneday', interval='1d')
   update(from_measurement='oneday', to_measurement='onemon', interval='1mo')
   update(from_measurement='onemon', to_measurement='oneyear', interval='1y')
   print('done after: {}s'.format(str(datetime.datetime.now()-x)))          
   time.sleep(5)