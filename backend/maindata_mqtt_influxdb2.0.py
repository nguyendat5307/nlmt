from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

import paho.mqtt.client as mqtt
import json
import datetime
import pandas as pd

def on_connect(client, userdata, flags, rc):
    client.subscribe('savina/#')

def on_message(client, userdata, msg):
    topic = msg.topic
    content = msg.payload
    objpayload = json.loads(content)
    data = objpayload["d"]
    data = pd.DataFrame(data)
    data[['device', 'parameter']] = data.tag.str.split(':', expand=True)
    data['time'] = datetime.datetime.now()
    data.set_index('time', inplace=True)
    data['topic'] = topic
    data[['admin', 'org', 'group', 'station', 'gateway']] = data.topic.str.split('/', expand=True)
    data = data.drop(['topic','tag', 'admin', 'gateway'], axis=1)
    data = data.rename(columns={'value':'realtime'})
    write_api.write(bucket, record=data, data_frame_measurement_name='realtime', data_frame_tag_columns=['org', 'group', 'station', 'device', 'parameter'])

# create indluxdb-client
token = "3qZ0imF1FlhHU4Y3PY-X32KswSKB8-odwwcvA-aOePGxLxxJsuagXZ3-Psdm_Uf1uT5EF3slmUBTeCw2Ji7ySw=="
org = "savina"
bucket = "maindata"
client = InfluxDBClient(url="http://localhost:9999", token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

# create mqtt-client
client = mqtt.Client()
client.username_pw_set(username="mind", password="123")
client.on_connect = on_connect
client.on_message = on_message

# connect mqtt and loop forever
client.connect("nangluong.iotmind.vn", 16766, 60)
client.loop_forever()