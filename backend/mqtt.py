import paho.mqtt.client as mqtt
import json
import datetime
import pandas as pd
from influxdb import DataFrameClient

dbclient = DataFrameClient(host='127.0.0.1', port=8086, username='root', password='root', database='savina')
dbclient.create_database('savina')

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
    dbclient.write_points(data, 'current', field_columns=['value'])

client = mqtt.Client()
client.username_pw_set(username="mind", password="123")
client.on_connect = on_connect
client.on_message = on_message

client.connect("nangluong.iotmind.vn", 16766, 60)
client.loop_forever()