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
    dbclient.write_points(data, 'current', field_columns=['value'])

# You can generate a Token from the "Tokens Tab" in the UI
token = "tLjGK3kL71jysgnQ41FBVmE3k7tycSsp2me2qam9W7AQCLnJVx0b9YP65fA2DNEdNu76II6tG-s1hlbbN1NfTA=="
org = "mind"
bucket = "nlmt"

client = InfluxDBClient(url="http://localhost:9999", token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

data = "mem,host=host1 used_percent=21.43234543"
write_api.write(bucket, org, data)

query = 'from(bucket: \"{}\") |> range(start: -1h)'.format("nlmt")
queryinto = 'from(bucket: "nlmt") |> range(start: -10h) |> window(every: 1h) |> '
data_frame = query_api.query_data_frame(query)

print(data_frame)





client = mqtt.Client()
client.username_pw_set(username="mind", password="123")
client.on_connect = on_connect
client.on_message = on_message

client.connect("nangluong.iotmind.vn", 16766, 60)
client.loop_forever()

