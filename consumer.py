import json
import os
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
import sys
from report_pb2 import Report

def read_data(part_num):
    if os.path.exists(f"partition-{part_num}.json"):
        with open(f"partition-{part_num}.json", "r") as f:
            return json.load(f)
    else:
        return {"partition": part_num, "offset": 0}
   
def write_data(part_num, data):
    path = f"files/partition-{part_num}.json"
    path2 = path + ".tmp"
    with open(path2, "w") as f:
        json.dump(data, f)
        os.rename(path2, path)
   
def consumer_func(consumer, part_data, messages):
    producer = KafkaProducer(bootstrap_servers=[broker], acks = "all", retries = 10)
    try:
        for mess in messages:
            partition = mess.partition
            month = mess.key.decode('utf-8')
            temperature = Report.FromString(mess.value).degrees
            date = Report.FromString(mess.value).date
            year = date[:4]
           
            if month not in part_data[partition]:
                part_data[partition][month] = {}
               
            if year not in part_data[partition][month]:
                part_data[partition][month][year] = {
                    "count": 0,
                    "sum": 0,
                    "avg": 0,
                    "start": date,
                    "end": ""
                }
           
            if date <= part_data[partition][month][year]["end"]:
                continue
               
            part_data[partition][month][year]["count"] += 1
            part_data[partition][month][year]["sum"] += temperature
            part_data[partition][month][year]["avg"] = (part_data[partition][month][year]["sum"] / part_data[partition][month][year]["count"])
            part_data[partition][month][year]["end"] = date
            print(part_data)
       
        for partition, data in part_data.items():
            last_message_offset = messages[-1].offset
            part_data[partition]['offset'] = last_message_offset + 1
            write_data(partition, data)
           
    except Exception as e:
        print(e)
    finally:
        producer.close()
   
if __name__ == "__main__":
    broker = 'localhost:9092'
    consumer = KafkaConsumer(bootstrap_servers=[broker])
    parts = [int(partition) for partition in sys.argv[1:]]
    consumer.assign([TopicPartition("temperatures", p) for p in parts])
    data = {p: read_data(p) for p in parts}
    try:
        while True:
            batch = consumer.poll(1000)
            for topic_partition, messages in batch.items():
                consumer_func(consumer, data, messages)
    finally:
        consumer.close()