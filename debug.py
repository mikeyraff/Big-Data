from kafka import KafkaConsumer
from report_pb2 import Report

broker = 'localhost:9092'
group_id = 'debug'

consumer = KafkaConsumer(
    'temperatures',
    group_id=group_id,
    bootstrap_servers=[broker],
    enable_auto_commit=True,
    auto_offset_reset='latest',  # Do not seek to the beginning
    value_deserializer=lambda x: Report.FromString(x), 
)

for message in consumer:
    key = message.key.decode('utf-8') if message.key else None
    value = message.value
    partition = message.partition

    try:
        report = value
        data_dict = {
            'partition': partition,
            'key': key,
            'date': report.date,
            'degrees': report.degrees,
        }
        print(data_dict)
    except Exception as e:
        print(f"Error decoding protobuf message: {e}")