import time
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import report_pb2
import weather
import calendar

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3)  # Deletion sometimes takes a while to reflect

# TODO: Create topic 'temperatures' with 4 partitions and replication factor = 1
topic = "temperatures"
num_partitions = 4
replication_factor = 1
admin_client.create_topics([NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)])

print("Topics:", admin_client.list_topics())

producer = KafkaProducer(
    bootstrap_servers=broker,
    retries=10,
    acks="all"
)

for date, degrees in weather.get_next_weather(delay_sec=0.1):

    report = report_pb2.Report(date=date, degrees=degrees)

    month_key = calendar.month_name[int(date.split('-')[1])]

    producer.send(topic, key=month_key.encode('utf-8'), value=report.SerializeToString())
