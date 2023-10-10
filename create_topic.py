from kafka.admin import KafkaAdminClient, NewTopic

bootstrap_servers = 'localhost:29092'
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

topic_name = 'test'
num_partitions = 8
replication_factor = 1
new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

admin_client.create_topics(new_topics=[new_topic])

admin_client.close()
