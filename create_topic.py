from kafka.admin import KafkaAdminClient, NewTopic

try:
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:29092')
    topic_list = [
        NewTopic(name="BTCUSDT", num_partitions=1, replication_factor=1),
        NewTopic(name="ETHUSDT", num_partitions=1, replication_factor=1)
    ]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("Topics created successfully")
except Exception as e:
    print(f"Error creating topics: {e}")
finally:
    admin_client.close()