from kafka.admin import KafkaAdminClient

admin_client = KafkaAdminClient(bootstrap_servers='st124vm101:9092')

# List all topics
topics = admin_client.list_topics()

# Delete all topics
admin_client.delete_topics(topics)
print(f"Deleted topics: {topics}")