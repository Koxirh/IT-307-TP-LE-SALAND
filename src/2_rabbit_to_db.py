import pika
from pymongo import MongoClient
import json
import os
import time

MONGO_USERNAME = "test"
MONGO_PASSWORD = "password"
MONGO_HOST = "mongo"
MONGO_PORT = 27017
MONGO_DB = "tp-note"
MONGO_COLLECTION = "movies"
MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}"

client = MongoClient(MONGO_URI)

DB_NAME = "tp-note"
COLLECTION_NAME = "movies"

db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def safe_connect_rabbitmq():
    channel = None
    while not channel:
        try:
            connection = pika.BlockingConnection(pika.URLParameters("amqp://rabbitmq"))
            channel = connection.channel()
        except pika.exceptions.AMQPConnectionError:
            time.sleep(1)
    return channel

def callback(ch, method, properties, body):
    try:
        data_string = json.loads(body.decode("utf-8"))

        # print(f"Received data: {data_string}")
        unique_id = data_string['@Id']
        data_string["_id"] = unique_id

        if collection.find_one({"_id": unique_id}) is None:
            result = collection.insert_one(data_string)

        ch.basic_ack(delivery_tag=method.delivery_tag)

        # print(f"Uploaded message with id {unique_id}")
    except Exception as e:
        print(f"Error in callback: {e}")

def main():
    print("Starting rabbit_to_db.py")

    channel = safe_connect_rabbitmq()
    channel.queue_declare(queue='posts_to_mongo')

    channel.basic_consume(
        queue='posts_to_mongo',
        on_message_callback=callback
    )

    channel.start_consuming()
    print("Done")
    client.close()

if __name__ == "__main__":
    main()