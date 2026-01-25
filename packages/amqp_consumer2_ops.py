#!/usr/bin/python
# -*- coding: UTF-8 -*-

# @Author: dibo
# @Time:
# @FileName:
# @Description:
# @Version: 1.0.0

import pika

# Assuming you have already established a connection and created a channel
connection = pika.BlockingConnection(pika.ConnectionParameters(host='xxxx.xxx.xxxx.79', heartbeat=360))
channel = connection.channel()

# Declare a queue
channel.queue_declare(queue='task_queue')

# Set Quality of Service (QoS) to limit the number of messages the consumer prefetches
channel.basic_qos(prefetch_count=1)  # Limit to one message at a time


# Define a callback function to handle messages
def callback(ch, method, properties, body):
    # Process the message
    print(f"Received {body}")

    # Simulate app work (replace with actual work)
    import time
    time.sleep(body.count(b'.') + 1)
    print("app work code here")
    print("Done")

    # Acknowledge the message manually
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Start consuming messages from the queue
channel.basic_consume(queue='task_queue', on_message_callback=callback, auto_ack=False)

print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()