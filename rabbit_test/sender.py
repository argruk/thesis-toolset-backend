# To be used by an endpoint to start a job for a receiver

# Implement:
#   * Start job {filename, period}
#   * Check if file downloaded {filename}


import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')
params = {"date": "today"}
channel.basic_publish(exchange='', routing_key='hello', body=params)
print(" [x] Sent 'Hello World!'")
connection.close()