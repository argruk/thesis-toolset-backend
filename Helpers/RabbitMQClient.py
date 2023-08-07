import pika
import json

class RabbitMQClient:
    def __init__(self, routing_key):
        self.host = 'localhost'
        self.routing_key = routing_key

    def send_message(self, params):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.queue_declare(queue=self.routing_key)
        channel.basic_publish(exchange='', routing_key=self.routing_key, body=params)
        connection.close()
        print(f"Sent job to {self.routing_key}")

    @staticmethod
    def prepare_new_dataset_obj(filename, date_from, date_to):
        return json.dumps({"filename":filename, "dateFrom": date_from, "dateTo":date_to})

    @staticmethod
    def prepare_new_dataset_obj_with_mt(filename, date_from, date_to, mt):
        return json.dumps({"filename": filename, "dateFrom": date_from, "dateTo": date_to, "mt": mt})
