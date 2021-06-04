import sys
import socket
import threading
import time
import os
import random
import pika
from argparse import ArgumentParser

HOST = '127.0.0.1'
FORMAT = 'utf-8'
TOPICS_DIR = 'topics'

class TokenRingMember():
    def __init__(self, name, left_socket_port, right_socket_port, is_first=False):
        self.name = name
        self.has_token = False
        self.id = random.randint(0, 10000)

        self.left_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.right_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if is_first:
            self.right_socket.connect((HOST, right_socket_port))
            self.__send('init')

            self.left_socket.bind((HOST, left_socket_port))
            self.left_socket.listen()
            self.left_conn, _ = self.left_socket.accept()

            msg = self.__receive(32)
            if str(msg) == 'init':
                self.__send(f'id:{self.id}')

                msg = self.__receive(32)
                largest_id = int(msg[3:])
                if largest_id == self.id:
                    self.has_token = True
                
                self.__send(f'elected:{largest_id}')

                _ = self.__receive(32)
        else:
            self.left_socket.bind((HOST, left_socket_port))
            self.left_socket.listen()
            self.left_conn, _ = self.left_socket.accept()

            msg = self.__receive(32)
            if str(msg) == 'init':
                self.right_socket.connect((HOST, right_socket_port))
                self.__send('init')

                msg = self.__receive(32)
                id = int(msg[3:])
                id = max(id, self.id)
                self.__send(f'id:{id}')
                msg = self.__receive(32)
                elected = int(msg[8:])
                if elected == self.id:
                    self.has_token = True
                self.__send(msg)

    def __send(self, message):
        self.right_socket.send(message.encode(FORMAT))

    def __receive(self, size):
        msg = self.left_conn.recv(size)
        return msg.decode(FORMAT)

    def token_passer(self):
        while True:
            if self.has_token:
                time.sleep(10)
                self.__send('token')
                self.has_token = False
            else:
                msg = self.__receive(32)
                if msg == 'token':
                    print('You can send messages now')
                    self.has_token = True

    def publisher(self, topics):
        params = pika.URLParameters('amqps://yvfsdwqi:MP_6vFWo3yK8CUGH_sVACnFx6cyQlW0N@cow.rmq2.cloudamqp.com/yvfsdwqi')
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(exchange='topics', exchange_type='direct')
        
        while True:
            if self.has_token:
                msg = input()
                if not self.has_token:
                    print("You can't send messages right now")
                else:
                    msg = msg.split(':')
                    if len(msg) != 2:
                        print('Please use the format <<TOPIC>>: <<MESSAGE>>')
                        continue
                    topic, message = msg[0], msg[1]
                    if topic not in topics:
                        print("You are not a publisher for that topic")
                        continue
                    channel.basic_publish(exchange='topics', routing_key=topic, body=message)
    
    def callback(self, ch, method, properties, body):
      print(" [x] Received " + str(body))

    def subscriber(self, topics):
        params = pika.URLParameters('amqps://yvfsdwqi:MP_6vFWo3yK8CUGH_sVACnFx6cyQlW0N@cow.rmq2.cloudamqp.com/yvfsdwqi')
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(exchange='topics', exchange_type='direct')
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        for topic in topics:
            channel.queue_bind(exchange='topics', queue=queue_name, routing_key=topic)

        channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True) 
        channel.start_consuming()
        connection.close()

    def start(self, publish_topics, subscribe_topics):
        thread_publisher = threading.Thread(target=self.publisher, args=(publish_topics,))
        thread_subscriber = threading.Thread(target=self.subscriber, args=(subscribe_topics,))
        thread_passer = threading.Thread(target=self.token_passer)

        thread_passer.start()
        thread_publisher.start()
        thread_subscriber.start()


def main(args):
    parser = ArgumentParser()
    parser.add_argument('-n', type=str, help='Name of the person')
    parser.add_argument('-l', type=int, help='Left socket port')
    parser.add_argument('-r', type=int, help='Right socket port')
    parser.add_argument('--subscribe', nargs='+', help='Subscribe topics')
    parser.add_argument('--publish', nargs='+', help='Publish topics')
    parser.add_argument('--is_first', action='store_true', help='If the person initially has the token')

    args = parser.parse_args(args)
    person = TokenRingMember(args.n, args.l, args.r, args.is_first)
    print(f'You are subscribed to the following topics: {" ".join(args.subscribe)}')
    print(f'You can publish messages to the following topics: {" ".join(args.publish)}')
    person.start(args.publish, args.subscribe)


if __name__ == '__main__':
    main(sys.argv[1:])