import sys
import socket
import threading
import time
import os
from argparse import ArgumentParser

HOST = '127.0.0.1'
FORMAT = 'utf-8'
TOPICS_DIR = 'topics'

class TokenRingMember():
    def __init__(self, name, left_socket_port, right_socket_port, has_token=False):
        self.name = name
        self.has_token = has_token

        self.left_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.right_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if has_token:
            self.right_socket.connect((HOST, right_socket_port))
            self.__send('token')

            self.left_socket.bind((HOST, left_socket_port))
            self.left_socket.listen()
            self.left_conn, _ = self.left_socket.accept()

            msg = self.__receive(self.left_conn, 32)
            if str(msg) == 'token':
                print('Finish init')
        else:
            self.left_socket.bind((HOST, left_socket_port))
            self.left_socket.listen()
            self.left_conn, _ = self.left_socket.accept()

            msg = self.__receive(self.left_conn, 32)
            if str(msg) == 'token':
                self.right_socket.connect((HOST, right_socket_port))
                self.__send('token')

    def __send(self, message):
        self.right_socket.send(message.encode(FORMAT))

    def __receive(self, conn, size):
        msg = conn.recv(size)
        return msg.decode(FORMAT)

    def token_passer(self):
        while True:
            if self.has_token:
                time.sleep(10)
                self.__send('token')
                self.has_token = False
            else:
                msg = self.__receive(self.left_conn, 32)
                if msg == 'token':
                    print('You can send messages now')
                    self.has_token = True

    def publisher(self, topics):
        topic_files = {}
        for topic in topics:
            topic_files[topic] = os.path.join(TOPICS_DIR, topic + '.log')
        
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
                    if topic not in topic_files:
                        print("You are not a publisher for that topic")
                        continue
                    file = open(topic_files[topic], 'a')
                    file.write(f'{self.name}:{message}\n')
                    file.close()

    def subscriber(self, topics):
        topic_files = []
        for topic in topics:
            topic_files.append(open(os.path.join(TOPICS_DIR, topic + '.log')))

        while True:
            for i, file in enumerate(topic_files):
                where = file.tell()
                line = file.readline()
                if not line:
                    time.sleep(0.1)
                    file.seek(where)
                else:
                    if not self.has_token:
                        print(f"[{topics[i]}] {line.strip()}")

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
    parser.add_argument('--has_token', action='store_true', help='If the person initially has the token')

    args = parser.parse_args(args)
    person = TokenRingMember(args.n, args.l, args.r, args.has_token)
    print(f'You are subscribed to the following topics: {" ".join(args.subscribe)}')
    print(f'You can publish messages to the following topics: {" ".join(args.publish)}')
    person.start(args.publish, args.subscribe)


if __name__ == '__main__':
    main(sys.argv[1:])