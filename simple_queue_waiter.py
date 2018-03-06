from collections import MutableSequence
from multiprocessing.pool import ThreadPool
import sys
import time
import yaml

import pika


class SimpleChecker(object):
    def __init__(self, host, port, virtual_host, queue, user, password):
        self.host = host
        self.port = port
        self.virtual_host = virtual_host
        self.queue = queue
        self.user = user
        self.password = password


class Checker(MutableSequence, object):
    def __init__(self, period=10, threads=2):
        self.period = period
        self.threads = threads
        self.simples = []

    # Collection methods start

    def __getitem__(self, index):
        return self.simples[index]

    def __setitem__(self, index, value):
        self.simples[index] = value

    def __delitem__(self, index):
        del self.simples[index]

    def __iter__(self):
        return iter(self.simples)

    def __len__(self):
        return len(self.simples)

    # Collection methods end

    def add(self, sc):
        if isinstance(sc, SimpleChecker):
            self.simples.append(sc)

    def insert(self, index, sc):
        if isinstance(sc, SimpleChecker):
            self.simples.insert(index, sc)

    def index(self, sc):
        if isinstance(sc, SimpleChecker):
            self.simples.index(sc)

    def remove(self, sc):
        if isinstance(sc, SimpleChecker):
            self.simples.remove(sc)

    def send_task_to_queue(self, simple):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=simple.host,
                port=simple.port,
                virtual_host=simple.virtual_host,
                credentials=pika.PlainCredentials(simple.user, simple.password)
            )
        )
        channel = connection.channel()
        channel.queue_declare(queue=simple.queue)
        channel.basic_publish(exchange="",
                              routing_key=simple.queue,
                              body=str("test"))
        connection.close()

    def get_task_from_queue(self, simple):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=simple.host,
                port=simple.port,
                virtual_host=simple.virtual_host,
                credentials=pika.PlainCredentials(simple.user, simple.password)
            )
        )
        channel = connection.channel()
        channel.queue_declare(queue=simple.queue)
        res = channel.basic_get(queue=simple.queue, no_ack=True)
        connection.close()

        return res

    def check(self, simple):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=simple.host,
                port=simple.port,
                virtual_host=simple.virtual_host,
                credentials=pika.PlainCredentials(simple.user, simple.password)
            )
        )
        channel = connection.channel()
        # channel.queue_declare(queue=simple.queue, durable=True)
        res = channel.queue_declare(queue=simple.queue, passive=True)
        connection.close()

        return res.method.message_count

    def run(self):
        results = [1]

        while True:
            pool = ThreadPool(self.threads)
            results = pool.map(self.check, self.simples)
            pool.close()
            pool.join()
            print(results)
            if all(el == 0 for el in results):
                break
            time.sleep(self.period * 60)


def main():
    try:
        with open(sys.argv[1], "r") as config:
            conf = yaml.load(config)
        ch = Checker(conf["period"], conf["threads"])
        for el in conf["targets"]:
            ch.add(SimpleChecker(el["host"], el["port"], el["virtual_host"],
                                 el["queue"], el["user"], el["password"]))
        ch.run()
    except KeyboardInterrupt:
        print('\nThe process was interrupted by the user')
        raise SystemExit


if __name__ == "__main__":
    main()
