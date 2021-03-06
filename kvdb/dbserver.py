import json
import socket
import functools
import time
import threading
from collections import defaultdict


class Store(object):

    def __init__(self, key, value, ttl=0, seed=None):
        self.key = key
        self.value = value
        self.__ttl = ttl
        self.seed = seed or int(time.time())

    def serialize(self):
        return {
            self.key: {
                'value': self.value,
                'ttl': self.__ttl,
                'seed': self.seed
            }
        }

    @classmethod
    def deserialize(cls, key, data):
        if data:
            return cls(key, data['value'], data.get('ttl'), seed=data.get('seed'))
        else:
            return cls(key, None)

    @property
    def ttl(self):
        """
        :returns ttl, persistent
        """
        diff = int(time.time()) - self.seed
        return max(self.__ttl - diff, 0), self.__ttl == 0

    @property
    def expired(self):
        ttl, persistent = self.ttl
        return ttl <= 0 and not persistent

    @property
    def expiration(self):
        return self.__ttl + self.seed

    @property
    def persistent(self):
        return self.__ttl == 0


class ServerError(Exception):
    message = 'Error :/'


class CommandNotFoundError(ServerError):
    message = 'Unknown command'


class NotIncrementableError(ServerError):
    message = 'Provided key`s value is not incrementable'


class ExpiryService(threading.Timer):

    def __init__(self, interval, args=None, kwargs=None):
        super(ExpiryService, self).__init__(
            interval,
            self.cleanup,
            args=args,
            kwargs=kwargs
        )

        self.keys = defaultdict(list)

    def subscribe(self, store):
        self.keys[store.expiration].append(store.key)

    def unsubscribe(self, store):
        keys = self.keys[store.expiration]
        keys.remove(store.key)

        self.keys[store.expiration] = keys

    def cleanup(self):
        timestamp = int(time.time())

        for key in self.keys[timestamp]:
            server.delete(key)


class Server(object):
    STATUS_OK = 'OK'
    STATUS_ERROR = 'ERROR'

    def __init__(self, host='localhost', port=4242):
        self.host = host
        self.port = port
        self.socket = None
        self.data = {}

        self.expiry_service = ExpiryService(1)
        self.expiry_service.run()

    def run(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # bind the socket to a public host, and a well-known port
        self.socket.bind((self.host, self.port))
        # become a server socket
        self.socket.listen(5)

        while True:
            # accept connections from outside
            (clientsocket, address) = self.socket.accept()
            print('Client connected: %s' % (address,))
            # now do something with the clientsocket
            self.handle_client(clientsocket)

    def handle_client(self, client):
        message = self._receive(client)
        result = self.handle_message(message)
        d = json.dumps(result)
        client.sendall(bytes(d + '\n', 'utf8'))

    def _receive(self, client):
        buff = ''
        while True:
            buff += client.recv(2048).decode('utf8')
            if not buff:
                # connection has been closed
                return None
            # messages are delimited by \n
            if buff[-1] == '\n':
                break
        buff = buff[:-1]
        print(buff)
        message = json.loads(buff)
        return message

    def handle_message(self, message):
        """
        - Handle Message
        - Process Input
        - Run Command
        - Return Output/Error
        """
        command = self.process_input(message)

        try:
            out = command()
        except ServerError as err:
            return self.serialize_error(err.message)
        else:
            return self.serialize_output(out)

    def serialize_output(self, output):
        return {
            'result': output,
            'status': Server.STATUS_OK
        }

    def serialize_error(self, message):
        return {
            'result': message,
            'status': Server.STATUS_ERROR
        }

    def process_input(self, input):
        """
        Parse input to callable
        """
        MAP = {
            'GET': self.get,
            'SET': self.set,
            'PING': self.ping,
            'DELETE': self.delete,
            'INCR': self.increment,
            'DECR': self.decrement,
            'TTL': self.ttl,
            'EXPIRE': self.expire
        }

        try:
            command_func = MAP[input['command']]
        except KeyError:
            raise CommandNotFoundError()

        return functools.partial(
            command_func,
            **input.get('args', {})
        )

    def ping(self):
        return 'PONG'

    def _get_store(self, key):
        return Store.deserialize(key, self.data.get(key))

    def _save_store(self, store):
        self.data.update(store.serialize())

        if not store.persistent:
            self.expiry_service.subscribe(store)

    def get(self, key):
        store = self._get_store(key)

        if store:
            if store.expired:
                return self.delete(key)
            else:
                return store.value
        else:
            return None

    def set(self, key, value, ttl=0):
        store = Store(key, value, ttl=ttl)
        self._save_store(store)

    def delete(self, key):
        store = self._get_store(key)

        if not store.persistent:
            self.expiry_service.unsubscribe(store)

        if key in self.data:
            del self.data[key]

    def increment(self, key, increment_by=1):
        store = self._get_store(key)

        if store.value:
            try:
                store.value += increment_by
            except TypeError:
                raise NotIncrementableError()
        else:
            store = Store(key, increment_by)

        self._save_store(store)
        return store.value

    def decrement(self, key):
        return self.increment(key, increment_by=-1)

    def expire(self, key, ttl):
        store = self._get_store(key)
        store.__ttl = ttl
        return self._save_store(store)

    def ttl(self, key):
        store = self._get_store(key)
        return store.ttl[0]


server = Server()


if __name__ == '__main__':
    server.run()
