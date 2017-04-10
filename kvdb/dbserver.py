import json
import socket
import functools


class Store(object):

    def __init__(self, key, value, ttl=0):
        self.key = key
        self.value = value
        self.ttl = ttl

    def serialize(self):
        return {self.key: {'value': self.value, 'ttl': self.ttl}}

    @classmethod
    def deserialize(cls, key, data):
        if data:
            return cls(key, data['value'], data.get('ttl'))
        else:
            return cls(key, None)


class ServerError(Exception):
    message = 'Error :/'


class CommandNotFoundError(ServerError):
    message = 'Unknown command'


class NotIncrementableError(ServerError):
    message = 'Provided key`s value is not incrementable'


class Server(object):
    STATUS_OK = 'OK'
    STATUS_ERROR = 'ERROR'

    def __init__(self, host='localhost', port=4242):
        self.host = host
        self.port = port
        self.socket = None
        self.data = {}

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
            'INCR': self.increment
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

    def get(self, key):
        store = Store.deserialize(key, self.data.get(key))

        if store:
            return store.value
        else:
            return None

    def set(self, key, value, ttl=0):
        store = Store(key, value, ttl=ttl)
        self.save_store(store)

    def save_store(self, store):
        self.data.update(store.serialize())

    def delete(self, key):
        if key in self.data:
            del self.data[key]

    def increment(self, key):
        value = self.data.get(key)['value']

        if value:
            try:
                value += 1
            except TypeError:
                raise NotIncrementableError()
            else:
                store = Store(key, value)
        else:
            store = Store(key, 1)

        self.save_store(store)
        return store.value

    def expire(self, key, ttl=None):
        store = self.data.get(key)

        if not ttl:
            return store['ttl']
        else:
            store.ttl = ttl
            return self.save_store(store)


if __name__ == '__main__':
    server = Server()
    server.run()
