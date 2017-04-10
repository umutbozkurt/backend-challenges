import unittest
import time
from unittest.mock import patch

from dbserver import Server


class ServerTests(unittest.TestCase):

    def setUp(self):
        self.server = Server()

    def test_ping(self):
        payload = {
            'command': 'PING'
        }

        response = self.server.handle_message(payload)

        self.assertEqual(response['status'], Server.STATUS_OK)
        self.assertEqual(response['result'], 'PONG')

    def test_set(self):
        key = 'KEY'
        val = 'VAL'

        self.server.set(key, val)

        store = self.server._get_store(key)
        self.assertEqual(store.value, val)

    def test_get(self):
        key = 'KEY'
        value = 'VAL'
        self.server.set(key, value)

        out = self.server.get(key)
        self.assertEqual(out, value)

    def test_delete(self):
        key = 'KEY'
        self.server.set(key, 'VAL')
        self.server.delete(key)
        self.assertEqual(self.server.get(key), None)

        with self.assertRaises(KeyError):
            _ = self.server.data[key]

    def test_increment(self):
        key = 'KEY'
        val = 1000

        self.server.set(key, val)
        out = self.server.increment(key)

        self.assertEqual(out, val + 1)

    def test_increment_null(self):
        out = self.server.increment('key')
        self.assertEqual(out, 1)

    def test_decrement(self):
        key = 'KEY'
        val = 1000

        self.server.set(key, val)
        out = self.server.decrement(key)

        self.assertEqual(out, val - 1)

    def test_decrement_null(self):
        out = self.server.decrement('key')

        self.assertEqual(out, -1)

    @patch('dbserver.time')
    def test_ttl(self, time):
        key = 'KEY'
        ttl = 1000

        time.time.return_value = 100000

        self.server.set(key, 'val', ttl=ttl)
        self.server.expire(key, ttl)

        time.time.return_value = 100001  # 1 second passed

        self.assertLess(self.server.ttl(key), ttl)


if __name__ == '__main__':
    unittest.main()