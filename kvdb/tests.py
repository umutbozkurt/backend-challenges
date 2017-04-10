import unittest

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

        payload = {
            'command': 'SET',
            'args': {
                'key': key,
                'value': val
            }
        }

        response = self.server.handle_message(payload)

        self.assertEqual(response['status'], Server.STATUS_OK)
        self.assertEqual(response['result'], None)
        self.assertEqual(self.server.data[key]['value'], val)

    def test_get(self):
        payload = {
            'command': 'GET',
            'args': {
                'key': 'ASD'
            }
        }

        response = self.server.handle_message(payload)

        self.assertEqual(response['status'], Server.STATUS_OK)
        self.assertEqual(response['result'], None)

    def test_delete(self):
        key = 'KEY'
        self.server.set(key, 'VAL')

        payload = {
            'command': 'DELETE',
            'args': {
                'key': key
            }
        }

        response = self.server.handle_message(payload)

        self.assertEqual(response['status'], Server.STATUS_OK)
        self.assertEqual(response['result'], None)

        with self.assertRaises(KeyError):
            _ = self.server.data[key]

    def test_increment(self):
        key = 'KEY'
        val = 1000

        self.server.set(key, val)

        payload = {
            'command': 'INCR',
            'args': {
                'key': key
            }
        }

        response = self.server.handle_message(payload)

        self.assertEqual(response['status'], Server.STATUS_OK)
        self.assertEqual(response['result'], val + 1)


if __name__ == '__main__':
    unittest.main()