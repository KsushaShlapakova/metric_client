import socket
import time as t


class ClientError(Exception):
    def __init__(self, notification, err=None):
        self.notification = notification
        self.err = err


class Client:
    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connection = None
        self.conn()

    def conn(self):
        try:
            self.connection = socket.create_connection((self.host, self.port), self.timeout)
        except socket.error as err:
            raise ClientError("Ошибка соединения", err)

    def put(self, key, value, timestamp=None):
        with self.connection:
            try:
                if not timestamp:
                    timestamp = str(int(t.time()))
                self.connection.send("put {} {} {}\n".format(key, value, timestamp).encode('utf-8'))
            except socket.error as err:
                raise ClientError("Ошибка при отправке запроса", err)
            self.answer()

    def get(self, key):
        with self.connection:
            try:
                self.connection.send("get {}\n".format(key).encode('utf-8'))
            except socket.error as err:
                raise ClientError("Ошибка при отправке запроса", err)

            ans = self.answer()
            return self.structure(ans)

    @staticmethod
    def structure(ans):
            di = {}

            if not ans:
                return di

            a = ans.split('\n')

            for i in a:
                key = str(i.split()[0])
                metric_value = float(i.split()[1])
                timestamp = int(i.split()[2])
                if key not in di:
                    di[key] = [(timestamp, metric_value)]
                else:
                    di[key].append((timestamp, metric_value))
            d = {}
            a = sorted(di.items(), key=lambda item: item[1][0])
            for i in a:
                d[i[0]] = i[1]
            return d

    def answer(self):
        with self.connection:
            try:
                s = self.connection.recv(1024).decode()
                main_answer = s.split("\n", maxsplit=1)
                result1 = main_answer[0]
                result2 = main_answer[1]

            except socket.error as err:
                raise ClientError("Ошибка получения ответа сервера", err)

            if result1 == "error":
                raise ClientError('Ошибка в запросе', result2)
            return result2[:-2]
