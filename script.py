import socket
import json
import re
import urllib.parse
import psycopg2
from psycopg2.extras import RealDictCursor
from db_config import dbname, user, password, host
from time_zones import time_zones


class Server:
    def __init__(self, ip: str, port: int) -> None:
        # ip - ip адрес сервера
        # port - порт сервера
        # server - сокет сервера

        self.__ip: str = ip
        self.__port: int = port
        self.__server: socket.socket = self.create_server()

    @property
    def ip(self) -> str:
        return self.__ip

    @property
    def port(self) -> int:
        return self.__port

    def create_server(self) -> socket.socket:
        # Возвращает сокет сервера

        return socket.create_server((self.__ip, self.__port))

    def close_server(self) -> None:
        # Закрывает сервер

        print('Closing server...')
        self.__server.close()

    def start_server(self) -> None:
        # Запускает сервер, подключает клиентов, передает запросы в обработку
        # и возвращает ответ клиенту

        try:
            self.__server.listen()
            print('Server is running')

            while True:
                client_socket, client_address = self.__server.accept()
                print(f'{client_address} connected')
                # Получаем запрос клиента
                data: str = client_socket.recv(1024).decode('utf-8')

                if data:
                    print(f"Processing {client_address} client's request...")
                    # Ответ сервера на запрос
                    content: bytes = self.process_request(data)
                    # Передаем ответ клиенту
                    client_socket.send(content)

                # Отключаем клиентский сокет
                client_socket.shutdown(socket.SHUT_WR)

        except KeyboardInterrupt:
            self.close_server()

    def process_request(self, data: str) -> bytes:
        # Обрабатывает запрос клиента и в зависимости от результата возвращает ответ
        # data - http запрос клиента
        # HDRS, HDRS_404 - заголовки html страницы
        # request - запрос клиента, например, '/code/p/name/санкт-петербург'
        # response - ответ сервера, если возвращается None, то значит запрос
        # был сформирован некорректно

        HDRS: str = 'HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n'
        HDRS_404: str = 'HTTP/1.1 404 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n'

        request: str = urllib.parse.unquote(data.split()[1][1:]).lower()

        if request[:5] == 'code/':
            if request[5] in ['a', 'h', 'l', 'p', 'r', 's', 't', 'u', 'v']:
                response: list = self.check_request(request[7:], request[5])
            else:
                response = None
        else:
            response: list = self.check_request(request)

        if response is None:
            return (HDRS_404 + 'Error. Unable to process the request').encode('utf-8')
        else:
            return (HDRS + self.convert_to_json(response)).encode('utf-8')

    def check_request(self, request: str, code: str = '') -> list:
        # Определяет вид запроса клиента, формирует sql запросы к базе данных
        # Возвращает результат SELECT запроса к бд в виде списка
        # request - get запрос клиента
        # code - код географического объекта, если не указан в запросе, то будет равен
        # пустой строке
        # sql_query - sql запрос
        # data - кортеж с данными, которые будут переданы в динамический запрос к бд

        sql_query: str = ''
        data: tuple = ()

        if not code:
            if re.fullmatch(r'id/[0-9]+$', request):
                # Поиск по id, осуществляется, если не указан код географического объекта
                # Запрос выглядит как id/{id географического объекта}

                object_id: int = int(request[3:])
                sql_query = 'SELECT * FROM geoname_ru WHERE geonameid = %s'
                data = (object_id,)

            elif re.fullmatch(r'compare/id[?]object1=[0-9]+&object2=[0-9]+$', request):
                # Возвращает список с информацией о двух географических объектах
                # и о различиях между ними (какой севернее и разность часовых поясов)
                # В данном запросе указывается id объекта
                # Примеры обрабатываемых запросов:
                # compare/id?object1=453489&object2=467263

                id_geo_object1: int = int(
                    re.search(r'[=][0-9]+[&]', request).group(0).replace("=", "").replace("&", ""))
                id_geo_object2: int = int(re.search(r'[=][0-9]+$', request).group(0).replace("=", ""))

                sql_query = 'SELECT * FROM geoname_ru WHERE geonameid = %s'

                data = (id_geo_object1,)
                geo_object1 = self.process_sql_query(sql_query, data)

                data = (id_geo_object2,)
                geo_object2 = self.process_sql_query(sql_query, data)

                return self.get_information_about_two_geo_objects(geo_object1, geo_object2)

        if re.fullmatch(r'tips/[0-9a-zа-яё -]+$', request):
            # Вывод подсказок для названий географических объектов
            # Примеры обрабатываемых запросов:
            # tips/моск
            # code/h/tips/моск

            part_of_name: str = '%' + request[5:] + '%'

            if code:
                sql_query = 'SELECT DISTINCT alternate_name, geonameid, feature_class, population FROM (' \
                            'SELECT *, UNNEST(alternatenames) AS alternate_name FROM geoname_ru ' \
                            'WHERE feature_class ILIKE %s) AS alternate_names ' \
                            'WHERE alternate_name ILIKE %s ORDER BY population DESC, alternate_name'
                data = (code, part_of_name)
            else:
                sql_query = 'SELECT DISTINCT alternate_name, geonameid, feature_class, population FROM (' \
                            'SELECT *, UNNEST(alternatenames) AS alternate_name FROM geoname_ru ' \
                            ') AS alternate_names ' \
                            'WHERE alternate_name ILIKE %s ORDER BY population DESC, alternate_name'
                data = (part_of_name,)

        elif re.fullmatch(r'all/[0-9]+$', request):
            # Возвращает указанное количество первых записей таблицы
            # Примеры обрабатываемых запросов:
            # all/10
            # code/h/all/5

            limit: int = int(request[4:])

            if code:
                sql_query = 'SELECT * FROM geoname_ru WHERE feature_class ILIKE %s LIMIT %s'
                data = (code, limit)
            else:
                sql_query = 'SELECT * FROM geoname_ru LIMIT %s'
                data = (limit,)

        elif re.fullmatch(r'all$', request):
            # Возвращает все записи
            # Примеры обрабатываемых запросов:
            # all
            # code/h/all

            if code:
                sql_query = 'SELECT * FROM geoname_ru WHERE feature_class ILIKE %s'
                data = (code,)
            else:
                sql_query = 'SELECT * FROM geoname_ru'

        elif re.fullmatch(r'name/[0-9a-zа-яё -]+$', request):
            # Производит поиск географисеского объекта по имени
            # Если находятся несколько, то сначала выводятся объекты с наибольшим населением
            # Примеры обрабатываемых запросов:
            # name/питер
            # code/p/name/москва

            name: str = request[5:]

            if code:
                sql_query = 'SELECT * FROM geoname_ru WHERE EXISTS ( ' \
                            'SELECT * FROM UNNEST(alternatenames) AS alternatename WHERE alternatename ILIKE %s ) ' \
                            'AND feature_class ILIKE %s ORDER BY population DESC, geoname_ru.name'
                data = (name, code)
            else:
                sql_query = 'SELECT * FROM geoname_ru WHERE EXISTS ( ' \
                            'SELECT * FROM UNNEST(alternatenames) AS alternatename WHERE alternatename ILIKE %s ) ' \
                            'ORDER BY population DESC, geoname_ru.name'
                data = (name,)

        return self.process_sql_query(sql_query, data)

    @staticmethod
    def process_sql_query(query: str, data: tuple) -> list:
        # Коннектится к базе данных, выполняет запрос и возвращает результат запроса
        # query - запрос к бд
        # data - кортеж с данными, которые будут переданы в динамический запрос к бд
        # response - список с результатом выполнения SELECT запроса

        if query:
            conn = psycopg2.connect(dbname=dbname, user=user,
                                    password=password, host=host)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query, data)
            response: list = cursor.fetchall()
            cursor.close()
            conn.close()

            return response

    @staticmethod
    def get_information_about_two_geo_objects(obj1: list, obj2: list) -> list:
        # Принимает на вход два списка:
        # obj1 - информация о первом географическом объекте
        # obj2 - информация о втором географическом объекте
        # Определяет различия временных зон и какой объект находится севернее
        # differences: dict - словарь c различиями
        # obj1_latitude: float - широта первого объекта
        # obj2_latitude: float - широта второго объекта
        # obj1_time_zone: int - часовой пояс первого объекта
        # obj2_time_zone: int - часовой пояс второго объекта
        # Возвращает список со словарями: информацию о двух географических объектах и их различия
        # Если id города указан неверно, то запрос не обрабатывается

        if obj1 and obj2:
            differences: dict = {}
            obj1_latitude: float = float(obj1[0]['latitude'])
            obj2_latitude: float = float(obj2[0]['latitude'])
            obj1_time_zone: int = time_zones[obj1[0]['timezone']]
            obj2_time_zone: int = time_zones[obj2[0]['timezone']]

            # Определяем самый северный город
            if obj1_latitude > obj2_latitude:
                differences['The northernmost geo object'] = obj1[0]['name']
            elif obj1_latitude < obj2_latitude:
                differences['The northernmost geo object'] = obj2[0]['name']
            else:
                differences['The northernmost geo object'] = \
                    "Can't say for sure. These two geo objects have the same latitude"

            # Определяем различие временных зон
            time_zone_diff: int = abs(obj1_time_zone - obj2_time_zone)
            differences['Timezones differences'] = time_zone_diff

            return [
                {'geo object 1': obj1[0]},
                {'geo object 2': obj2[0]},
                {'differences': differences}
            ]
        else:
            return ['An unknown geo object was specified in the request']

    @staticmethod
    def convert_to_json(data: list):
        # Конвертирует список data в формат JSON

        return json.dumps(data, indent=4, ensure_ascii=False, default=str)


if __name__ == '__main__':
    server = Server('127.0.0.1', 8000)
    server.start_server()
