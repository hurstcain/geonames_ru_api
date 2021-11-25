import socket
import json
import re
import threading
import urllib.parse


# Из файла с географическими объектами выбирает только населенные пункты
# и добавляет информацию о них в список, а затем возвращает его
def init_city_list():
    f = open('RU.txt', 'r', encoding='utf-8')
    city_list = []
    # id столбца feature class, в котором содержится
    # класс географического объекта
    feature_class_id = 6
    # Класс географического объекта, который обозначает населенный пункт
    feature_class = 'P'

    for string in f:
        city_info_list = string[:-1].split('\t')
        if city_info_list[feature_class_id] == feature_class:
            city_list.append(city_info_list)

    f.close()

    return city_list


# Список с городами и их информацией
CITY_LIST = init_city_list()


# Запускает сервер
def start_server():
    server = socket.create_server(('127.0.0.1', 8000))
    try:
        server.listen()
        print('Server is running')
        while True:
            client_socket, client_address = server.accept()
            # Создаем и запускаем поток, который будет обрабатывать конкретный запрос
            client_handler = threading.Thread(target=process_request, args=(client_socket,))
            client_handler.start()
    except KeyboardInterrupt:
        print('Closing server...')
        server.close()


# Обрабатывает запрос клиента
def process_request(client_socket):
    # Содержимое запроса
    data = client_socket.recv(1024).decode('utf-8')
    if data:
        print('Processing a request...')
        # Ответ сервера на запрос
        content = get_response(data)
        # Передаем ответ клиенту
        client_socket.send(content)
    # Отключаем клиентский сокет
    client_socket.shutdown(socket.SHUT_WR)


# Возвращает ответ сервера на запрос клиента
def get_response(data):
    HDRS = 'HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n'
    HDRS_404 = 'HTTP/1.1 404 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n'

    request = urllib.parse.unquote(data.split()[1][1:])
    response = check_request(request)
    if response is None:
        return (HDRS_404 + 'Error. Unable to process the request').encode('utf-8')
    else:
        return (HDRS + convert_to_json(response)).encode('utf-8')


# Определяет вид запроса и в зависимости от вида запроса возвращает ответ
def check_request(request):
    if re.fullmatch(r'[0-9a-zA-Zа-яА-ЯёЁ -]+$', request):
        return get_tips(request)
    elif re.fullmatch(r'id/[0-9]+$', request):
        return get_city_info_by_id(request[3:])
    elif re.fullmatch(r'name/[0-9a-zA-Zа-яА-ЯёЁ -]+$', request):
        return get_city_info_by_name(request[5:])
    elif re.fullmatch(r'cities/[0-9]+$', request):
        return get_cities_list(int(request[7:]))
    elif re.fullmatch(r'[?]city1=[0-9a-zA-Zа-яА-ЯёЁ -]+&city2=[0-9a-zA-Zа-яА-ЯёЁ -]+$', request):
        return get_information_about_two_cities(request)
    else:
        return None


# Возвращает список городов, название которых начинается
# на или совпадает со значением переменной first_part_of_name
# first_part_of_name - часть названия города, введенная пользователем
def get_tips(first_part_of_name):
    # id столбца alternatenames, в котором содержатся
    # альтернативные названия городов
    id = 3
    cities_list = []

    for city_info_list in CITY_LIST:
        # Список с альтернативными названиями текущего города
        alternative_names_list = city_info_list[id].split(',')
        for alternative_name in alternative_names_list:
            if re.match(first_part_of_name.lower(), alternative_name.lower()):
                cities_list.append(alternative_name)

    return list(set(cities_list))


# Возвращает информацию о городе по id
def get_city_info_by_id(id):
    city_info = None

    for city_info_list in CITY_LIST:
        if city_info_list[0] == id:
            city_info = city_info_list
            break

    if city_info:
        return put_data_to_dict(city_info)
    else:
        return 'There is no city with the entered id'


# Записывает информацию о городе в словарь
def put_data_to_dict(city_info):
    # Ключи словаря
    dict_keys = ['geonameid', 'name', 'asciiname', 'alternatenames', 'latitude',
                    'longitude', 'feature class', 'feature code', 'country code',
                    'cc2', 'admin1 code', 'admin2 code', 'admin3 code', 'admin4 code',
                    'population', 'elevation', 'dem', 'timezone', 'modification date']
    # Словарь, содержащий информацию о городе
    dict_with_city_info = {}

    for i in range(0, len(dict_keys)):
        if city_info[i]:
            dict_with_city_info.update({dict_keys[i]: city_info[i]})
        else:
            dict_with_city_info.update({dict_keys[i]: 'no info'})

    return dict_with_city_info


# Возвращает список с информацией о городах
def get_cities_list(number_of_cities):
    cities_list = []

    if number_of_cities > len(CITY_LIST):
        number_of_cities = len(CITY_LIST)

    for i in range(0, number_of_cities):
        cities_list.append(put_data_to_dict(CITY_LIST[i]))

    return cities_list


# Возвращает словарь с информацией о двух городах
# и о различиях между ними (какой город севернее и разность часовых поясов)
def get_information_about_two_cities(request):
    # Выделяем названия городов из запроса
    city1 = re.search(r'[=][0-9a-zA-Zа-яА-ЯёЁ -]+[&]', request).group(0).replace("=", "").replace("&", "")
    city2 = re.search(r'[=][0-9a-zA-Zа-яА-ЯёЁ -]+$', request).group(0).replace("=", "")
    # Получаем информацию о городах
    city1_info = get_city_info_by_name(city1)
    city2_info = get_city_info_by_name(city2)

    if city1_info and city2_info:
        # Получаем различия двух городов
        diff = get_differences(city1_info, city2_info)

        cities_info = {}
        cities_info.update({
            'city1': city1_info,
            'city2': city2_info,
            'differences': diff
        })
        return cities_info
    else:
        return 'An unknown city was specified in the request'


# Возвращает информацию о городе по его названию
# Если существует несколько городов с таким названием, то
# выбирается город с наибольшим населением
def get_city_info_by_name(name):
    # id столбца alternatenames, в котором содержатся
    # альтернативные названия городов
    id = 3
    cities_list = []

    for city_info_list in CITY_LIST:
        # Список с альтернативными названиями текущего города
        alternative_names_list = city_info_list[id].lower().split(',')
        if name.lower() in alternative_names_list:
            cities_list.append(put_data_to_dict(city_info_list))

    if cities_list:
        return choose_city(cities_list)
    else:
        return None


# Из списка городов возвращает город с наибольшим населением
def choose_city(cities_list):
    max_population = int(cities_list[0].get('population'))
    id = 0

    for i in range(1, len(cities_list)):
        if int(cities_list[i].get('population')) > max_population:
            max_population = int(cities_list[i].get('population'))
            id = i

    return cities_list[id]


# Возвращает словарь с различиями двух городов
def get_differences(city1, city2):
    differences = {'differences': {}}
    time_zones = get_time_zones()
    city1_latitude = city1.get('latitude')
    city2_latitude = city2.get('latitude')

    # Определяем самый северный город
    if float(city1_latitude) > float(city2_latitude):
        differences['differences'].update({
            'The northernmost city': city1.get('name')
        })
    elif float(city1_latitude) < float(city2_latitude):
        differences['differences'].update({
            'The northernmost city': city2.get('name')
        })
    else:
        differences['differences'].update({
            'The northernmost city': "Can't say for sure. These two cities have the same latitude"
        })

    # Определяем различие временных зон
    time_zone_diff = abs(time_zones.get(city1.get('timezone')) - time_zones.get(city2.get('timezone')))
    differences['differences'].update({
        'Timezones differences': time_zone_diff
    })

    return differences


# Возвращает словарь с названиями временных зон и их GMT
def get_time_zones():
    time_zones = {
        'Europe/Kiev': 2.0,
        'Asia/Vladivostok': 10.0,
        'Asia/Tbilisi': 4.0,
        'Europe/Simferopol': 3.0,
        'Asia/Krasnoyarsk': 7.0,
        'Asia/Sakhalin': 11.0,
        'Asia/Anadyr': 12.0,
        'Europe/Volgograd': 3.0,
        'Asia/Srednekolymsk': 11.0,
        'Asia/Qyzylorda': 5.0,
        'Asia/Shanghai': 8.0,
        'Asia/Aqtobe': 5.0,
        'Europe/Samara': 4.0,
        'Asia/Omsk': 6.0,
        'Asia/Novokuznetsk': 7.0,
        'Asia/Ulaanbaatar': 8.0,
        'Asia/Barnaul': 7.0,
        'Asia/Tokyo': 9.0,
        'Europe/Warsaw': 1.0,
        'Europe/Paris': 1.0,
        'Asia/Hovd': 7.0,
        'Europe/Helsinki': 2.0,
        'Europe/Saratov': 4.0,
        'Europe/Oslo': 1.0,
        'Asia/Chita': 9.0,
        'Asia/Magadan': 11.0,
        'Asia/Tashkent': 5.0,
        'Asia/Ashgabat': 5.0,
        'Asia/Yakutsk': 9.0,
        'Europe/Riga': 2.0,
        'Europe/Kirov': 3.0,
        'Europe/Monaco': 1.0,
        'Asia/Baku': 4.0,
        'Europe/Zaporozhye': 2.0,
        'Asia/Tomsk': 7.0,
        'Asia/Novosibirsk': 7.0,
        'Asia/Khandyga': 9.0,
        'Europe/Vilnius': 2.0,
        'Europe/Kaliningrad': 2.0,
        'Europe/Astrakhan': 4.0,
        'Asia/Yekaterinburg': 5.0,
        'Asia/Ust-Nera': 10.0,
        'Asia/Irkutsk': 8.0,
        'Europe/Minsk': 3.0,
        'Asia/Kamchatka': 12.0,
        'Europe/Moscow': 3.0,
        'Europe/Ulyanovsk': 4.0,
    }

    return time_zones


# Преобразовывает данные в формат JSON
def convert_to_json(data):
    return json.dumps(data, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    start_server()
