import os.path
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time
import psycopg2
import requests


#Получение доступа
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, 'credentials.json')

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

SAMPLE_SPREADSHEET_ID = '13l1ypPyESI1Q3ullkY4W70dgBf5QVV3veC8TaXnp7Vc'
SAMPLE_RANGE_NAME = 'Лист1'

#Получение данных
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                            range=SAMPLE_RANGE_NAME).execute()

values = result.get('values', [])
#Берем id последнего заказа
size_list = int(len(values))
last_number_1 = values[size_list-1][0]

#Курс доллара
data = requests.get('https://www.cbr-xml-daily.ru/daily_json.js').json()
data_USD = data['Valute']['USD']
value_USD = float(data_USD['Value'])

#Подключение к posеgresql и создание БД
conn = psycopg2.connect(user="postgres",
                        password="qwerty",
                        host="127.0.0.1",
                        port="5432",
                        database='postgres')
c = conn.cursor()
c.execute(
      'CREATE TABLE IF NOT EXISTS orders(id int, number_order int, price_USD int, price_RUB int, data date );')

for x in range(1, len(values)):
    id = values[x][0]
    number_order = values[x][1]
    price_USD = int(values[x][2])
    price_RUB = int(price_USD*value_USD)
    data = values[x][3]
    c.execute(
        "INSERT INTO orders(id, number_order, price_USD, price_RUB, data) VALUES (%s, %s, %s, %s, %s)",
        [id, number_order, price_USD, price_RUB, data])
    conn.commit()

conn.close()

# id последнего заказа
list=[]
list.append(last_number_1)

#Запускаем мониторинг
while True:
    print('START REFRESH')
    #Мониторинг USD
    data = requests.get('https://www.cbr-xml-daily.ru/daily_json.js').json()
    data_USD = data['Valute']['USD']
    value_USD_2 = int(data_USD['Value'])
    #Считываем таблицу
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()

    values2 = result.get('values', [])
    #Берем  последней id новой таблицы
    size_list2 = int(len(values2))
    last_number_3 = values2[size_list2-1][0]
    #Переносим последний id предыдущей таблицы в цикл
    test_number = list[0]
    last_number_2 = int(test_number)
    #Очищаем последний id предыдущей таблицы
    list.clear()
    #Сохраняем последний id новой таблицы
    list.append(last_number_3)
    #Подключаемся к БД и начинаем сверять значения
    conn = psycopg2.connect(user="postgres",
                            password="qwerty",
                            host="127.0.0.1",
                            port="5432",
                            database='postgres')
    c = conn.cursor()

    for x in range(1, size_list2):
        id1 = int(values2[x][0])
        # print('Look'+str(id1))
        if id1 <= last_number_2:
            c.execute(f"SELECT COUNT(id) FROM orders WHERE id ={id1}")
            id11 = int(c.fetchone()[0])
            if id11 == 0:
                # print('id ==0 '+str(id1))
                c.execute(f"DELETE FROM orders WHERE id ='{id1}'")
                conn.commit()
            elif id11 == 1:
                # print('id == 1 '+str(id1))
                var11 = c.execute(f"SELECT * FROM orders WHERE id ={id1}")
                var1 = c.fetchone()
                var2 = values2[x]
                dt = time.strptime(str(var1[4]), '%Y-%m-%d')
                dt1 = time.strftime('%Y-%m-%d', dt)
                dt = time.strptime(str(var2[3]), '%d.%m.%Y')
                dt2 = time.strftime('%Y-%m-%d', dt)
                if (var1[1] != int(var2[1])) or (int(var1[2]) != int(var2[2])) or (dt1 != dt2) :
                    if var1[1] != int(var2[1]):
                        # print('Меняю №заказа'+str(id1))
                        c.execute(f"UPDATE orders SET number_order ={var2[1]} WHERE number_order={var1[1]}")
                    if int(var1[2]) != int(var2[2]):
                        # print('Меняю прайс' + str(id1))
                        c.execute(f"UPDATE orders SET price_USD ={var2[2]} WHERE price_USD={var1[2]}")
                        new_RUB=int(int(var2[2])*(value_USD_2))
                        c.execute(f"UPDATE orders SET price_RUB ='{new_RUB}' WHERE price_RUB={var1[3]}")
                    if dt1 != dt2:
                        # print('Меняю Дату' + str(id1))
                        c.execute(f"UPDATE orders SET data ='{dt2}' WHERE data='{dt1}'")
                    conn.commit()
                else:
                    pass
        else:
            pass
        if id1 > last_number_2:
            # print('Добавляю новый id' + str(id1))
            id_cikl = values2[x][0]
            number_order_cikl = values2[x][1]
            price_USD_cikl = int(values2[x][2])
            price_RUB_cikl = int(price_USD_cikl*value_USD_2)
            data_cikl = values2[x][3]
            c.execute(
                    "INSERT INTO orders(id, number_order, price_USD, price_RUB, data) VALUES (%s, %s, %s, %s, %s)",
                    [id_cikl, number_order_cikl, price_USD_cikl, price_RUB_cikl, data_cikl])
            conn.commit()
        else:
            pass
        # time.sleep(0.5)

    c.execute('SELECT * FROM orders ORDER BY id');
    conn.commit()
    #Таймер для обновления данных
    # print("SLEEP")
    time.sleep(10)
