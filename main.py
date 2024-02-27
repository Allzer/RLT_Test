from datetime import datetime
import pandas as pd
import telebot
import json
import bson
from pymongo import MongoClient

TOKEN = '6663362459:AAGMxziqOkpQ9-EDuVC1sgHzXMJNJIRU22A'

bot = telebot.TeleBot(TOKEN)


def send_message(chat_id, text):
    # Определение максимальной длины сообщения (в символах)
    max_message_length = 4096

    # Проверка, нужно ли разделить сообщение на несколько частей
    if len(text) <= max_message_length:
        # Если длина текста меньше или равна максимальной длине сообщения,
        # отправляем его как есть
        bot.send_message(chat_id, text)
    else:
        # Если длина текста превышает максимальную длину сообщения,
        # разбиваем текст на части по максимальной длине
        for i in range(0, len(text), max_message_length):
            # Отправляем очередную часть текста
            bot.send_message(chat_id, text[i:i + max_message_length])


@bot.message_handler(content_types=['text'])
def send_message_handler(message):
    try:
        # Парсинг JSON из текстового сообщения
        data = json.loads(message.text)

        # Запуск функции aggregate_data
        result = aggregate_data(data)

        # Отправка результата обратно пользователю
        send_message(message.chat.id, json.dumps(result))
    except Exception as e:
        # Если произошла ошибка, отправляем сообщение об ошибке
        send_message(message.chat.id, f"Error: {e}")


def connect_to_mongodb(database_name, collection_name):
    client = MongoClient('localhost', 27017)
    db = client[database_name]
    collection = db[collection_name]
    return collection


def read_bson_file(file_path):
    with open(file_path, 'rb') as file:
        data = file.read()
    return data


def aggregate_data(data):
    dt_from = datetime.strptime(data.get("dt_from"), "%Y-%m-%dT%H:%M:%S")
    dt_upto = datetime.strptime(data.get("dt_upto"), "%Y-%m-%dT%H:%M:%S")
    group_type = data.get("group_type")

    # Чтение данных из файла .bson
    bson_data = read_bson_file('sample_collection.bson')

    # Преобразование данных в DataFrame
    bson_dicts = bson.decode_all(bson_data)
    df = pd.DataFrame(bson_dicts)

    # Фильтрация данных по временному диапазону
    df = df[(df['dt'] >= pd.to_datetime(dt_from)) & (df['dt'] <= pd.to_datetime(dt_upto))]

    # Группировка данных
    if group_type == 'hour':
        df_grouped = df.groupby(df['dt'].dt.strftime('%Y-%m-%d %H'))['value'].sum().reset_index()
    elif group_type == 'day':
        df_grouped = df.groupby(df['dt'].dt.strftime('%Y-%m-%d'))['value'].sum().reset_index()
    elif group_type == 'month':
        df_grouped = df.groupby(df['dt'].dt.strftime('%Y-%m'))['value'].sum().reset_index()
    else:
        return "Invalid group_type. Please choose from 'hour', 'day', or 'month'."

    dataset = df_grouped['value'].tolist()
    labels = df_grouped['dt'].tolist()
    labels = [pd.to_datetime(label).strftime('%Y-%m-%dT%H:%M:%S') for label in labels]

    return {"dataset": dataset, "labels": labels}


if __name__ == "__main__":
    bot.polling(none_stop=True)
