import os
import csv
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, MetaData, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError

def create_table_classes(db_str):
    """
    Динамически создает классы для всех таблиц имеющихся в БД.
    Возвращает словарь с именами таблиц и соответствующими классами
    """
    try:
        engine = create_engine(db_str)
        # Проверяем соединение с базой данных
        with engine.connect() as connection:
            print("Соединение с базой данных успешно установлено.")

        # Создаем объект MetaData и отражаем таблицы
        metadata = MetaData()
        metadata.reflect(bind=engine)

        # Создаем базовый класс для моделей
        Base = declarative_base()

        # Динамически создаем классы для всех таблиц
        table_classes = {}
        for table_name, table in metadata.tables.items():
            # Создание класса модели
            model_class = type(
                table_name.capitalize(),  # Имя класса
                (Base,),  # Базовый класс
                {'__table__': table}  # Атрибуты класса
            )
            table_classes[table_name] = model_class
            # вывод в консоль созданных классов
            print(f'Класс для таблицы {table_name} создан')

        return table_classes

    except SQLAlchemyError as e:
        print(f"Ошибка при работе с базой данных: {e}")
        return None, None

def execute_query_and_save(query_name, db_str):
    '''
    Принимает имя запроса хранящегося в queries.txt и строку подключения.
    сохраняет полученные данные в файл и возвращает датафрэйм пандас
    '''
    # Проверка на наличие файла queries.txt
    if not os.path.exists('queries.txt'):
        raise FileNotFoundError("Файла 'queries.txt' нет в указанной директории")
    
    # Чтение запроса из файла
    with open('queries.txt', 'r') as file:
        queries = {}
        current_query = None
        for line in file:
            line = line.strip()
            if line.endswith(':'):  # Название запроса
                current_query = line[:-1]
                queries[current_query] = ''
            elif current_query:  # Добавляем строки к текущему запросу
                queries[current_query] += line + ' '
    
    # Проверяем, существует ли запрос
    if query_name not in queries:
        raise ValueError(f"Запрос '{query_name}' не найден в файле: 'queries.txt'")
    
    sql_query = text(queries[query_name].strip())
    # Создание движка
    engine = create_engine(db_str)
    # Используем контекстный соединения
    with engine.connect() as connection:
        result = connection.execute(sql_query)
        # Преобразуем результат в DataFrame
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    # Создаем папку output_data, если она не существует
    os.makedirs('output_data', exist_ok=True)
    
    # Сохраняем данные в CSV
    output_file = os.path.join('output_data', f'{query_name}.csv')
    df.to_csv(output_file, index=False)
    print(f'Данные сохранены в output_data/{query_name}.csv')
    
    return df

def numeric_analysis(dataframe):
    '''Функция для анализа числовых переменных'''
    numeric_df = dataframe.select_dtypes(include=[np.number])  # Выбор числовых колонок
    numeric_summary = []

    for column in numeric_df.columns:
        stats = {
            'Параметр': column,
            'Доля пропусков': numeric_df[column].isnull().mean(),
            'Максимальное значение': numeric_df[column].max(),
            'Минимальное значение': numeric_df[column].min(),
            'Среднее значение': numeric_df[column].mean(),
            'Медиана': numeric_df[column].median(),
            'Дисперсия': numeric_df[column].var(),
            'Квантиль 0.1': numeric_df[column].quantile(0.1),
            'Квантиль 0.9': numeric_df[column].quantile(0.9),
            'Квартиль 1': numeric_df[column].quantile(0.25),
            'Квартиль 3': numeric_df[column].quantile(0.75)
        }
        numeric_summary.append(stats)

    return pd.DataFrame(numeric_summary)

def categorical_analysis(dataframe):
    '''Функция для анализа категориальных переменных'''
    categorical_df = dataframe.select_dtypes(exclude=[np.number])  # Выбор категориальных колонок
    categorical_summary = []

    for column in categorical_df.columns:
        stats = {
            'Параметр': column,
            'Доля пропусков': categorical_df[column].isnull().mean(),
            'Количество уникальных значений': categorical_df[column].nunique(),
            'Мода': categorical_df[column].mode()[0] if not categorical_df[column].mode().empty else None
        }
        categorical_summary.append(stats)

    return pd.DataFrame(categorical_summary)