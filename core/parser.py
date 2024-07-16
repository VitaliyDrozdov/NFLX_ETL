import pandas as pd

from .logging_config import logger

# from sqlalchemy import Column, Date, Integer, MetaData, String, Table


def read_data(file_path, encoding="utf-8", delimiter=";"):
    try:
        logger.info(f"Чтение данных из файла: {file_path}")
        data = pd.read_csv(
            file_path, parse_dates=True, delimiter=delimiter, encoding=encoding
        )
        logger.info("Данные получены.")
        logger.info(f"\n{data}")
        return data
    except Exception as err:
        logger.error(f"Ошибка при чтении данных из: {file_path}; {err}")


def load_to_db(data, table_name, engine, pk=None):
    try:
        logger.info(f"Загрузка данных в таблицу: {table_name}.")
        if pk:
            data.set_index(pk, inplace=True)
        data.to_sql(
            table_name, engine, schema="DS", if_exists="replace", index=False
        )
        logger.info("Данные загружены.")
    except Exception as err:
        logger.error(
            (f"Ошибка при загрузке данных в таблицу {table_name}: {err}")
        )
