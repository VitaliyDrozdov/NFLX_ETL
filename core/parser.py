import pandas as pd
from sqlalchemy import inspect

from .db_config import SCHEMA
from .logging_config import logger


def read_data(file_path, encoding="utf-8", delimiter=";"):
    try:
        logger.info(f"Чтение данных из файла: {file_path}")
        data = pd.read_csv(
            file_path, parse_dates=True, delimiter=delimiter, encoding=encoding
        )
        logger.info("Данные получены.")
        # logger.info(f"\n{data}")
        return data
    except Exception as err:
        logger.error(f"Ошибка при чтении данных из: {file_path}; {err}")


def clean_data(data):
    data.dropna(how="any", inplace=True)
    data.drop_duplicates(inplace=True)
    if "CURRENCY_CODE" in data.columns:
        data["CURRENCY_CODE"] = data["CURRENCY_CODE"].str[:3]
    return data


def load_to_db(data, table_name, engine):
    try:
        inspector = inspect(engine)
        logger.info(f"Загрузка данных в таблицу: {table_name}.")
        columns_in_db = [
            column["name"]
            for column in inspector.get_columns(table_name, schema=SCHEMA)
        ]
        logger.info(f"Колонки в БД: {columns_in_db}")
        data_columns = [col for col in data.columns if col in columns_in_db]
        # logger.info(f"Колонки в DF: {data_columns}")
        cleaned_data = clean_data(data[data_columns])
        logger.info(f"Cleaned data:\n {cleaned_data.dtypes}")

        cleaned_data.to_sql(
            table_name, engine, schema=SCHEMA, if_exists="append", index=False
        )
        logger.info("Данные загружены.")
    except Exception as err:
        logger.error(
            (f"Ошибка при загрузке данных в таблицу {table_name}: {err}")
        )
