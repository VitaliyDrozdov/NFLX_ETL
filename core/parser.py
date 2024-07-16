import pandas as pd

from .logging_config import logger


def read_data(file_path, encoding="utf-8", delimiter=";"):
    try:
        logger.info(f"Чтение данных из файла: {file_path}")
        data = pd.read_csv(
            file_path, parse_dates=True, delimiter=delimiter, encoding=encoding
        )
        logger.info("Данные загружены.")
        # logger.info(f"\n{data}")
        return data
    except Exception as err:
        logger.error(f"Ошибка при чтении данных из файла: {file_path}; {err}")


def load_to_db(data, table_name, engine):
    try:
        logger.info(f"Начало загрузки данных в таблицу: {table_name}.")

        data.to_sql(table_name, engine, schema="DS", if_exists="replace")
        logger.info("Данные загружены.")
    except Exception as err:
        logger.error(
            (f"Ошибка при загрузке данных в таблицу {table_name}: {err}")
        )
