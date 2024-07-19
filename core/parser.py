import pandas as pd

from .db_config import SCHEMA, engine
from .logging_config import logger


def read_data(file_path, encoding="utf-8", delimiter=";"):
    try:
        logger.info(f"Чтение данных из файла: {file_path}")
        data = pd.read_csv(
            file_path,
            parse_dates=True,
            delimiter=delimiter,
            encoding=encoding,
        )
        return data
    except Exception as err:
        logger.error(f"Ошибка при чтении данных из: {file_path}; {err}")


def clean_data(data):
    data.dropna(how="any", inplace=True)
    data.drop_duplicates(inplace=True)
    data.columns = data.columns.str.lower()
    for col in data.columns:
        if col == "currency_code":
            data["currency_code"] = data["currency_code"].astype(str).str[:3]
    return data


def load_to_db(data, table_name, engine):
    try:
        cleaned_data = clean_data(data)

        cleaned_data.to_sql(
            table_name,
            engine,
            schema=SCHEMA,
            if_exists="append",
            index=False,
        )
        # logger.info("Данные загружены.")

    except Exception as err:
        logger.error(
            (f"\nОшибка при загрузке данных в таблицу {table_name}: {err}\n")
        )


def export_f101_to_csv(csv_path):
    sql_query = """
        SELECT * FROM "DM".f101_round_f
    """
    try:
        df = pd.read_sql_query(sql_query, engine)
        df.to_csv(csv_path, index=False, encoding="utf-8-sig", sep=";")
        logger.info(f"Данные экспортированы в CSV файл: {csv_path}")
    except Exception as e:
        logger.error(f"Ошибка при экспорте данных в CSV: {e}")
