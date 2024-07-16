import pandas as pd

from .logging_config import setup_logging

# import os
# from dotenv import load_dotenv

# load_dotenv()

logger = setup_logging()


def read_data(file_path):
    try:
        logger.info(f"Чтение данных из файла: {file_path}")
        data = pd.read_csv(file_path, parse_dates=True)
        logger.info("Данные загружены")
        return data
    except Exception as err:
        logger.error(f"Ошибка при чтении данных из файла {file_path}: {err}")
