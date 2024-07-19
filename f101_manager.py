import os
from dotenv import load_dotenv

from core.db_config import engine
from core.parser import export_to_csv
from core.runtime import log_execution

load_dotenv()

CSVPATH_F101 = os.getenv("CSVPATH_F101")
EXPORT_TABLE_NAME = os.getenv("EXPORT_TABLE_NAME")


@log_execution
def proccess(filename):
    table_name = filename.split(".")[0]
    path = f"{CSVPATH_F101}/{EXPORT_TABLE_NAME}"
    export_to_csv(
        table_name=table_name,
        csv_path=path,
        engine=engine,
    )
    # Для загрузки:
    # filepath = os.path.join(CSVPATH_F101, filename)
    # data = read_data(filepath)
    # load_to_db(data, table_name, engine, schema="DM",
    # clean=True, dropna=False)


def main():
    """Выгрузка данных их таблицы в csv файл."""
    proccess(EXPORT_TABLE_NAME)
    engine.dispose()


if __name__ == "__main__":
    main()