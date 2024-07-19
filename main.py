import os

from dotenv import load_dotenv

from core.manage_tables import (
    create_tables,
    drop_table_if_exists,
)
from core.db_config import engine
from core.parser import load_to_db, read_data
from core.runtime import log_execution

load_dotenv()

CSVPATH = os.getenv("CSVPATH")


@log_execution
def proccess(filename):
    table_name = filename.split(".")[0]
    drop_table_if_exists(engine, table_name)
    create_tables(engine)
    filepath = os.path.join(CSVPATH, filename)
    encoding = "utf-8"
    # для этого файла стандартная utf-8 не подходит
    if "md_currency_d.csv" in filename:
        encoding = "cp1252"
    data = read_data(filepath, encoding=encoding)
    load_to_db(data, table_name, engine, schema="DM")


def main(filenames):
    """Основной ETL процесс."""
    for filename in filenames:
        proccess(filename)
    engine.dispose()


if __name__ == "__main__":
    csv_files = [
        "ft_balance_f.csv",
        "ft_posting_f.csv",
        "md_account_d.csv",
        "md_currency_d.csv",
        "md_exchange_rate_d.csv",
        "md_ledger_account_s.csv",
    ]
    main(csv_files)
