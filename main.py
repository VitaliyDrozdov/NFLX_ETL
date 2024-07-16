import os

from dotenv import load_dotenv

from core.create_tables import create_tables
from core.db_config import engine
from core.parser import load_to_db, read_data

load_dotenv()

CSVPATH = os.getenv("CSVPATH")


def main(file_paths):
    """Основной ETL процесс."""
    create_tables(engine)
    for filename in file_paths:
        filepath = os.path.join(CSVPATH, filename)
        encoding = "utf-8"
        # для этого файла стандартная utf-8 не подходит
        if "md_currency_d.csv" in filename:
            encoding = "cp1252"
        data = read_data(filepath, encoding=encoding)
        table_name = filename.split(".")[0]
        load_to_db(data, table_name, engine)


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
