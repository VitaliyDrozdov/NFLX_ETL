import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

from core.parser import load_to_db, read_data

load_dotenv()

CSVPATH = os.getenv("CSVPATH")

DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ENGINE_PATH = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

pk_mapping = {
    "ft_balance_f.csv": ["ON_DATE", "ACCOUNT_RK"],
    "ft_posting_f.csv": [],
    "md_account_d.csv": ["DATA_ACTUAL_DATE", "ACCOUNT_RK"],
    "md_currency_d.csv": ["CURRENCY_RK", "DATA_ACTUAL_DATE"],
    "md_exchange_rate_d.csv": ["DATA_ACTUAL_DATE", "CURRENCY_RK"],
    "md_ledger_account_s.csv": ["LEDGER_ACCOUNT", "START_DATE"],
}


def main(file_paths):
    """Основной ETL процесс."""
    engine = create_engine(ENGINE_PATH)
    for filename in file_paths:
        filepath = os.path.join(CSVPATH, filename)
        encoding = "utf-8"
        # для этого файла стандартная utf-8 не подходит
        if "md_currency_d.csv" in filename:
            encoding = "cp1252"
        data = read_data(filepath, encoding=encoding)
    for filename in file_paths:
        table_name = filename.split(".")[0]
        pk = pk_mapping.get(filename, [])
        load_to_db(data, table_name, engine, pk=pk)


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
