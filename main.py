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


def main(file_paths):
    engine = create_engine(ENGINE_PATH)
    for filename in file_paths:
        filepath = os.path.join(CSVPATH, filename)
        data = read_data(filepath)
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
