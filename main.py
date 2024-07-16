from core.logging_config import setup_logging
from core.parser import read_data

logger = setup_logging()


def etl_process(file_paths):
    for file_path in file_paths:
        data = read_data(file_path)
        logger.info(f"ETL процесс завершен успешно для файла: {file_path}")
        # logger.info(f"{data}")
    return data


if __name__ == "__main__":
    csv_files = [
        "./Исходные_файлы/ft_balance_f.csv",
        "./Исходные_файлы/ft_posting_f.csv",
        "./Исходные_файлы/md_account_d.csv",
    ]
    etl_process(csv_files)
