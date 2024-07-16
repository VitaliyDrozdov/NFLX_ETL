from sqlalchemy import (
    CHAR,
    Column,
    Date,
    Interval,
    Float,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    TIMESTAMP,
)
from sqlalchemy.ext.declarative import declarative_base

from .db_config import SCHEMA, LOG_SCHEMA
from .logging_config import logger

Base = declarative_base()
metadata = MetaData()


class FT_BALANCE_F(Base):
    __table_args__ = {"schema": SCHEMA}
    __tablename__ = "ft_balance_f"
    ON_DATE = Column(Date, primary_key=True, nullable=False)
    ACCOUNT_RK = Column(Numeric, primary_key=True, nullable=False)
    CURRENCY_RK = Column(Numeric)
    BALANCE_OUT = Column(Float)


FT_POSTING_F = Table(
    "ft_posting_f",
    metadata,
    Column("OPER_DATE", Date, nullable=False),
    Column("CREDIT_ACCOUNT_RK", Numeric, nullable=False),
    Column("DEBET_ACCOUNT_RK", Numeric, nullable=False),
    Column("CREDIT_AMOUNT", Float),
    Column("DEBET_AMOUNT", Float),
    schema=SCHEMA,
)


class MD_ACCOUNT_D(Base):
    __table_args__ = {"schema": SCHEMA}
    __tablename__ = "md_account_d"
    DATA_ACTUAL_DATE = Column(Date, primary_key=True, nullable=False)
    DATA_ACTUAL_END_DATE = Column(Date, nullable=False)
    ACCOUNT_RK = Column(Numeric, primary_key=True, nullable=False)
    ACCOUNT_NUMBER = Column(String(20), nullable=False)
    CHAR_TYPE = Column(CHAR(1), nullable=False)
    CURRENCY_RK = Column(Numeric, nullable=False)
    CURRENCY_CODE = Column(String(3), nullable=False)


class MD_CURRENCY_D(Base):
    __table_args__ = {"schema": SCHEMA}
    __tablename__ = "md_currency_d"
    CURRENCY_RK = Column(Numeric, primary_key=True, nullable=False)
    DATA_ACTUAL_DATE = Column(Date, primary_key=True, nullable=False)
    DATA_ACTUAL_END_DATE = Column(Date)
    CURRENCY_CODE = Column(String(3))
    CODE_ISO_CHAR = Column(String(3))


class MD_EXCHANGE_RATE_D(Base):
    __table_args__ = {"schema": SCHEMA}
    __tablename__ = "md_exchange_rate_d"
    DATA_ACTUAL_DATE = Column(Date, primary_key=True, nullable=False)
    DATA_ACTUAL_END_DATE = Column(Date)
    CURRENCY_RK = Column(Numeric, primary_key=True, nullable=False)
    REDUCED_COURCE = Column(Float)
    CODE_ISO_NUM = Column(String(3))


class MD_LEDGER_ACCOUNT_S(Base):
    __table_args__ = {"schema": SCHEMA}
    __tablename__ = "md_ledger_account_s"
    LEDGER_ACCOUNT = Column(Integer, primary_key=True, nullable=False)
    START_DATE = Column(Date, primary_key=True, nullable=False)
    CHAPTER = Column(CHAR(1))
    CHAPTER_NAME = Column(String(16))
    SECTION_NUMBER = Column(Integer)
    SECTION_NAME = Column(String(22))
    SUBSECTION_NAME = Column(String(21))
    LEDGER1_ACCOUNT = Column(Integer)
    LEDGER1_ACCOUNT_NAME = Column(String(47))
    LEDGER_ACCOUNT_NAME = Column(String(153))
    CHARACTERISTIC = Column(CHAR(1))
    IS_RESIDENT = Column(Integer)
    IS_RESERVE = Column(Integer)
    IS_RESERVED = Column(Integer)
    IS_LOAN = Column(Integer)
    IS_RESERVED_ASSETS = Column(Integer)
    IS_OVERDUE = Column(Integer)
    IS_INTEREST = Column(Integer)
    PAIR_ACCOUNT = Column(String(5))
    END_DATE = Column(Date)
    IS_RUB_ONLY = Column(Integer)
    MIN_TERM = Column(CHAR(1))
    MIN_TERM_MEASURE = Column(CHAR(1))
    MAX_TERM = Column(CHAR(1))
    MAX_TERM_MEASURE = Column(CHAR(1))
    LEDGER_ACC_FULL_NAME_TRANSLIT = Column(CHAR(1))
    IS_REVALUATION = Column(CHAR(1))
    IS_CORRECT = Column(CHAR(1))


class ETLLog(Base):
    __tablename__ = "etl_log"
    __table_args__ = {"schema": LOG_SCHEMA}

    id = Column(Integer, primary_key=True)
    table_name = Column(String(255), nullable=False)
    start_time = Column(TIMESTAMP, nullable=False)
    end_time = Column(TIMESTAMP, nullable=False)
    duration = Column(Interval)


def create_tables(engine):
    try:
        logger.info("Создание таблиц.")
        Base.metadata.create_all(engine, checkfirst=True)
        metadata.create_all(engine, checkfirst=True)
        logger.info("Таблицы созданы.")
    except Exception as err:
        logger.error(f"Ошибка при создании таблиц: {err}")
    return Base.metadata, metadata


def drop_table_if_exists(engine, table_name, schema=SCHEMA):
    try:
        metadata.reflect(bind=engine, schema=schema)
        logger.info(f"Удаление существующей таблицы: {table_name}")
        table = Table(
            table_name,
            metadata,
            autoload_with=engine,
            schema=schema,
        )
        table.drop(bind=engine)
        logger.info(f"Таблица '{table_name}' удалена.")
    except Exception as err:
        logger.error(f"\nОшибка при удалении таблицы: {err}\n")
