import logging
import os
import sqlalchemy as sql


def get_logger(logger_name: str) -> logging.Logger:
    logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    return logger


LOG = get_logger("reference_data")


def get_db_connection(local: bool) -> sql.Engine:
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")
    db_name = os.getenv("LOCAL_DB_NAME" if local else "DB_NAME")
    host = os.getenv("LOCAL_DB_HOST" if local else "DB_HOST")
    port = os.getenv("DB_PORT")
    dsn = f"postgresql://{user}:{pwd}@{host}:{port}/{db_name}"
    return sql.create_engine(dsn)


def execute_query(db: sql.Engine, query: str):
    with db.connect() as con:
        transaction = con.begin()
        try:
            con.execute(sql.text(query))
            transaction.commit()
        except:
            transaction.rollback()
