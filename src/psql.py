from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database
from db_config import postgresql as settings


def get_database():
    try:
        engine = get_engine_from_settings()
    except IOError:
        return None, 'fail'
    return engine


def get_engine_from_settings():
    keys = ['pguser','pgpasswd','pghost','pgport','pgdb']
    if not all(key in keys for key in settings.keys()):
        raise Exception('Bad config file')

    return get_engine(settings['pguser'],
                      settings['pgpasswd'],
                      settings['pghost'],
                      settings['pgport'],
                      settings['pgdb'])


def get_engine(user, passwd, host, port, db):
    url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
        user=user, passwd=passwd, host=host, port=port, db=db)
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url, pool_size=50, echo=False)
    return engine


def get_session():
    engine = get_database()
    session = sessionmaker(bind=engine)()
    #session = Session()
    return session


db = get_database()
session = get_session()
Base = declarative_base()