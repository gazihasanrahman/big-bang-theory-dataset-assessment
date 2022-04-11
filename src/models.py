
from sqlalchemy import Column, Date, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from psql import Base, db, session


class ImdbSourcedEpisode(Base):
    __tablename__ = 'imdb_sourced_episode'
    season_id = Column(String(8), primary_key=True)
    season = Column(Integer, nullable=False)
    episode_num = Column(Integer, nullable=False)
    title = Column(String(200), nullable=False)
    original_air_date = Column(Date, nullable=False)
    imdb_rating = Column(Float(3), nullable=False)
    total_votes = Column(Integer, nullable=False)
    description = Column(String(1000), nullable=True)


class WikiSourcedEpisode(Base):
    __tablename__ = 'wiki_sourced_episode'
    season_id = Column(String(8), primary_key=True)
    season = Column(Integer, nullable=False)
    episode_num_in_season = Column(Integer, nullable=False)
    episode_num_overall = Column(Integer, nullable=False)
    title = Column(String(200), nullable=False)
    original_air_date = Column(Date, nullable=False)
    prod_code = Column(String(20), nullable=True)
    us_viewers = Column(Integer, nullable=True)


class Directors(Base):
    __tablename__ = 'directors'
    id = Column(Integer, primary_key=True, autoincrement=True)
    season_id = Column(String(8), nullable=False)
    directed_by = Column(String(200), nullable=False)


class Writers(Base):
    __tablename__ = 'writers'
    id = Column(Integer, primary_key=True, autoincrement=True)
    season_id = Column(String(8), nullable=False)
    written_by = Column(String(200), nullable=False)


class TeleplayCoordinators(Base):
    __tablename__ = 'teleplay_coordinators'
    id = Column(Integer, primary_key=True, autoincrement=True)
    season_id = Column(String(8), nullable=False)
    teleplay_by = Column(String(200), nullable=False)


def create():
    Base.metadata.create_all(db)

create()