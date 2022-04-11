# import the necessary packages
import pandas as pd
import numpy as np
from ast import literal_eval
from sqlalchemy import create_engine, func
from db_config import postgresql as settings
from psql import db, session
from models import ImdbSourcedEpisode, WikiSourcedEpisode, Directors, Writers, TeleplayCoordinators



### Data Preprocessing:


# import big_bang_theory_imdb dataset
bbt_imdb_df = pd.read_csv('../data/big_bang_theory_imdb.csv')

# convert the original air date column to datetime format
bbt_imdb_df.original_air_date = pd.to_datetime(bbt_imdb_df.original_air_date)

# create a new column season_id by concatenating season and episode_num
bbt_imdb_df['season_id'] = 'S' + bbt_imdb_df.season.astype(str) + 'E' + bbt_imdb_df.episode_num.astype(str)

# import big_bang_theory_episodes dataset
bbt_episodes_df = pd.read_csv('../data/big_bang_theory_episodes.csv')

# manual intervention to fix episode identification mismatch
"""
There are 2 datasets provided in the task. big_bang_theory_imdb & big_bang_theory_episodes.
The first dataset has one episode more which is 'Unaired Pilot'. For this extra episode, the identification between the two datasets is not matching with each other.
For example, the episode "The Fuzzy Boots Corollary" is identified as season 1 episode 4 in the first dataset and as season 1 episode 3 in the second dataset.
To fix this mistake, we are adding 'Unaired Pilot' in the second dataset. If it is not a mistake, then we can skip next 4 lines of code.
"""
# increment episode_num_in_season by 1 for season 1
bbt_episodes_df.loc[bbt_episodes_df.season == 1, 'episode_num_in_season'] = bbt_episodes_df.loc[bbt_episodes_df.season == 1, 'episode_num_in_season'] + 1

# insert the row for 'Unaired Pilot' episode
bbt_episodes_df.loc[-1] = [1, 1, np.NAN ,'Unaired Pilot',np.NAN,np.NAN,'01-May-06',np.NAN,np.NAN]

# sort by index
bbt_episodes_df = bbt_episodes_df.sort_index()

# reset the index of the dataframe
bbt_episodes_df = bbt_episodes_df.reset_index(drop=True)

# correct the serial of episode_num_overall
bbt_episodes_df['episode_num_overall'] = bbt_episodes_df.index + 1

# convert the original air date column to datetime format
bbt_episodes_df.original_air_date = pd.to_datetime(bbt_episodes_df.original_air_date)

# create a new column season_id by concatenating season and episode_num
bbt_episodes_df['season_id'] = 'S' + bbt_episodes_df.season.astype(str) + 'E' + bbt_episodes_df.episode_num_in_season.astype(str)

# split the column written_by into two columns using 'Teleplay by: ' as the delimiter and expand is set to True
bbt_episodes_df[['written_by', 'teleplay_by']] = bbt_episodes_df['written_by'].str.split('Teleplay by: ', expand=True)

# remove the word 'Story by: ' from the written_by column
bbt_episodes_df.written_by = bbt_episodes_df.written_by.str.replace('Story by: ', '')

# create a dataframe for directors
bbt_director_df = bbt_episodes_df[['season_id','directed_by']].dropna()
bbt_director_df = bbt_director_df.reset_index(drop=True)

# create a dataframe for writers and stack the writer's names into a single column
bbt_writers_df = bbt_episodes_df[['season_id','written_by']].dropna()
bbt_writers_df.written_by = bbt_writers_df.written_by.str.replace(" & ", "', '")
bbt_writers_df['written_by'] = "['" + bbt_writers_df.written_by.astype(str) + "']"
bbt_writers_df['written_by'] = bbt_writers_df['written_by'].apply(literal_eval)
bbt_writers_df = bbt_writers_df.explode('written_by')
bbt_writers_df = bbt_writers_df.reset_index(drop=True)

# create a dataframe for teleplay_coordinators and stack the coordinator's names into a single column
bbt_coordinator_df = bbt_episodes_df[['season_id','teleplay_by']].dropna()
bbt_coordinator_df.teleplay_by = bbt_coordinator_df.teleplay_by.str.replace(" & ", ", ")
bbt_coordinator_df.teleplay_by = bbt_coordinator_df.teleplay_by.str.replace(", ", "', '")
bbt_coordinator_df['teleplay_by'] = "['" + bbt_coordinator_df.teleplay_by.astype(str) + "']"
bbt_coordinator_df['teleplay_by'] = bbt_coordinator_df['teleplay_by'].apply(literal_eval)
bbt_coordinator_df = bbt_coordinator_df.explode('teleplay_by')
bbt_coordinator_df = bbt_coordinator_df.reset_index(drop=True)

# final dataframe wiki sourced episodes
bbt_episodes_df = bbt_episodes_df[['season_id', 'season','episode_num_in_season', 'episode_num_overall', 'title', 'original_air_date', 'prod_code', 'us_viewers']]

# final dataframe imdb sourced episode
bbt_imdb_df = bbt_imdb_df[['season_id', 'season','episode_num', 'title', 'original_air_date', 'imdb_rating', 'total_votes', 'desc']]
# change column name desc to description
bbt_imdb_df.rename(columns={'desc':'description'}, inplace=True)




### Create database schema using SQLAlchemy
"""
If we are using this for the first time:
1. provide database details in the db_config.py file
2. run psql.py to create the database
3. run model.py to create the schema
"""



### Upload the data to the database:

# create the database engine
url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
    user=settings['pguser'], 
    passwd=settings['pgpasswd'], 
    host=settings['pghost'], 
    port=settings['pgport'], 
    db=settings['pgdb'])
engine = create_engine(url, pool_size=50, echo=False)

# populate the imdb_sourced_episode table
bbt_imdb_df.to_sql(
    name='imdb_sourced_episode',
    con=engine,
    if_exists='append',
    index=False)

# populate the wiki_sourced_episode table
bbt_episodes_df.to_sql(
    name='wiki_sourced_episode',
    con=engine,
    if_exists='append',
    index=False)

# populate the directors table
bbt_director_df.to_sql(
    name='directors',
    con=engine,
    if_exists='append',
    index=False)

# populate the writers table
bbt_writers_df.to_sql(
    name='writers',
    con=engine,
    if_exists='append',
    index=False)

# populate the teleplay_coordinators table
bbt_coordinator_df.to_sql(
    name='teleplay_coordinators',
    con=engine,
    if_exists='append',
    index=False)




### Queries to get the results:


# SQLAlchemy query to get average IMDB rating for each director
query_director = session.query(Directors.directed_by, func.avg(ImdbSourcedEpisode.imdb_rating)).filter(ImdbSourcedEpisode.season_id == Directors.season_id).group_by(Directors.directed_by).order_by(func.avg(ImdbSourcedEpisode.imdb_rating).desc())
raing_director = pd.read_sql(query_director.statement, query_director.session.bind)
# export the result to a csv file
raing_director.to_csv('Rating - Directors.csv', index=False)


# SQLAlchemy query to get average IMDB rating for each writer
query_writer = session.query(Writers.written_by, func.avg(ImdbSourcedEpisode.imdb_rating)).filter(ImdbSourcedEpisode.season_id == Writers.season_id).group_by(Writers.written_by).order_by(func.avg(ImdbSourcedEpisode.imdb_rating).desc())
raing_writer = pd.read_sql(query_writer.statement, query_writer.session.bind)
# export the result to a csv file
raing_writer.to_csv('Rating - Writers.csv', index=False)


# SQLAlchemy query to get average IMDB rating for each teleplay coordinator
query_coordinator = session.query(TeleplayCoordinators.teleplay_by, func.avg(ImdbSourcedEpisode.imdb_rating)).filter(ImdbSourcedEpisode.season_id == TeleplayCoordinators.season_id).group_by(TeleplayCoordinators.teleplay_by).order_by(func.avg(ImdbSourcedEpisode.imdb_rating).desc())
raing_coordinator = pd.read_sql(query_coordinator.statement, query_coordinator.session.bind)
# export the result to a csv file
raing_coordinator.to_csv('Rating - Teleplay coordinator.csv', index=False)


# SQLAlchemy query to return all IMDB records containing Amy in the description
query = session.query(ImdbSourcedEpisode).filter(ImdbSourcedEpisode.description.ilike('%' + 'Amy' + '%'))
amy_result = pd.read_sql(query.statement, query.session.bind)
# export the result to a csv file
amy_result.to_csv('Result - Amy in description.csv', index=False)

# print the result in the console
for amy in session.query(ImdbSourcedEpisode).filter(ImdbSourcedEpisode.description.ilike('%' + 'Amy' + '%')):
     print (amy.season_id, '-' , amy.title)

