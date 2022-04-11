

### Big Bang Theory dataset - SQL skills assessment

Original Dataset: https://www.kaggle.com/datasets/bcruise/big-bang-theory-episodes

Data was scraped from IMDB and Wikipedia, and contains both categoric and continuous variables.


#### Task Requirements

- Python3 and SQLAlchemy must be used.
- A relational SQL database must be used, such as MySQL, MariaDB, Postgres, SQLite. Anything not Document based.
- GitHub (or similar) should be used to share your solution.

#### Task Objectives

- Prepare the IMDB and Wiki sourced csvs into a suitable relational database schema, and populate the tables.
- Categoric variables should be prepared to a good degree of Normal Form to demonstrate good database design practice (Eg, in this case I expect tables for the following to be present: imdb sourced episode, wiki sourced episode, and separate table(s) for the directors/writers/teleplay coordinators. Since some relationships are many-to-many you will also need some association tables).
- Indexes should also be used where appropriate. Choosing the most appropriate data types for each column is also advised.
- NOTE: in the source data, the writers ("written by:" and teleplay coordinators "teleplay by:") should be split into individual names and stored in the database as separate entities. Eg, a Wiki sourced episode record should have relationships for 1 director, many writers and many teleplay coordinaters.
- Once the database is completed, write a query in Sqlalchemy to get the average IMDB rating for each director, writer, and teleplay coordinator sorted descending by rating.
- Using Sqlalchemy, write a query to return all IMDB records where the descriptions mentions "Amy" anywhere.

#### Notes on Scoring

Given the open ended nature of the problem, and programming in general, an exact scoring system isn't defined but will be based on clean and clear code following closely (but not necessarily exactly to) the python PEP standards. Python code should be well structured in a typical python package format, such as described [here](https://packaging.python.org/en/latest/tutorials/packaging-projects/). You can name the directories whatever you like as long as it's logical, I don't expect you to use pip setuptools or anything to install the package into the pip environment; running it from the source code is fine. Good practice SQL database design is also of high importance.

You are expected to finish the tasks within 2 days. If you are the sole author of a different project which uses all of the skills above, you can use that as your assessment instead. During the interview I would like you to talk through all of your database and code design choices on a screenshare, and execute some functions/queries to show that it works.




### Solution:
prepared by Gazi Hasan Rahman


#### How to run

The solution is located in the src folder under the file name solution.py

If we are running this for the first time:
- Provide database details in the db_config.py file
- Run psql.py to create the database
- Run model.py to create the schema
