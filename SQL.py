# -*- coding: utf-8 -*-
"""
Created on Sun Jun  3 09:23:30 2018

@author: Shabaka
"""

import pyodbc
import odbc

#%%

source = odbc.SQLDataSources(odbc.SQL_FETCH_FIRST)
while source:
    print(source)
    source = odbc.SQLDataSources(odbc.SQL_FETCH_NEXT)


#%%

pyodbc.connect(‘DRIVER={SQL Server};
               SERVER=server name;
               DATABASE=database name;
               UID=user_name;
               PWD=password’)
# ############ POSTGRESQL  ################

SELECT * FROM films WHERE release_year = 2016;

SELECT COUNT(*) FROM films WHERE release_year < 2000;

SELECT title, release_year FROM films WHERE release_year > 2000

#%%

# ######## Basic Filetering of Text Values #######

SELECT * FROM films WHERE language = 'French';

SELECT name, birthdate FROM people WHERE birthdate = '1974-11-11';

SELECT COUNT(*) FROM films WHERE language = 'Hindi';

SELECT * FROM films WHERE certification = 'R';


#%%

# ######## Using the WHERE & AND keywords ###########

SELECT title, release_year
FROM films
WHERE language = 'Spanish'
AND release_year < 2000;

SELECT * FROM films WHERE language = 'Spanish'
AND release_year > 2000 AND release_year < 2010;

#%%

# ### WHERE, AND, OR, Operators  ##########

SELECT title
FROM films
WHERE release_year = 1994
OR release_year = 2000;

SELECT title
FROM films
WHERE (release_year = 1994 OR release_year = 1995)
AND (certification = 'PG' OR certification = 'R');

# the following query selects all films that were released in 1994 or 1995
# which had a rating of PG or R.

SELECT title
FROM films
WHERE (release_year = 1994 OR release_year = 1995)
AND (certification = 'PG' OR certification = 'R');


# a query to get the title and release year of films released in
# the 90s which were in French or Spanish and which took in more
# than $2M gross

SELECT title, release_year FROM films WHERE release_year > 1989
AND release_year < 2000;

# filter records to only include French or Spanish language films

SELECT title, release_year FROM films WHERE
(release_year >= 1990 AND release_year < 2000) AND
(language = 'French' OR language = 'Spanish') AND gross > 2000000;

#%%

# #### The BETWEEN keyword ##########

SELECT title
FROM films
WHERE release_year >= 1994
AND release_year <= 2000;

# Same as

SELECT title
FROM films
WHERE release_year
BETWEEN 1994 AND 2000;

# get the title and release year of all Spanish language films
# released between 1990 and 2000 (inclusive) with budgets
# over $100 million.

SELECT title, release_year
FROM films
WHERE release_year BETWEEN 1990 AND 2000
AND budget > 100000000;

SELECT title, release_year FROM films
WHERE (language = 'Spanish' OR language = 'French')
AND release_year BETWEEN 1990 AND 2000 AND budget > 100000000;

#%%

# ###### Using the WHERE & IN Keywords  #############

SELECT title, release_year FROM films WHERE release_year IN (1990, 2000) AND duration > 120;

SELECT title, language FROM films WHERE language IN ('English', 'Spanish', 'French');

SELECT title, certification FROM films WHERE certification IN ('NC-17', 'R');