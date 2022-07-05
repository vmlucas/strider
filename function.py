import pandas as pd
from pandas import DataFrame 
import json
import psycopg2
from sqlalchemy import create_engine
import numpy
from psycopg2.extensions import register_adapter, AsIs
import datetime 

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)

#connect to the PostgreSQL on Heroku
def getEngine():
    engine = create_engine('postgresql+psycopg2://postgres:vsvLL430@localhost/postgres')
    return engine

#load internal data fazer parte q verifica arquivo na pasta, e copia para outra ao carregar
def loadInternalData():
    loadUsers()
    loadMovies()
    loadStreams()

#load external data
def loadExternalData():
    loadAuthors()
    loadBooks()
    
#load books
def loadBooks():
    engine = getEngine()
    conn = engine.connect()
    trans = conn.begin()

    print('loading books...')
    booksDF = pd.read_json('./to_load/books.json')
    booksDF.to_sql(name='books_tmp',con=engine,schema='strider', if_exists='replace',index=False)
    engine.execute('CREATE TABLE IF NOT EXISTS \
                   strider.books(name text PRIMARY KEY, pages int, author text, publisher text )')
    
    try:
        engine.execute('delete from strider.books \
                          where name in (select name from strider.books_tmp)') 
        trans.commit()
        booksDF.to_sql(name='books',con=engine,schema='strider', if_exists='append',index=False)
        print('Books loaded')
    except:
        trans.rollback()
        raise
    finally:
        trans.close()
        conn.close()
        engine.dispose()

#load Authors
def loadAuthors():
    engine = getEngine()
    conn = engine.connect()
    trans = conn.begin()
    
    print('loading authors...')
    engine.execute('CREATE TABLE IF NOT EXISTS \
                   strider.authors(name text PRIMARY KEY, birth_date text, died_at text)')
    engine.execute('CREATE TABLE IF NOT EXISTS \
                   strider.nationalities(id text,label text, slug text, author text,\
                    FOREIGN KEY (author) REFERENCES strider.authors(name))')               
    
    autsDF = pd.read_json('./to_load/authors.json')
    for index,row in autsDF.iterrows():
            autDF = pd.json_normalize(autsDF['metadata'][index])
            if(pd.notnull(autDF['name']).any()):
               autDF.to_sql(name='authors_tmp',con=engine,schema='strider', if_exists='append',index=False)
               natsDF =  pd.json_normalize(autsDF['nationalities'][index])
               natsDF = natsDF.drop(natsDF[(natsDF.label == 'None') | (natsDF.label =='')].index)
               natsDF = natsDF.assign(author= autDF['name'][0]) 
               natsDF.to_sql(name='nationalities_tmp',con=engine,schema='strider', if_exists='append',index=False)
               
    try:
        engine.execute('delete from strider.nationalities \
                          where author in (select distinct author from strider.nationalities_tmp)') 
        engine.execute('delete from strider.authors \
                          where name in (select name from strider.authors_tmp)') 
        
        engine.execute('insert into strider.authors \
                          select * from strider.authors_tmp') 
        engine.execute('insert into strider.nationalities \
                          select * from strider.nationalities_tmp') 

        engine.execute('delete from strider.authors_tmp')
        engine.execute('delete from strider.nationalities_tmp')          

        trans.commit()
        print('Authors loaded')
    except:
        trans.rollback()
        raise
    finally:
        trans.close()
        conn.close()
        engine.dispose()


#loading users
def loadUsers():
    engine = getEngine()
       
    print('loading users...')
    usersDF = pd.read_csv('./to_load/users.csv')
    usersDF.to_sql(name='users',con=engine,schema='strider', if_exists='replace',index=False)
    
    #ADD PRIMARY KEY
    engine.execute('ALTER TABLE strider.users ADD PRIMARY KEY (email)')
    
    print('users loaded!!')    
    engine.dispose()


#loading movies
def loadMovies():
    engine = getEngine()
    
    print('loading movies...')
    moviesDF = pd.read_csv('./to_load/movies.csv')
    moviesDF.to_sql(name='movies',con=engine,schema='strider', if_exists='replace',index=False)
    
    #ADD PRIMARY KEY
    engine.execute('ALTER TABLE strider.movies ADD PRIMARY KEY (title)')
    
    print('movies loaded!!')    
    engine.dispose()    


#loading streaming    
def loadStreams():
    engine = getEngine()
    
    print('loading streamings...')
    moviesDF = pd.read_csv('./to_load/streams.csv')
    moviesDF.to_sql(name='streams',con=engine,schema='strider', if_exists='append',index=False)
    
    #ADD PRIMARY KEY
    #engine.execute('ALTER TABLE strider.streams ADD PRIMARY KEY (start_at)')
    engine.execute('ALTER TABLE strider.streams \
        ADD CONSTRAINT fk_email_users FOREIGN KEY (user_email) REFERENCES strider.users(email)')
    
    print('streamings loaded!!')    
    engine.dispose()        