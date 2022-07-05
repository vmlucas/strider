import pandas as pd
from pandas import DataFrame 
import json
import psycopg2
from sqlalchemy import create_engine,func
from sqlalchemy.orm import Session
import numpy
import datetime 
from psycopg2.extensions import register_adapter, AsIs
from function import getEngine

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)

#load Reviews
def loadReviews():
    engine = getEngine()
    conn = engine.connect()
    trans = conn.begin()
    
    print('loading reviews...')
    engine.execute('CREATE TABLE IF NOT EXISTS \
                   strider.reviews(id text PRIMARY KEY, text text, rate int8, label text, updated text, created text)')
    engine.execute('CREATE TABLE IF NOT EXISTS \
                   strider.reviews_books(id text,title text, pages text, review_id text,\
                    FOREIGN KEY (review_id) REFERENCES strider.reviews(id))') 
    
    rwsDF = pd.read_json('./to_load/reviews.json')
    for index,row in rwsDF.iterrows():
       df1 = pd.json_normalize(rwsDF['content'][index])
       rwDF = pd.DataFrame()
       rwDF['text'] = df1['text'] 
       df2 = pd.json_normalize(rwsDF['rating'][index])
       rwDF['rate']= df2['rate']
       rwDF['label'] = df2['label']
       rwDF['updated'] = rwsDF['updated'][index]
       rwDF['created'] = rwsDF['created'][index]        
       rwDF['id'] = str(datetime.datetime.now())
       #books array
       booksDF = pd.json_normalize(rwsDF['books'][index])
       for j,r in booksDF.iterrows():
            bookDF = pd.DataFrame()
            bookDF['id'] = booksDF['id']
            bookDF['title'] = booksDF['metadata.title']
            bookDF['pages'] = booksDF['metadata.pages']
            bookDF['review_id'] = rwDF['id']
            bookDF.to_sql(name='reviews_books_tmp',con=engine,schema='strider',if_exists='append',index=False)
       rwDF.to_sql(name='reviews_tmp',con=engine,schema='strider', if_exists='append',index=False)

    try:
        engine.execute('delete from strider.reviews_books \
                          where review_id in (select distinct review_id from strider.reviews_books_tmp)') 
        engine.execute('delete from strider.reviews \
                          where id in (select id from strider.reviews_tmp)') 
        
        engine.execute('insert into strider.reviews(id, text, rate, label, updated, created) \
                          select id, text, rate, label, updated, created from strider.reviews_tmp') 
        engine.execute('insert into strider.reviews_books(id, title, pages, review_id) \
                          select id, title, pages, review_id from strider.reviews_books_tmp') 

        engine.execute('delete from strider.reviews_tmp')
        engine.execute('delete from strider.reviews_books_tmp')          

        trans.commit()
        print('reviews loaded')
    except:
        trans.rollback()
        raise
    finally:
        trans.close()
        conn.close()
        engine.dispose()
    
    
               
 

