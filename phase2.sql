--What percentage of the streamed movies are based on books?
select (b.based::float/t.total)*100 as percentage
 from (
select count(distinct s.movie_title) as total
   from strider.streams s ) as t, --91
(select count(distinct s.movie_title) as based  
 from strider.streams s, strider.reviews_movies rm, strider.reviews_books rb
    where upper(s.movie_title) = rm.title 
        and rm.review_id = rb.review_id) as b   --85


--Answer: 93.40%  
 
-- During Christmas morning (7 am and 12 noon on December 25), a partial system outage was caused by a corrupted file. Knowing the file was part of the movie "Unforgiven" thus could affect any in-progress streaming session of that movie, how many users were potentially affected?
select u.first_name,u.last_name,s.user_email  
    from strider.streams s, strider.users u
    where s.movie_title ='Unforgiven'
        and s.user_email = u.email 
        and to_timestamp(concat(substring(s.start_at,1,10),' ',substring(s.start_at,12,19)),'YYYY-MM-DD HH24:MI:SS') >= 
              TO_TIMESTAMP('2021-12-25 07:00:00','YYYY-MM-DD HH24:MI:SS')  
        and to_timestamp(concat(substring(s.start_at,1,10),' ',substring(s.start_at,12,19)),'YYYY-MM-DD HH24:MI:SS') < 
              TO_TIMESTAMP('2021-12-25 12:00:00','YYYY-MM-DD HH24:MI:SS')   
--answer:1 user
    
-- How many movies based on books written by Singaporeans authors were streamed that month?
select distinct s.movie_title, rm.title as movie, rb.title as book 
    from strider.streams s, strider.reviews_movies rm, strider.reviews_books rb, strider.books b  
    where upper(s.movie_title) = rm.title 
        and rm.review_id = rb.review_id 
        and rb.title = upper(b."name") 
        and b.author in ( select a."name" 
                             from strider.authors a, strider.nationalities n
                             where n.author = a."name" 
                                 and n."label" = 'Singaporeans' )
        and to_timestamp(concat(substring(s.start_at,1,10),' ',substring(s.start_at,12,19)),'YYYY-MM-DD HH24:MI:SS') >= 
              TO_TIMESTAMP('2021-12-01 00:00:00','YYYY-MM-DD HH24:MI:SS')  
        and to_timestamp(concat(substring(s.start_at,1,10),' ',substring(s.start_at,12,19)),'YYYY-MM-DD HH24:MI:SS') < 
              TO_TIMESTAMP('2021-12-31 23:59:00','YYYY-MM-DD HH24:MI:SS')
    order by 1         
--answer:3

--- What's the average streaming duration?
select (sum(t.dur)/sum(t.n)) as average_duration
   from (    
select  s.movie_title,s.user_email,
      sum(to_timestamp(concat(substring(s.end_at,1,10),' ',substring(s.end_at,12,19)),'YYYY-MM-DD HH24:MI:SS') - 
         to_timestamp(concat(substring(s.start_at,1,10),' ',substring(s.start_at,12,19)),'YYYY-MM-DD HH24:MI:SS')) as dur,
      count(*) as n   
     from strider.streams s 
     group by s.movie_title,s.user_email
     order by 1,2 ) as t
--answer 12 hours..
     
--- What's the **median** streaming size in gigabytes?
select (sum(t.size_mb)/sum(t.n))/1024 as median_size
  from(
select  s.movie_title,s.user_email,
      sum(s.size_mb) as size_mb,
      count(*) as n   
     from strider.streams s 
     group by s.movie_title,s.user_email
     order by 1,2) as t 
--answer 1gb
     
--Given the stream duration (start and end time) and the movie duration, 
--how many users watched at least 50% of any movie in the last week of the month (7 days)?
select count(t.user_email) as users_watched_50
  from(     
    select s.movie_title,
        s.user_email,
        extract(epoch from to_timestamp(concat(substring(s.end_at,1,10),' ',substring(s.end_at,12,19)),'YYYY-MM-DD HH24:MI:SS') - 
         to_timestamp(concat(substring(s.start_at,1,10),' ',substring(s.start_at,12,19)),'YYYY-MM-DD HH24:MI:SS'))/3600 as spent,
        (m.duration_mins::float/60) as movie_duration  
     from strider.streams s, strider.movies m 
     where s.movie_title = m.title   
        and to_timestamp(concat(substring(s.start_at,1,10),' ',substring(s.start_at,12,19)),'YYYY-MM-DD HH24:MI:SS') >= 
              TO_TIMESTAMP('2021-12-25 00:00:00','YYYY-MM-DD HH24:MI:SS')  
        and to_timestamp(concat(substring(s.start_at,1,10),' ',substring(s.start_at,12,19)),'YYYY-MM-DD HH24:MI:SS') < 
              TO_TIMESTAMP('2021-12-31 23:59:00','YYYY-MM-DD HH24:MI:SS')
     order by s.movie_title,s.user_email ) as t
  where t.spent > (t.movie_duration/2)   
--answer 1,863 users  
  