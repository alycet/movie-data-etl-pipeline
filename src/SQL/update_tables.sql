--merging awards table

merge into MOVIE_DB.PROD.DIM_AWARDS a
using MOVIE_DB.STAGE.DIM_AWARDS_RAW b
   on  a.movie_id = b.movie_id
when matched and a.movie_id <> b.movie_id or
                 a.wins <> b.wins  or
                 a.nominations <> b.nominations then update
    set a.movie_id = b.movie_id
        ,a.wins  = b.wins 
        ,a.nominations   = b.nominations  
        ,update_timestamp = current_timestamp()
when not matched then insert
           (a.movie_id, a.wins, a.nominations, a.load_timestamp)
    values (b.movie_id, b.wins, b.nominations, b.load_timestamp);

truncate table MOVIE_DB.STAGE.DIM_AWARDS_RAW;

--merging fact table

merge into MOVIE_DB.PROD.FACT_MOVIE a
using MOVIE_DB.STAGE.FACT_MOVIE_RAW b
   on  a.movie_id = b.movie_id
   and a.director_id = b.director_id
   and a.actor_id = b.actor_id
   and a.writer_id = b.writer_id
when matched and a.movie_id <> b.movie_id or
                 a.imdb_votes <> b.imdb_votes  or
                 a.box_office <> b.box_office or
                 a.director_id <> b.director_id or
                 a.actor_id <> b.actor_id or
                 a.writer_id <> b.writer_id then update
    set a.movie_id = b.movie_id
                ,a.imdb_votes = b.imdb_votes 
                ,a.box_office = b.box_office
                ,a.director_id = b.director_id
                ,a.actor_id = b.actor_id
                ,a.writer_id = b.writer_id 
                ,update_timestamp = current_timestamp()
when not matched then insert
           (a.movie_id, a.imdb_votes, a.box_office, a.director_id, a.actor_id, a.writer_id, a.load_timestamp)
    values (b.movie_id, b.imdb_votes, b.box_office, b.director_id, b.actor_id, b.writer_id, b.load_timestamp);

truncate table MOVIE_DB.STAGE.FACT_MOVIE_RAW;

--merging movie details table

merge into MOVIE_DB.PROD.DIM_MOVIE_DETAILS a
using MOVIE_DB.STAGE.DIM_MOVIE_DETAILS_RAW b
   on  a.movie_id = b.movie_id
when not matched then insert
           (a.movie_id, a.title, a.content_rating, a.release_date, a.runtime_min, a.genre, a.plot, a.load_timestamp)
    values (b.movie_id, b.title, b.content_rating, b.release_date, b.runtime_min, b.genre, b.plot, b.load_timestamp);

truncate table MOVIE_DB.STAGE.DIM_MOVIE_DETAILS_RAW ;

--merging rating table

merge into MOVIE_DB.PROD.DIM_RATING a
using MOVIE_DB.STAGE.DIM_RATING_RAW b
   on  a.movie_id = b.movie_id
when matched and a.movie_id <> b.movie_id or
                 a.metascore <> b.metascore  or
                 a.imdb_rating <> b.imdb_rating then update
    set a.movie_id = b.movie_id
        ,a.metascore  = b.metascore 
        ,a.imdb_rating   = b.imdb_rating  
        ,update_timestamp = current_timestamp()
when not matched then insert
           (a.movie_id, a.metascore, a.imdb_rating, a.load_timestamp)
    values (b.movie_id, b.metascore, b.imdb_rating, b.load_timestamp);

truncate table MOVIE_DB.STAGE.DIM_RATING_RAW;

--merging actor table

merge into MOVIE_DB.PROD.DIM_ACTOR a
using MOVIE_DB.STAGE.DIM_ACTOR_RAW b
   on  a.movie_id = b.movie_id
   and a.actor_id = b.actor_id
when not matched then insert
           (a.movie_id, a.first_name, a.last_name, a.actor_id, a.load_timestamp)
    values (b.movie_id, b.first_name, b.last_name, b.actor_id, b.load_timestamp);

truncate table MOVIE_DB.STAGE.DIM_ACTOR_RAW;

--merging director table

merge into MOVIE_DB.PROD.DIM_DIRECTOR a
using MOVIE_DB.STAGE.DIM_DIRECTOR_RAW b
   on  a.movie_id = b.movie_id
   and a.director_id = b.director_id
when not matched then insert
           (a.movie_id, a.first_name, a.last_name, a.director_id, a.load_timestamp)
    values (b.movie_id, b.first_name, b.last_name, b.director_id, b.load_timestamp);

truncate table MOVIE_DB.STAGE.DIM_DIRECTOR_RAW;

--merging writer table

merge into MOVIE_DB.PROD.DIM_WRITER a
using MOVIE_DB.STAGE.DIM_WRITER_RAW b
   on  a.movie_id = b.movie_id
   and a.writer_id = b.writer_id
when not matched then insert
           (a.movie_id, a.first_name, a.last_name, a.writer_id, a.load_timestamp)
    values (b.movie_id, b.first_name, b.last_name, b.writer_id, b.load_timestamp);

truncate table MOVIE_DB.STAGE.DIM_WRITER_RAW;




