
import boto3
from datetime import datetime
import json
import numpy as np
import pandas as pd
import uuid
import utils as utils


def handler(event, context):

        # getting imdb data and keys
    
    s3=boto3.client('s3')
    Bucket = "movies-etl-project-at"
    Key = "raw_data/to_process/"
    
    movie_raw_data = []
    movie_keys = []
    
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        filename = file['Key']
        
    
        if filename.split('.')[-1] == 'json':
            response = s3.get_object(Bucket = Bucket, Key = filename)
            content = response['Body']
            json_object = json.loads(content.read())
            movie_raw_data.append(json_object)
            movie_keys.append(filename)

    # Creating and processing dataframes

    for raw_data in movie_raw_data:
        awards_df = utils.create_awards_dataframe(raw_data)
        rating_df = utils.create_rating_dataframe(raw_data)
        fact_df = utils.create_fact_dataframe(raw_data)
        movie_df = utils.create_movie_dataframe(raw_data)
        actor_df = utils.create_actors_dataframe(raw_data)
        writer_df = utils.create_writer_dataframe(raw_data)
        director_df = utils.create_director_dataframe(raw_data)

        # processing awards data
        awards_df[['wins', 'nominations']] = awards_df['awards'].str.split('&', expand=True)

        awards_df['nominations'] = np.where(awards_df['wins'].str.endswith('nominations'), awards_df['wins'], awards_df['nominations'])
        awards_df['wins'] = np.where(awards_df['wins'].str.endswith('nominations'), 'N/A', awards_df['wins'])
    
        awards_df['wins'] = awards_df['wins'].str.replace(r'[a-zA-Z/.]', '', regex=True)
        awards_df['nominations'] = awards_df['nominations'].str.replace(r'[a-zA-Z]', '', regex=True)
    
        awards_df['wins'] = awards_df['wins'].str.rstrip().str.split(' ').str.get(-1)
    
        awards_df['wins']=awards_df['wins'].replace('', 0)
        awards_df['nominations'].fillna(0, inplace = True)
    
        awards_df['wins'] = awards_df['wins'].astype('int')
        awards_df['nominations'] = awards_df['nominations'].astype('int')
        awards_df.drop('awards', axis = 1, inplace = True)

        # processing ratings data
        rating_df1 = utils.change_columns_to_float(rating_df, ['metascore','imdb_rating'])

        # processing movie data
        movie_df['release_date'] = pd.to_datetime(movie_df['release_date'], errors = 'coerce', format = 'mixed')
        movie_df1 = utils.change_columns_to_float(movie_df, ['runtime_min'])
        movie_df1['plot'] = movie_df1['plot'].str.replace(';', '')

        # processing actor, writer, and director data
        actor_df1 = utils.process_list_columns(actor_df, 'actors')
        writer_df1 = utils.process_list_columns(writer_df, 'writer')
        director_df1 = utils.process_list_columns(director_df, 'director')

        #processing fact data
        fact_df1 = utils.change_columns_to_float(fact_df, ['imdb_votes', 'box_office'])

        fact_df2 = fact_df1.merge(director_df1[['movie_id','director_id']], how = 'left', on ='movie_id')
        fact_df3 = fact_df2.merge(actor_df1[['movie_id','actors_id']], how = 'left', on ='movie_id')
        fact_df4 = fact_df3.merge(writer_df1[['movie_id','writer_id']], how = 'left', on ='movie_id')
        

        # put each dataframe in transformed data folder

        utils.to_transformed_folder('awards', awards_df)
        utils.to_transformed_folder('rating', rating_df1)
        utils.to_transformed_folder('fact', fact_df4)
        utils.to_transformed_folder('movie', movie_df1)
        utils.to_transformed_folder('writer', writer_df1)
        utils.to_transformed_folder('actor', actor_df1)
        utils.to_transformed_folder('director', director_df1)

    #moving imdb raw data to processed folder
    s3_resource = boto3.resource('s3')
    for key in movie_keys:
        copy_source = {
            'Bucket':Bucket,
            'Key': key
            }
            
        s3_resource.Object(Bucket, 'raw_data/processed/' + key.split('/')[-1]).copy(copy_source)
        s3_resource.Object(Bucket, key).delete()
