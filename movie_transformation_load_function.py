import json
import boto3
from datetime import datetime
import io
from io import StringIO
import pandas as pd
import numpy as np

#funtion to create netflix dataframe from raw json 
def create_netflix_movie_df(netflix_movies_raw):

    #creating dataframe from netflix movie data
    netflix_movie_data = []
    for row in netflix_movies_raw:
        temp = {
        'url': row['url'],
        'title': row['name'],
        'content_rating': row['contentRating'],
        'description': row['description'],
        'genre': row['genre'],
        'date_added': row['dateCreated'],
        'actors': [x['name'] for x in row['actors']],
        'director': [x['name'] for x in row['director']]
        }
        netflix_movie_data.append(temp)
    df = pd.DataFrame(netflix_movie_data)
    return df
    
#function to create imdb movie datframe
def create_imdb_movie_df(imdb_movies_raw):
    
    #ombd movie data
    more_movie_data = []
    for row in imdb_movies_raw:
        try:
            temp = {
                'title': row['title'],
                'release_date': row['released'],
                'runtime_min': row['runtime'],
                'country': row['country'],
                'metascore': row['metascore'],
                'imdb_rating': row['imdb_rating'],
                'imdb_votes': row['imdb_votes'],
                'box_office': row['box_office']
                    }
        except KeyError:
            pass
        finally:
            more_movie_data.append(temp)
    df = pd.DataFrame(more_movie_data)
    return df
#funtion to process imdb columns    
def change_to_float(df, column_list):
    char_to_replace = [',', '$','min']
    for column in column_list:
        if df[column].str.contains('N/A').any():
            df[column] = df[column].replace('N/A', np.nan)
            
        for char in char_to_replace:
            if df[column].str.contains(char).any():
                df[column] = df[column].str.replace(char, '')
        df[column] = df[column].astype('float')

def lambda_handler(event, context):
    
    #getting netflix and keys
    s3=boto3.client('s3')
    Bucket = "netflix-etl-project-at"
    Key = "raw_data/to_process/netflix/"
    
    netflix_data = []
    netflix_keys = []
    
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        filename = file['Key']
    
        if filename.split('.')[-1] == 'json':
            response = s3.get_object(Bucket = Bucket, Key = filename)
            content = response['Body']
            json_object = json.loads(content.read())
            netflix_data.append(json_object)
            netflix_keys.append(filename)
            
    # getting imdb data and keys
    
    s3=boto3.client('s3')
    Bucket = "netflix-etl-project-at"
    Key = "raw_data/to_process/imdb/"
    
    imdb_data = []
    imdb_keys = []
    
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        filename = file['Key']
    
        if filename.split('.')[-1] == 'json':
            response = s3.get_object(Bucket = Bucket, Key = filename)
            content = response['Body']
            json_object = json.loads(content.read())
            imdb_data.append(json_object)
            imdb_keys.append(filename)
            
    
    #netflix data transformations and column processing
    
    for data in netflix_data:
        netflix_movie_df = create_netflix_movie_df(data)
        netflix_movie_df['date_added'] = pd.to_datetime(netflix_movie_df['date_added'])
    
        netflix_filename = 'transformed_data/netflix/netflix_transformed_' + str(datetime.now()) +'.csv'
        netflix_buffer = StringIO() #creating an empty buffer
        netflix_movie_df.to_csv(netflix_buffer, sep = ';', index =False) #filling the empty buffer with song csv file
        netflix_contents = netflix_buffer.getvalue() #getting values in song buffer
        s3.put_object(Bucket = Bucket, Key = netflix_filename, Body = netflix_contents)
    
    
    #imdb data transformations and column processing
    
    for data in imdb_data:
        imdb_movie_df = create_imdb_movie_df(data)
        imdb_movie_df['release_date'] = pd.to_datetime(imdb_movie_df['release_date'])
        change_to_float(imdb_movie_df, ['metascore', 'imdb_rating', 'imdb_votes','box_office','runtime_min'])
    
        imdb_filename = 'transformed_data/imdb/imdb_transformed_' + str(datetime.now()) +'.csv'
        imdb_buffer = StringIO() #creating an empty buffer
        imdb_movie_df.to_csv(imdb_buffer, sep = ';', index =False) #filling the empty buffer with song csv file
        imdb_contents = imdb_buffer.getvalue() #getting values in song buffer
        s3.put_object(Bucket = Bucket, Key = imdb_filename, Body = imdb_contents)
    
    #moving netflix raw data to processed folder
    s3_resource = boto3.resource('s3')
    for key in netflix_keys:
        copy_source = {
            'Bucket':Bucket,
            'Key': key
            }
            
        s3_resource.Object(Bucket, 'raw_data/processed/netflix/' + key.split('/')[-1]).copy(copy_source)
        s3_resource.Object(Bucket, key).delete()
        
    
    #moving imdb raw data to processed folder
    s3_resource = boto3.resource('s3')
    for key in imdb_keys:
        copy_source = {
            'Bucket':Bucket,
            'Key': key
            }
            
        s3_resource.Object(Bucket, 'raw_data/processed/imdb/' + key.split('/')[-1]).copy(copy_source)
        s3_resource.Object(Bucket, key).delete()
    

            
            
            
            
        
        


    

