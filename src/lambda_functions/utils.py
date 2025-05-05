import boto3
from datetime import datetime
import io
from io import StringIO
import numpy as np
import pandas as pd
import uuid




s3=boto3.client('s3')
Bucket = "movies-etl-project-at"

# get fact table data
def create_fact_dataframe(movies_raw):
    fact_data = []
    for movie in movies_raw:
        try:
            temp = {
                'movie_id': movie['imdb_id'],
                'imdb_votes': movie['imdb_votes'],
                'box_office': movie['box_office']
                }
            fact_data.append(temp)

        except KeyError:
            pass
        
    fact_df = pd.DataFrame(fact_data)
    return fact_df


# create director dataframe
def create_director_dataframe(movies_raw):
    director_data = []
    for movie in movies_raw:
        try:
            temp = {
                'movie_id': movie['imdb_id'],
                'director': movie['director']
                }
            director_data.append(temp)

        except KeyError:
            pass
        
    director_df = pd.DataFrame(director_data)
    return director_df

# get writer dataframe
def create_writer_dataframe(movies_raw):
    writer_data = []
    for movie in movies_raw:
        try:
            temp = {
                'movie_id': movie['imdb_id'],
                'writer': movie['writer']
                }
            writer_data.append(temp)

        except KeyError:
            pass
        
    writer_df = pd.DataFrame(writer_data)
    return writer_df

# create actors dataframe
def create_actors_dataframe(movies_raw):
    actors_data = []
    for movie in movies_raw:
        try:
            temp = {
                'movie_id': movie['imdb_id'],
                'actors': movie['actors']
                }
            actors_data.append(temp)

        except KeyError:
            pass
        
    actors_df = pd.DataFrame(actors_data)
    return actors_df

# create movie dataframe
def create_movie_dataframe(movies_raw):
    movie_data = []
    for movie in movies_raw:
        try:
            temp = {
                'movie_id': movie['imdb_id'],
                'title': movie['title'],
                'content_rating': movie['rated'],
                'release_date': movie['released'],
                'runtime_min': movie['runtime'],
                'genre': movie['genre'],
                'plot': movie['plot'],
                #'lanuage': more_info['language'],
                }
            movie_data.append(temp)

        except KeyError:
            pass
        
    movie_df = pd.DataFrame(movie_data)
    return movie_df

#create rating dataframe
def create_rating_dataframe(movies_raw):
    rating_data = []
    for movie in movies_raw:
        try:
            temp = {
                'movie_id': movie['imdb_id'],
                'metascore': movie['metascore'],
                'imdb_rating': movie['imdb_rating'],
                }
            rating_data.append(temp)

        except KeyError:
            pass
        
    rating_df = pd.DataFrame(rating_data)
    return rating_df

# create awards dataframe
def create_awards_dataframe(movies_raw):
    awards_data = []
    for movie in movies_raw:
        try:
            temp = {
                'movie_id': movie['imdb_id'],
                'awards': movie['awards']
                }
            awards_data.append(temp)

        except KeyError:
            pass
        
    awards_df = pd.DataFrame(awards_data)
    return awards_df

#creating funtion to change object columns to float
def change_columns_to_float(df, column_list):
    for column in column_list:
        df[column] = df[column].str.replace(r'[a-zA-Z/$,]', '', regex=True)
        df[column] = df[column].replace('',0)
        df[column] = df[column].astype('float')
    return df

def generate_uuid5(value):
    namespace = uuid.NAMESPACE_DNS  # Choose an appropriate namespace
    return str(uuid.uuid5(namespace, str(value)))[0:13]

#function to process actors, directors, writers dataframe columns
def process_list_columns(df,column_name):
    df_copy = df.copy()
    df_copy[column_name] = df_copy[column_name].str.split(',')
    df_copy = df_copy.explode(column_name, ignore_index = True)
    new_copy = df_copy[column_name].str.split(n=1, expand = True)
    df_copy['first_name'] = new_copy[0]
    df_copy['last_name'] = new_copy[1]
    id_col = df_copy[column_name].apply(generate_uuid5)
    df_copy[column_name +'_id'] = id_col
    df_copy.drop(column_name, axis = 1, inplace=True)
    
    return df_copy

def to_transformed_folder (data_name, dataframe):
    filename = 'transformed_data/' + data_name + '_data/' + data_name + '_transformed_' + str(datetime.now()) +'.csv'
    buffer = StringIO() #creating an empty buffer
    dataframe.to_csv(buffer, sep = ';', index =False) #filling the empty buffer with movie csv file
    contents = buffer.getvalue() #getting values in song buffer
    s3.put_object(Bucket = Bucket, Key = filename, Body = contents)

    return
