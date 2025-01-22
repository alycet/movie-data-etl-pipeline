# IMDB Movie End-To-End Data Engineering Project

### Introduction
I love watching movies, so in this project, we will build an ETL (Extract, Transform, Load) pipeline using Selenium, Beatiful Soup, OMDB API, AWS, and Snowflake.  The pipeline will retrienve movie data for the top 1000 movies on IMDB from the OMDB API, transform it into a desired format, and load it onto Snowflake data wharehouse where it can be queried whenver I need movie night ideas.

### System Architecture
![Architecture Diagram](https://github.com/alycet/movie-data-etl-pipeline/blob/main/IMDB%20Movie%20Pipeline%20Architecture%20-%20Page%201.png)
### About the Data/AP
This [OMDB API](https://www.omdbapi.com/) contains movie information from IMDB website including title, release_date, runtime, genre, plot, awards, actors, writers, directors, content_rating, box_office, imdb_rating, imdb_votes, and metascore. 

### Services Used
1.  **S3**: Amazon Simple Storage Service (Amazon S3) is a cloud storage service that allows users to store, retrieve, and manage data.
2.  **AWS CloudWatch**: An AWS CloudWatch trigger is a feature that allows you to trigger actions in AWS services based on events or schedules. CloudWatch is a monitoring service that collects and tracks metrics, monitors log files, and sets alarms.
3.  **AWS Lamdba**: Amazon Web Services (AWS) Lambda is a service that lets developers run code without managing servers. It's a serverless compute service that automatically scales and manages resources based on events.
4.  **AWS EC2**: Amazon Elastic Compute Cloud (EC2) is a web service that allows users to create and run virtual machines (instances) in the cloud. It's designed to make it easier for developers to build and run applications at web scale.
5.  **AWS ECR**:
6.  **Snowflake**: Snowflake is a cloud-based data warehouse platform that allows users to store, analyze, and exchange data. It's a Software as a Service (SaaS) platform that's designed to be scalable and flexible.
7.  **Apache Airflow**: Apache Airflow is an open-source platform for scheduling, monitoring, and creating data and computing workflows. It's written in Python and is used by data engineers to orchestrate pipelines.
8.  **Docker**: Docker is a software platform that allows developers to create, test, and deploy applications quickly. It uses containers, which are standardized units that contain all the software required to run an application.

### Packages

```
pip install bs4
pip install requests
pip install selenium
pip install omdb
pip installl numpy
pip install pandas
```

### Project Execution Flow
1. ***Extract***
   
     * Both the extraction and transformation lambda functions are deployed using a docker image to package all dependencies and libraries.
     * CloudWatch triggers the data extraction lambda function which is executed.
     * Using selenium, beautiful soup, and requests, the top 1000 movie titles are extracted from IMDB website.
     * The titles are passed to the OMDB API to extract more information about each movie.
     * The data is then loaded into an AWS S3 bucket that holds data in a to be processed folder. 
  
  

2.  ***Transform***

     * Both the tranformation and load phases are deployed in Apache Airflow within an AWS EC2 instance.
     * An S3 sensor operator detect when the raw data object is created onto the S3 bucket.
     * The transformation lambda function is triggered to clean data and transform the raw data to csv files based on the data model.
     * These csv files are loaded to S3 buckets that hold the processed and transformed data.


3.  ***Load***
   
    * Dimestion and fact table are created in Snowflake.
    * Cleaned data is copies from S3 buckets into Snowflake staging tables.
    * Data from the staging tables are then merged into target tables to keep the most up to date movie information for querying.


### Data Model
![Data Model](https://github.com/alycet/movie-data-etl-pipeline/blob/main/Movie%20DB%20Dimensional%20Model.png)
