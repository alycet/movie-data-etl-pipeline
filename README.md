# IMDB Movie End-To-End Data Engineering Project

## Table of Contents:
   - [Introduction](https://github.com/alycet/movie-data-etl-pipeline/blob/main/README.md#introduction)
   - [System Architecture]()
   - [About the Data/API]()
   - [Technologies Used]()
   - [Packages]()
   - [Data Execution Flow]()
   - [Data Model](https://github.com/alycet/movie-data-etl-pipeline/blob/main/README.md#data-model)

## Introduction
The Movie Data ETL Pipeline is a project designed to combine the magic of movies with the power of data engineering. Leveraging technologies like Selenium, Beautiful Soup, OMDB API, AWS, and Snowflake, this project builds an end-to-end ETL (Extract, Transform, Load) pipeline for managing movie data.

The pipeline retrieves comprehensive information about the top 1000 movies from IMDB using the OMDB API, including details like titles, genres, release dates, and ratings. This raw data is cleaned, transformed into a structured format, and ultimately loaded into a Snowflake data warehouse. With this setup, you can query the data to generate insightful recommendations or find inspiration for your next movie night. The project is a perfect blend of web scraping, API integration, and data warehouse management, making it an ideal practice ground for aspiring data engineers and movie enthusiasts alike.

[Back to table of contents](https://github.com/alycet/movie-data-etl-pipeline/blob/main/README.md#table-of-contents)

## System Architecture
![Architecture Diagram](https://github.com/alycet/movie-data-etl-pipeline/blob/main/IMDB%20Movie%20Pipeline%20Architecture%20-%20Page%201.png)
[Back to table of contents](https://github.com/alycet/movie-data-etl-pipeline/blob/main/README.md#table-of-contents)
### About the Data/API
The [OMDB API](https://www.omdbapi.com/) provides comprehensive movie information sourced from the IMDB website. This includes details such as the title, release date, runtime, genre, plot summary, awards, cast (actors, writers, directors), content rating, box office earnings, IMDb rating, number of IMDb votes, and metascore.

[Back to table of contents](https://github.com/alycet/movie-data-etl-pipeline/blob/main/README.md#table-of-contents)



## Technologies Used
1.  **S3**: Amazon Simple Storage Service (Amazon S3) is a cloud storage service that allows users to store, retrieve, and manage data.
2.  **AWS CloudWatch**: An AWS CloudWatch trigger is a feature that allows you to trigger actions in AWS services based on events or schedules. CloudWatch is a monitoring service that collects and tracks metrics, monitors log files, and sets alarms.
3.  **AWS Lamdba**: Amazon Web Services (AWS) Lambda is a service that lets developers run code without managing servers. It's a serverless compute service that automatically scales and manages resources based on events.
4.  **AWS EC2**: Amazon Elastic Compute Cloud (EC2) is a web service that allows users to create and run virtual machines (instances) in the cloud. It's designed to make it easier for developers to build and run applications at web scale.
5.  **AWS ECR**: An Amazon Elastic Container Registry (ECR) repository is a storage space for Docker and Open Container Initiative (OCI) images, and OCI-compatible artifacts
6.  **Snowflake**: Snowflake is a cloud-based data warehouse platform that allows users to store, analyze, and exchange data. It's a Software as a Service (SaaS) platform that's designed to be scalable and flexible.
7.  **Apache Airflow**: Apache Airflow is an open-source platform for scheduling, monitoring, and creating data and computing workflows. It's written in Python and is used by data engineers to orchestrate pipelines.
8.  **Docker**: Docker is a software platform that allows developers to create, test, and deploy applications quickly. It uses containers, which are standardized units that contain all the software required to run an application.

[Back to table of contents](https://github.com/alycet/movie-data-etl-pipeline/blob/main/README.md#table-of-contents)

## Packages

```
pip install bs4
pip installl numpy
pip install omdb
pip install pandas
pip install requests
pip install selenium
```
[Back to table of contents](https://github.com/alycet/movie-data-etl-pipeline/blob/main/README.md#table-of-contents)

## Project Execution Flow
1. **Environment Setup**:

   - Clone the GitHub repository and install all required packages.
   - Build the docker image to package all dependencies and libraries for the extraction and transformation functions.
   - Store docker image in AWS ECR repository.
   - Run docker compose up to install and launch Apache airflow for orchestration.
   - Set up access credentials for the OMDB API, AWS, and Snowflake.

2. **Data Extraction**:

   - Set up cloud watch trigger to execute the data extraction lambda function.
   -mUsing selenium, beautiful soup, and requests, the top 1000 movie titles are extracted from IMDB website.
   - For each movie, the titles are passed to the OMDB API to extract more information about each movie, such as cast details, box office performance, and ratings.
   - The data is then loaded into an AWS S3 bucket that raw holds data in a folder to be processed. 

3. **Data Transformation**:

   - Both the transformation and load phases are written in the DAG and deployed in Apache Airflow within an AWS EC2 instance.
   - An S3 sensor operator detects when the raw data object is created onto the S3 bucket.
   - The transformation lambda function is triggered to clean data and transform the raw data to csv files based on the data model.
   - These csv files are loaded to S3 buckets that hold the processed and transformed data.

4. **Data Loading**:

   - Create dimension and fact tables in the Snowflake data warehouse.
   - Snowflake task is defined in DAG to load the cleaned data from the S3 bucket into Snowflake staging tables.
   - Snowflake DAG is defined in DAG to merge data from staging tables into target tables, ensuring the most up-to-date movie information is available for querying.

5. **Testing and Validation**:

   - Perform validation checks to ensure the accuracy and completeness of the data.
   - Query the Snowflake warehouse to confirm the pipeline's output.

6. **Usage**:

   - Use SQL queries to explore the movie data stored in Snowflake.
   - Create visualizations or recommendations for movie nights based on genres, ratings, or other factors.

7. **Monitoring and Optimization**:

   - Implement logging and monitoring to track the pipeline's execution and performance.
   - Optimize code and configurations to improve pipeline efficiency.

[Back to table of contents](https://github.com/alycet/movie-data-etl-pipeline/blob/main/README.md#table-of-contents)

## Data Model
![Data Model](https://github.com/alycet/movie-data-etl-pipeline/blob/main/Movie%20DB%20Dimensional%20Model.png)
[Back to table of contents](https://github.com/alycet/movie-data-etl-pipeline/blob/main/README.md#table-of-contents)
