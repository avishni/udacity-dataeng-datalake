# Sparkify data lake
this project is part of the [Udacity data engeneering nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027)

**Sparkify** is a music streaming application, users can selects songs and listen to them from  apps on the phone or from web browser

In this project we will create datalake for sparkify data to help the analytics team get some insights from the data that is beeing collected. the process contains load data from S3, process the data into analytics tables using Spark, and load them back into S3

## dataset 
The ETL contains two types of data located on S3 Bucket
- Song data 
based on the 'Million Song Dataset', contains JSON files 
Log data partitioned by the first three letters of each song's track ID. each file contains data about songs for example :song name , artist, duration 

- Logs data
Contains json files orgenize by datas . each file contains events from the app activity log

## output schema
the output data model for the analytics will contain the tables : 

### Fact Table
1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension Tables
2. **users** - users in the app
    - user_id, first_name, last_name, gender, level
3. **songs** - songs in music database
    - song_id, title, artist_id, year, duration
4. **artists** - artists in music database
    - artist_id, name, location, lattitude, longitude
5. **time** - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

## Project files
- etl.py - code for running the ETL Process
- dl.cfg - configuration data
- sql_etl.sql - sql queries for the ETL



## prerequisite 
there are several prerequisite to succesfully run the ETL

1. EMR cluster on aws
2. S3 Bucket for the ETL output files

## how to run
1. open terminal and run
 `python etl.py`

## analytics
to check the process and valiadate the data, you can use the analytics.py to run some queries on the extracted data

