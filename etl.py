import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# -----------------------------------------------------------
#sql queries
#
songs_query = "SELECT distinct song_id, title as song_title, artist_id, year, duration FROM songs"
artists_query = "SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM songs"
log_filter_query = "SELECT *, cast(ts/1000 as Timestamp) as timestamp from log_data where page = 'NextSong' "
users_query = "select distinct userId, firstName, lastName, gender, level FROM log_data" 
time_query = "select distinct ts as start_time, hour(ts) as hour, day(ts) as day, weekofyear(ts) as week, month(ts) as month, year(ts) as year, weekday(ts) as weekday from log_data"
songplay_query = "SELECT to_timestamp(ts/1000) as start_time ,userid as user_id, level, song_id,  artist_id, sessionId as session_id , location ,userAgent as user_agent FROM log_data JOIN songs ON  log_data.song = songs.title AND log_data.artist = songs.artist_name"

# -----------------------------------------------------------

def create_spark_session():
    """ this function create and connect spark session , that will be uses by other fuctions
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    this function read the json data from the song dataset and creates
    songs and srtists data sets that are writen to the output S3 bucket
    as parquet files
    
    function arguments :
    - spark - the spark session
    - input_data - path for the source files 
    - output_data - path to write the output
    """
    print("input data : {}".format(input_data))
    print("outputdata : {}".format(output_data))
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*'
    
    print("song_data : {}".format(song_data))
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql(songs_query)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = output_data + "/songs.parquet", mode = "overwrite")
    
    print("Number Of songs in songs table : {}".format(songs_table.count()))

    # extract columns to create artists table
    artists_table = spark.sql(artists_query)
    
    # write artists table to parquet files
    artists_table.write.parquet(path = output_data + "/artists.parquet", mode = "overwrite")
    
    print("Number Of artists in artists table : {}".format(artists_table.count()))

def process_log_data(spark, input_data, output_data):
    """
    this function read the json data from the song dataset and creates
    songs and srtists data sets that are writen to the output S3 bucket
    as parquet files
    
    function arguments :
    - spark - the spark session
    - input_data - path for the source files 
    - output_data - path to write the output
    """
    print("input data : {}".format(input_data))
    print("output data : {}".format(output_data))
    
    # get filepath to log data file
    #log_data = input_data + 'log_data/*/*/*'
    log_data = input_data + 'log_data/*'
    
    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("log_data")
    
    print("log_data : {}".format(log_data))
    
    # filter by actions for song plays
    df = spark.sql(log_filter_query)

    # extract columns for users table    
    users_table = spark.sql(users_query)
    
    # write users table to parquet files
    artists_table.write.partitionBy("level").parquet(path = output_data + "/users.parquet", mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df =  df.withColumn('start_time', get_timestamp('ts').cast(TimestampType()))
    
    # extract columns to create time table
    time_table = time_df.select('start_time')
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.withColumn('day', dayofmonth('start_time'))
    time_table = time_table.withColumn('week', weekofyear('start_time'))
    time_table = time_table.withColumn('month', month('start_time'))
    time_table = time_table.withColumn('year', year('start_time'))
    time_table = time_table.withColumn('weekday', dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path = output_data + "/time.parquet", mode = "overwrite")

    # extract columns from joined song and log datasets to create songplays table 
    songplay_table = spark.sql(songplay_query)

    # write songplays table to parquet files partitioned by year and month
    songplay_table.write.parquet(path = output_data + "/songplay.parquet", mode = "overwrite")

def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = "data/"
    output_data = "s3a://avishni-udacity-dend/output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
