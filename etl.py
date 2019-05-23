""" 
    This file contains the functions to run the ETL job to populate the songplays, users, songs, artists and time tables. 
"""
import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import to_timestamp,monotonically_increasing_id

#Read the configuration File
config = configparser.ConfigParser()
config.read('dl.cfg')

#Set up the Environment variables for S3 Access
os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """  
    This function returns an existing SparkSession Object or creates one if it does not exist.
    
    Parameters: 
    None
      
    Returns: 
    A Spark Session Object
    """    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """  
    This function reads the song data files provided in the input_data in JSON format and creates the song and artist
    data frames and writes them in parquet format in the out_data folder.
    
    Parameters: 
    spark: The Spark Session Object
    input_data: Path to the root folder which contains the song data in the folder structure in JSON format
    output_data: Path to the folder in which the songs and artists table are written to
    
    Returns: 
    None
    """    
    # get filepath to song data file
    song_data = input_data+"song-data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "artist_id","year","duration").dropDuplicates()
   
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data+"songs")
 
    # extract columns to create artists table
    artists_table = df.select("artist_id",col("artist_name").alias("name"),col("artist_location").alias("location"),col("artist_latitude").alias("latitude"),col("artist_longitude").alias("longitude")).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+"artists")


def process_log_data(spark, input_data, output_data):
    """  
    This function reads the files provided in the Log input_data in JSON format and creates the song and artist
    data frames and writes them in parquet format in the out_data folder.
    
    Parameters: 
    spark: The Spark Session Object
    input_data: Path to the root folder which contains the song data in the folder structure in JSON format
    output_data: Path to the folder in which the songs and artists table are written to.
    
    Returns: 
    None
    """    
    
    # get filepath to log data file
    log_data = input_data + 'log-data'

    # read log data file
    df = spark.read.json(log_data)
    
    
    # filter by actions for song plays and Non Null Users
    df = df.filter(col("page")=='NextSong').filter(df.userId.isNotNull())
    
    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"),col("firstName").alias("first_name"),col("lastName").alias("last_name"),"gender","level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+"users")


    tsFormat = "yyyy-MM-dd HH:MM:ss z"
    #Converting ts to a timestamp format    
    time_table =df.withColumn('ts', to_timestamp(date_format((df.ts /1000).cast(dataType=TimestampType()), tsFormat),tsFormat))

    # extract columns to create time table    
    time_table = time_table.select(col("ts").alias("start_time"),hour(col("ts")).alias("hour"),dayofmonth(col("ts")).alias("day"), weekofyear(col("ts")).alias("week"), month(col("ts")).alias("month"),year(col("ts")).alias("year"))

    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"time")

    # read in song data to use for songplays table
    song_data = input_data+"song-data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df, song_df.artist_name==df.artist).withColumn("songplay_id", monotonically_increasing_id()).withColumn('start_time', to_timestamp(date_format((col("ts") /1000).cast(dataType=TimestampType()), tsFormat),tsFormat)).select("songplay_id","start_time",col("userId").alias("user_id"),"level","song_id","artist_id",col("sessionId").alias("session_id"),col("artist_location").alias("location"),"userAgent",month(col("start_time")).alias("month"),year(col("start_time")).alias("year"))

    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"songplays")
    

def main():
    """  
    This function is the main function that runs the ETL job.
    
    Parameters: 
    None
      
    Returns: 
    None
    """
    #Create the Spark Session Object     
    spark = create_spark_session()
    
    #Specify Locations of the input and output 
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    #Call the methods for processing the song and log datasets
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
