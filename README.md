## Sparkify and ETL

Sparkify has a requirement to analyze user activity on their new streaming music app. Their information of interest is stored in JSON files for both the song metadata and user activity. 

They want to do analytics on the songs ,users and artists involved and thus an ETL job has been created to meet this requirement. The expectation is that the ETL job will create parquet files which can be used for Analytics.

### Dataset:
#### Song Dataset
The song dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

#### Log Dataset
The logdataset consists of log files in JSON format generated by this  [event simulator](https://github.com/Interana/eventsim)  based on the songs in the dataset above. These simulate app activity logs from a music streaming app based on specified configurations. The log files in the dataset are partitioned by year and month

#### Songplays Files
The songplays files consist of parquet records in the *songplays* folder and contain log data associated with song plays i.e. records with page `NextSong`. The attributes are _songplay_id, start_time, userId, level, song_id, artist_id, session_id, location, user_agent_.  The *songplay_id* is set as an auto-incrementing variable, the *start_time* is set as a TIMESTAMP, the *session_id* being an INTEGER and the rest are strings.

#### Users Files
The Users Files consists of parquet records in the *users* folder and contain the *userId , firstname, lastname, gender* and *level*. All the attributes are alphanumeric in nature and have thus been set to strings.

 #### Songs Files
 The Song Files from the music database are in parquet format in the *songs* folder and consists of *songId, Title, artistID, Year* and *duration*. INTEGER data type is a good fit for the *Year* field and the *duration* field is a floating point number and is therefore set to a NUMERIC type in the DB. The others are all set to VARCHAR since they are alphanumeric.
 
 #### Artists Files
The Artists  Files from the music database are in parquet format in the *artists* folder and consists of *ArtistID, Name, Location , the Latitude* and *Longitude*. The *Latitude* and *Longitude* are floating point variables. The others are strings.
 
 
 #### Time Files
 The Time Table consists of timestamps of records in **songplays** broken down into specific units and are in parquet format in the *time* folder. The attributes are  *start_time, hour, day, week, month, year*, and *weekday*. The *start_time* is set to a TIMESTAMP and rest are integers.
 
 ### ETL Approach
The ETL job first processes the song files followed by the log files. The song files are listed and iterated over entering relevant information in the *artists* and the *song* folders in parquet. This is followed by processing the log files. The log files are first filtered by the `NextSong` action and Non Null Users. The subsequent dataset is then processed to extract the date , time , year etc. fields and records are then appropriately entered into the *time*, *users* and *songplays* folders in parquet for analysis.

### Files
There are only 3 files. The *etl.py* file consists of the main python code. The *dl.cfg* file consists of the location of the AWS Credentials to read and write to S3. A Test Notebook *Test.ipynb* is provided to run the python script