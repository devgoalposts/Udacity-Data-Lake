import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F
import sys

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
# os.environ['AWS_SESSION_TOKEN']=config['AWS']['AWS_SESSION_TOKEN']

def create_spark_session():
    spark = SparkSession.builder\
                     .master("spark://pop-os.localdomain:7077")\
                     .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.3")\
                     .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    print("indexing song_data")
    # song_data = input_data + "/song_data/A/B/Q/TRABQTA128F148D048.json" # For Testing
    song_data = input_data + "/song_data/*/*/*/*.json"
    
    # read song data file
    df_songs = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df_songs[["song_id", "title", "artist_id", "year", "duration"]]
    songs_table = songs_table.dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    print("writing song_data")
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "/songs.parquet",mode="overwrite")
    # df.coalesce(1).write.format('json').save('./out/songs.out')

    # extract columns to create artists table
    print("indexing artists_table")
    artists_table = df_songs[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    artists_table = artists_table.dropDuplicates()
    
    # write artists table to parquet files
    print("writing artists_table")
    artists_table = artists_table.write.parquet(output_data + "/artists.parquet",mode="overwrite")
    # df.coalesce(1).write.format('json').save('./out/artist.out')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    print("indexing log_data")
    # log_data = "s3a://udacity-dend/log_data/2018/11/2018-11-05-events.json" # For Testing
    log_data = input_data + "/log_data/*.json"
    
    # read log data file
    df_logs = spark.read.json(log_data)
    
    # filter by actions for song plays
    print("writing log_data")
    df_logs = df_logs.filter(df_logs.page == "NextSong")

    # extract columns for users table 
    users_table = df_logs[["userId", "firstName", "lastName", "gender", "level"]]
    users_table = users_table.withColumnRenamed("user_id","userId")
    users_table = users_table.withColumnRenamed("first_name","firstName")
    users_table = users_table.withColumnRenamed("last_name","lastName")
    users_table = users_table.dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "/users.parquet",mode="overwrite")

    # create timestamp column from original timestamp column    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df_logs = df_logs.withColumn("start_time", F.to_timestamp(F.from_unixtime(F.col("ts") / 1000)) )
    
    # extract columns to create time table
    time_table = df_logs.select(
        F.col("start_time").alias('start_time'),
        F.hour("start_time").alias('hour'),
        F.dayofmonth("start_time").alias('day'),
        F.weekofyear("start_time").alias('week'),
        F.month("start_time").alias('month'), 
        F.year("start_time").alias('year'), 
        # F.dayofweek("timestamp").alias('day of week'),
        F.when(  F.dayofweek("start_time") ==  7 , False).when(  F.dayofweek("start_time") ==  1 , False).otherwise(True).alias("weekday")
    )
    time_table = time_table.dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    print("Writing time table")
    time_table.write.partitionBy("year", "month").parquet(output_data + "/time.parquet",mode="overwrite")

    # read in song data to use for songplays table
    print("writing songs table")
    artists_df = spark.read.parquet(output_data + "/artists.parquet").alias("artists")
    songs_df   = spark.read.parquet(output_data + "/songs.parquet").alias("songs")

    # extract columns from joined song and log datasets to create songplays table 
    print("Creating songplay table")
    songplay_table = df_logs.alias("logs")\
        .join(artists_df, df_logs.artist == artists_df.artist_name, 'inner')\
        .join(songs_df, df_logs.song == songs_df.title, 'inner')
    songplay_table = songplay_table.withColumnRenamed("firstName","first_name")
    songplay_table = songplay_table.withColumnRenamed("lastName","last_name")
    songplay_table = songplay_table.withColumnRenamed("userId","user_id")
    songplay_table = songplay_table.withColumnRenamed("sessionId","session_id")
    songplay_table = songplay_table.withColumnRenamed("userAgent","user_agent")
    songplay_table = songplay_table.withColumn("songplay_id", F.monotonically_increasing_id())
    songplay_table = songplay_table.withColumn('year',  F.year(F.col('start_time'))) 
    songplay_table = songplay_table.withColumn('month', F.month(F.col('start_time'))) 
    print(songplay_table.printSchema())
    songplay_table = songplay_table[["songplay_id", "start_time", "user_id", "level", "song_id", "songs.artist_id", "session_id", "logs.location", "user_agent", "year", "month"]]

    # write songplays table to parquet files partitioned by year and month
    print("writing songplay_table")
    songplay_table.write.partitionBy('year', 'month').parquet(output_data + "/songplays.parquet",mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://myawsbucket4321a"
    # input_data  = "/home/paul/Projects/DataEngineering/Spark/data/"
    # output_data = "/home/paul/Projects/DataEngineering/Spark/out/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    print("All done")
    sys.exit()

if __name__ == "__main__":
    main()
