from pyspark.sql.types import *


songplays_schema = StructType(
    [
        StructField("songplay_id", StringType(), True),
        StructField("start_time", DoubleType(), True),
        StructField("user_id", StringType(), True),
        StructField("level", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("user_agent", StringType(), True)
    ]
)
df_songplays = spark.createDataFrame([], songplays_schema)
df_songplays.createOrReplaceTempView("songplays")


users_schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("level", StringType(), True)
    ]
)
df_users = spark.createDataFrame([], songplays_schema)
df_users.createOrReplaceTempView("users")


songs_schema = StructType(
    [
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("year", ShortType(), True),
        StructField("duration", DoubleType(), True)
    ]
)
df_songs = spark.createDataFrame([], songs_schema)
df_songs.createOrReplaceTempView("songs")


artists_schema = StructType(
    [
        StructField("artist_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ]
)
df_artists = spark.createDataFrame([], artists_schema)
df_artists.createOrReplaceTempView("artists")


time_schema = StructType(
    [
        StructField("start_time", TimestampType(), True),
        StructField("hour", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", ShortType(), True),
        StructField("weekend", BooleanType(), True)
    ]
)
df_time = spark.createDataFrame([], time_schema)
df_time.createOrReplaceTempView("time")

