import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "{}song_data/*/*/*/*.json".format(input_data)
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("raw_song_data")
    songs_table = spark.sql("""
                                SELECT DISTINCT song_id, title, artist_id, year, duration
                                FROM raw_song_data
                           """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(output_data + "songs/songs_table.parquet")

    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                                FROM raw_song_data
                            """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists/artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "{}log-data/*.json".format(input_data)

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    df.createOrReplaceTempView("raw_log_data")
    users_table = spark.sql("""
                                SELECT DISTINCT userId, firstName, lastName, gender, level
                                FROM raw_log_data
                            """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users/users_table.parquet")

    # create timestamp column from original timestamp column
    def format_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0)
       
    get_timestamp = udf(lambda x: format_datetime(int(x)),TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: format_datetime(int(x)), DateType())
    df = df.withColumn("start_time", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT DISTINCT start_time, hour(timestamp) as hour, day(timestamp) as day, weekofyear(timestamp) as week, \
                                            month(timestamp) as month, year(timestamp) as year, dayofmonth(timestamp) as weekday
                            FROM raw_log_data
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + "time/time_table.parquet")

    # read in song data to use for songplays table
    song_data = "{}song_data/*/*/*/*.json".format(input_data)
    song_df = spark.read.json(song_data)
    song_df = song_df.withColumn("idx", monotonically_increasing_id())
    song_df.createOrReplaceTempView("raw_song_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT DISTINCT idx, log.ts as start_time, log.userId as user_id, log.level, song.song_id, song.artist_id, \
                                                log.sessionId as session_id, log.location, log.userAgent
                                FROM raw_log_data as log JOIN raw_song_data as song ON (log.artist = song.artist_name)
                                WHERE log.page = 'NextSong'
                            """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + "songplays/songplays_table.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://lake-proj-bucket/"
    output_data = "s3a://lake-proj-bucket/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
