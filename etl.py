import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    This Spark Session is used to launch Hadoop.
    Parameters - None
    Rerturn - It returns the spark connection to the calling function
    '''

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function processes song data file retrieved from udacity-dend S3 Bucket and
    stores the songs and artists dimensional tables in my own S3 Bucket sri-data-lake
    Parameters - Spark Connection, S3 Bucket link for udacity-dend, S3 Bucket link for my own AWS account respectively mse-data-lake
    Return - None
    '''

    # get filepath to song data file
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")

    # read song data file
    df = spark.read.json(song_data)

    df.createOrReplaceTempView("songs")

    df.printSchema()

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT distinct song_id, title, artist_id, year, duration 
                            FROM   songs 
                            WHERE  song_id IS NOT NULL""")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path=output_data + "/songs/songs.parquet",
                                                               mode="overwrite")

    print('songs Table loaded into S3 bucket')

    # extract columns to create artists table
    artists_table = spark.sql("""
                              SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
                              FROM   songs 
                              WHERE  artist_id IS NOT NULL""")

    # write artists table to parquet files
    artists_table.write.parquet(path=output_data + "/artists/artists.parquet", mode="overwrite")

    print('artists table loaded into S3 bucket')


def process_log_data(spark, input_data, output_data):
    '''
    This function processes the log file retrived from udacity-dend S3 Bucket
    and stores the users and time dimensional tables in my individual S3 Bucket sri-data-lake
    Parameters - Spark Connection, S3 Bucket link for udacity-dend, S3 Bucket link for my AWS account mse-data-lake
    Return - None
    '''

    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # Creating Temporary table
    df.createOrReplaceTempView("staging_events")

    # filter by actions for song plays
    df = spark.sql("""
                   SELECT * FROM staging_events WHERE page = 'NextSong'
                   """)

    df.createOrReplaceTempView("staging_events")

    # extract columns for users table
    users_table = spark.sql("""SELECT  DISTINCT userId, firstName, lastName, gender 
                               FROM staging_events 
                               WHERE userId IS NOT NULL""")

    # write users table to parquet files
    users_table.write.parquet(path=output_data + "/users/users.parquet", mode="overwrite")

    print('users table loaded into S3 bucket')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))

    df.createOrReplaceTempView("staging_events")

    df.printSchema()

    # extract columns to create time table
    time_table = spark.sql("""SELECT distinct timestamp as start_time, hour(timestamp) as hour, 
                              day(timestamp) as day, weekofyear(timestamp) as week, month(timestamp) as month, 
                              year(timestamp) as year, weekday(timestamp) as weekday FROM   staging_events""")

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path=output_data + "/time/time.parquet", mode="overwrite")

    print('time table loaded into S3 bucket')

    print('All dimensional tables are completed')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs/songs.parquet")
    song_df.createOrReplaceTempView("songs_parquet")
    song_df.printSchema()

    artists_df = spark.read.parquet(output_data + "/artists/artists.parquet")
    artists_df.createOrReplaceTempView("artists_parquet")
    artists_df.printSchema()

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("SELECT e.timestamp,\
                                 e.userId, e.level, s.song_id,\
                                 s.artist_id, e.sessionId, e.userAgent, \
                                 year(e.timestamp) as year, month(e.timestamp) as month \
                                 FROM staging_events e \
                                 JOIN songs_parquet s ON e.song = s.title AND e.length = s.duration \
                                 JOIN artists_parquet a ON e.artist = a.artist_name AND a.artist_id = s.artist_id")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(path=output_data + "/songplays/songplays.parquet",
                                                               mode="overwrite")

    print('songplays table loaded into S3 bucket')

    print('All fact tables are completed')


def main():
    '''
    This is the main function and uses all below functions:
    create_spark_session() - for creating spark session
    process_song_data(spark, input_data, output_data) - To Process song file from udacity-dend S3 Bucket
    process_log_data(spark, input_data, output_data) - To Process log file from udacity-dend S3 Bucket
    '''

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://mse-data-lake"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()