import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession


def create_spark_session():

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    # get s3 path to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    print('\nReading song data ...')
    df = spark.read.json(song_data)

    # extract columns to create songs table
    song_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']

    print('\nCreating songs table ...')
    songs_table = df.select(song_cols) \
        .distinct() \
        .withColumn('song_id',
                    F.when(F.col('song_id') == '', None)
                     .otherwise(F.col('song_id')))\
        .na.drop(subset=['song_id'])

    # write songs table to parquet files
    print('\nWriting songs table into s3')

    songs_table.write\
               .parquet(output_data + "songs_table", mode='Overwrite')

    # extract columns to create artists table
    artist_cols = ["artist_id", "artist_name", "artist_location",
                   "artist_latitude", "artist_longitude"]

    print('\nCreating artists table ...')
    artists_table = df.select(artist_cols) \
        .distinct() \
        .withColumn('artist_id',
                    F.when(F.col('artist_id') == '', None)
                     .otherwise(F.col('artist_id')))\
        .na.drop(subset=['artist_id'])

    # write artists table to parquet files
    print('\nWriting artists table into s3')

    artists_table.write\
                 .parquet(output_data + "artists_table", mode='Overwrite')


def process_log_data(spark, input_data, output_data):

    # get s3 path to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read log data file
    print('\nReading log data ...')
    df = spark.read.json(log_data)

    # Converting userId type from string to integer
    df.withColumn('userId',
                  F.when(F.col('userId') == '', None).otherwise(F.col('userId')))\
        .withColumn("userId", F.col("userId").cast(T.IntegerType()))

    # filter by actions for song plays
    df = df.filter(F.col("page") == "NextSong")

    # extract columns for users table
    user_cols = ["user_id", "first_name", "last_name", "gender", "level"]

    print('\nCreating users table ...')
    users_table = df.withColumnRenamed('userId', 'user_id')\
                    .withColumnRenamed('firstName', 'first_name')\
                    .withColumnRenamed('lastName', 'last_name')\
                    .select(user_cols)\
                    .distinct()\
                    .na.drop(subset=['user_id'])

    # write users table to parquet files
    print('\nWriting users table into s3')

    users_table.write\
               .parquet(output_data + 'users_table', mode='Overwrite')

    # Converting epoch to timestamp
    df = df.na.drop(subset=['ts'])\
           .withColumn('start_time', F.to_timestamp(F.col("ts") / 1000))

    # extract columns to create time table
    print('\nCreating time table ...')
    time_table = df.select('start_time')\
                   .withColumn('hour', F.hour(F.col("start_time")))\
                   .withColumn('day', F.dayofmonth(F.col("start_time")))\
                   .withColumn('week', F.weekofyear(F.col("start_time")))\
                   .withColumn('month', F.month(F.col("start_time")))\
                   .withColumn('year', F.year(F.col("start_time")))\
                   .withColumn('weekday', F.dayofweek(F.col("start_time")))

    # write time table to parquet files partitioned by year and month
    print('\nWriting time table into s3')
    time_table.write\
              .parquet(output_data + 'time_table',
                       partitionBy=['year', 'month'], mode='Overwrite')

    # read in song data to use for songplays table
    songplays_cols = ['songplay_id', 'start_time', 'user_id', 'level', 'song_id',
                      'artist_id', 'session_id', 'location', 'user_agent']

    # First renaming columns
    # Dropping rows with null values in song and artist columns
    # (because I'll be joinning dataframes based on them)
    df_songplays = df.withColumnRenamed('userId', 'user_id')\
                     .withColumnRenamed('sessionId', 'session_id')\
                     .withColumnRenamed('userAgent', 'user_agent')\
                     .na.drop(subset=['song', 'artist'])\
                     .select('start_time', 'user_id', 'level', 'song', 'artist',
                             'session_id', 'location', 'user_agent')

    # extract columns from joined song and log datasets to create songplays table
    df_song = spark.read.json(song_data)

    print('\nCreating songplays table ...')
    # joinning on song=title and artist=artist_name
    # adding sequential column songplay_id
    songplays_table = df_song.join(df_songplays,
                                   (df_song.artist_name == df_songplays.artist) &
                                   (df_song.title == df_songplays.song))\
        .withColumn("songplay_id", F.row_number()
                    .over(Window.partitionBy().orderBy(F.col('session_id'))))\
        .select(songplays_cols)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.withColumn('month',
                                                 F.month(F.col("start_time")))\
        .withColumn('year', F.year(F.col("start_time")))

    print('\nWriting songplays table into s3')
    songplays_table.write\
                   .parquet(output_data + 'songplays_table',
                            partitionBy=['year', 'month'], mode='Overwrite')


def main():

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dataset/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
