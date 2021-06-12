import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, IntegerType,StringType,  DoubleType,LongType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']= config.get('s3_credentials', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']= config.get('s3_credentials', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
     This procedure creates a spark session which will be used as a entry point to
     programming Spark with the Dataset and DataFrame API and return spark variable.
     INPUTS: 
     None
     Outputs:
     Return sparksession variable
    """
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the artist information in order to store it into the artist table.
    Then it extracts the song information in order to store it into the song table.
    and store the tables information into paraquet format

    INPUTS: 
    * spark - sparkSession variable
    * input_data - Input S3 bucket where song_data folder present
    * output_data - output S3 bucket where paraquet files need to be written
    Outputs:
    Return None
    """
    # get filepath to song data file
    song_data=input_data+"song_data"
    
    schema = StructType([
      StructField("num_songs",IntegerType(),True),
      StructField("artist_id",StringType(),True),
      StructField("artist_latitude",DoubleType(),True),
      StructField("artist_longitude",DoubleType(),True),
      StructField("artist_location",StringType(),True),
      StructField("artist_name",StringType(),True),
      StructField("song_id",StringType(),True),
      StructField("title",StringType(),True),
      StructField("duration",DoubleType(),True),
      StructField("year",IntegerType(),True)])
    
    # read song data file
    df = spark.read.schema(schema).json(song_data)
    
    df.createOrReplaceTempView("songs_table")
    columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # extract columns to create songs table
    songs_table = spark.sql(
    """
    SELECT  DISTINCT song_id,
                     title,
                     artist_id,
                     year,
                     duration
    FROM songs_table
    """
    ).toDF(*columns).dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs.parquet"), "overwrite")

    # extract columns to create artists table
    df.createOrReplaceTempView("artists_table")
    columns=["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = spark.sql(
    """
    SELECT DISTINCT artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM artists_table
    """
    ).toDF(*columns).dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists.parquet"), "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It filters by NextSong action and converts the timestamp colum to datetime.
    creates a time data records using timestamp and store it the time table.
    extracts the user information in order to store it into the users table.
    Then it extracts the songplay information using song_select query and logfile
    in order to store it into the songplay table.
    and store the tables information into paraquet format

    INPUTS: 
    * spark - sparkSession variable
    * input_data - Input S3 bucket where song_data and log_data folders present
    * output_data - output S3 bucket where paraquet files need to be written
    Outputs:
    Return None
    """
    # get filepath to log data file
    log_data = input_data+"log_data"
    
    schema_1 = StructType([
      StructField("artist",StringType(),True),
      StructField("auth",StringType(),True),
      StructField("firstName",StringType(),True),
      StructField("gender",StringType(),True),
      StructField("itemInSession",IntegerType(),True),
      StructField("lastName",StringType(),True),
      StructField("length",DoubleType(),True),
      StructField("level",StringType(),True),
      StructField("location",StringType(),True),
      StructField("method",StringType(),True),
      StructField("page",StringType(),True),
      StructField("registration",LongType(),True),
      StructField("sessionId",IntegerType(),True),
      StructField("song",StringType(),True),
      StructField("status",IntegerType(),True),
      StructField("ts",StringType(),True),
      StructField("userAgent",StringType(),True),
      StructField("userId",IntegerType(),True)])

    # read log data file and filter by actions for song plays

    df = spark.read.schema(schema_1).json(log_data).filter(col('page')=="NextSong")
    
    df.createOrReplaceTempView("users_table")
    columns = ['userId', 'firstName', 'lastName', 'gender', 'level']

    # write users table to parquet files
    users_table = spark.sql("""
    SELECT DISTINCT userId, 
                    firstName,
                    lastName, 
                    gender,
                    level
    FROM users_table
    """).toDF(*columns).dropDuplicates(['userId'])

    users_table.write.parquet(os.path.join(output_data, "users.parquet"), "overwrite")

    # create timestamp column from original timestamp column
    get_start_time = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    get_hour = udf(lambda x: datetime.fromtimestamp(x / 1000.0).hour)
    get_day = udf(lambda x: datetime.fromtimestamp(x / 1000.0).day)
    get_week = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%W'))
    get_month = udf(lambda x: datetime.fromtimestamp(x / 1000.0).month)
    get_year = udf(lambda x: datetime.fromtimestamp(x / 1000.0).year)
    get_weekday = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%A'))

    df = df.withColumn('start_time', get_start_time(df['ts']))
    df = df.withColumn('hour', get_hour(df['ts']))
    df = df.withColumn('day', get_day(df['ts']))
    df = df.withColumn('week', get_week(df['ts']))
    df = df.withColumn('month', get_month(df['ts']))
    df = df.withColumn('year', get_year(df['ts']))
    df = df.withColumn('week_day', get_weekday(df['ts']))

    df.createOrReplaceTempView("time_table")

    columns = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'week_day']

    # extract columns to create time table
    time_table = spark.sql("""
    SELECT DISTINCT start_time,
                    hour,
                    day,
                    week,
                    month,
                    year,
                    week_day
    FROM time_table
    """).toDF(*columns).dropDuplicates(['start_time'])

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time.parquet"), "overwrite")
    
    song_data=input_data+"song_data"
    
    schema = StructType([
      StructField("num_songs",IntegerType(),True),
      StructField("artist_id",StringType(),True),
      StructField("artist_latitude",DoubleType(),True),
      StructField("artist_longitude",DoubleType(),True),
      StructField("artist_location",StringType(),True),
      StructField("artist_name",StringType(),True),
      StructField("song_id",StringType(),True),
      StructField("title",StringType(),True),
      StructField("duration",DoubleType(),True),
      StructField("year",IntegerType(),True)])
    
    # read song data file
    song_df = spark.read.schema(schema).json(song_data)
    song_df.createOrReplaceTempView("songs_table")
    
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    df.createOrReplaceTempView("songplays_table")
    columns = ['songplay_id', 'start_time', 'userId', 'level', 'sessionId', 'location', 'userAgent', 'year', 'month',
               'length', 'song_id', 'artist_id', 'title', 'artist_name', 'duration']

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(
        """
            SELECT sp.songplay_id,
                   sp.start_time,
                   sp.userId,
                   sp.level,
                   sp.sessionId,
                   sp.location,
                   sp.userAgent,
                   sp.year, 
                   sp.month,
                   sp.length,
                   s.song_id,
                   s.artist_id,
                   s.title,
                   s.artist_name,
                   s.duration
            FROM songplays_table AS sp 
                JOIN songs_table AS s 
                    ON sp.song = s.title 
                    AND sp.artist = s.artist_name
                    AND sp.length = s.duration
        """).toDF(*columns)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays.parquet"), "overwrite")


def main():
    """
    The function creates a sparksession variable using create_spark_session and run the ETL ON song_data folder and
    log_data using process_song_data and process_log_data functions.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    output_data = config.get('output_folder', 's3_output')
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
