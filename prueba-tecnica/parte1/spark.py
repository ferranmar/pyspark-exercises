# Import the necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, to_date
from pyspark.sql.types import LongType, StringType, StructField, StructType

# Path to csv files in master machine
path_data = "/opt/bitnami/spark/prueba-tecnica/data"

schema = StructType(
    [
        StructField("video_id", StringType()),
        StructField("trending_date", StringType()),
        StructField("title", StringType()),
        StructField("channel_title", StringType()),
        StructField("category_id", StringType()),
        StructField("publish_time", StringType()),
        StructField("tags", StringType()),
        StructField("views", StringType()),
        StructField("likes", StringType()),
        StructField("dislikes", StringType()),
        StructField("comment_count", StringType()),
        StructField("thumbnail_link", StringType()),
        StructField("comments_disabled", StringType()),
        StructField("ratings_disabled", StringType()),
        StructField("video_error_or_removed", StringType()),
        StructField("description", StringType()),
    ]
)

############################
##       EJERCICIO 1      ##
############################
builder: SparkSession.Builder = SparkSession.builder
spark = (
    builder.appName("prueba-tecnica")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
############################
##       EJERCICIO 2      ##
############################
df_ca = spark.read.csv("./data/CAvideos.csv", schema=schema, header=True)
for column_name in ("views", "likes"):
    df_ca = df_ca.withColumn(column_name, df_ca[column_name].cast(LongType()))

rename_columns = {"trending_date": "statistics_date", "publish_time": "publish_date"}
for old_name, new_name in rename_columns.items():
    df_ca = df_ca.withColumnRenamed(old_name, new_name)


df_ca = df_ca.withColumns(
    {
        "statistics_date": to_date("statistics_date", "yy.dd.MM"),
        "publish_date": to_date("publish_date", "yyyy-MM-dd'T'HH:mm:ss.SSSX"),

    }
)
df_ca.printSchema()
###########################
##        RESULTADO      ##
###########################


############################
##       EJERCICIO 3      ##
############################


###########################
##        RESULTADO      ##
###########################


############################
##       EJERCICIO 4      ##
############################


###########################
##        RESULTADO      ##
###########################


############################
##       EJERCICIO 5      ##
############################


###########################
##        RESULTADO      ##
###########################


############################
##       EJERCICIO 6      ##
############################


###########################
##        RESULTADO      ##
###########################


############################
##       EJERCICIO 7      ##
############################


###########################
##        RESULTADO      ##
###########################


############################
##       EJERCICIO 8     ##
############################


###########################
##        RESULTADO      ##
###########################
