# Import the necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
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

############################
##       EJERCICIO 3      ##
############################
from pyspark.sql.functions import count, format_number, isnull, lit, when

amount_missing_df = df_ca.select(
    [
        (format_number(count(when(isnull(c), c)) / count(lit(1)), 2)).alias(c)
        for c in df_ca.columns
    ]
)
###########################
##        RESULTADO      ##
###########################


############################
##       EJERCICIO 4      ##
############################
# df_ca.fillna(
#     {
#         "city": "unknown",
#         "otro": 0,
#     }
# )

from pyspark.sql.types import DateType

fill_values = {}
for field in df_ca.schema:
    match field.dataType:
        case StringType():
            fill_values[field.name] = "default"
        case LongType():
            fill_values[field.name] = 0
        case DateType():
            fill_values[field.name] = "9999-99-99"

amount_missing_df = df_ca.select(
    [
        (format_number(count(when(isnull(c), c)) / count(lit(1)), 2)).alias(c)
        for c in df_ca.columns
    ]
)
amount_missing_df.show()
###########################
##        RESULTADO      ##
###########################


############################
##       EJERCICIO 5      ##
############################
from pyspark.sql.functions import year

df_grouped = (
    df_ca.dropDuplicates(["video_id"])
    .groupBy(year(df_ca["publish_date"]).alias("year_publish_date"))
    .count()
)
df_grouped.show()
most_videos_year = df_grouped.orderBy(df_grouped["year_publish_date"].desc()).first()
print(most_videos_year)

###########################
##        RESULTADO      ##
###########################


############################
##       EJERCICIO 6      ##
############################
df_ca.where(year(df_ca["publish_date"]) == 2017).show()
breakpoint()
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
