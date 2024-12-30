# Import the necessary modules
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    column,
    count,
    desc,
    format_number,
    isnull,
    lit,
    month,
    row_number,
    to_date,
    when,
    year,
)
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

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
df_mx = spark.read.csv("./data/MXvideos.csv", schema=schema, header=True)
df_us = spark.read.csv("./data/USvideos.csv", schema=schema, header=True)
dfs = {"CA": df_ca, "MX": df_mx, "US": df_us}

for name, df in dfs.items():
    for column_name in ("views", "likes"):
        df = df.withColumn(column_name, df[column_name].cast(LongType()))
    dfs[name] = df

rename_columns = {"trending_date": "statistics_date", "publish_time": "publish_date"}
for name, df in dfs.items():
    for old_name, new_name in rename_columns.items():
        df = df.withColumnRenamed(old_name, new_name)
    dfs[name] = df

for name, df in dfs.items():
    df = df.withColumns(
        {
            "statistics_date": to_date("statistics_date", "yy.dd.MM"),
            "publish_date": to_date("publish_date", "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        }
    )
    df.printSchema()
    dfs[name] = df

############################
##       EJERCICIO 3      ##
############################
for name, df in dfs.items():
    amount_missing_df = df.select(
        [
            (
                format_number(
                    count(when(isnull(c), c)) / count(lit(1)),
                    2,
                )
            ).alias(c)
            for c in df.columns
        ]
    )
    print(name)
    amount_missing_df.show()
############################
##       EJERCICIO 4      ##
############################
# df_ca.fillna(
#     {
#         "city": "unknown",
#         "otro": 0,
#     }
# )


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

############################
##       EJERCICIO 5      ##
############################

df_grouped = (
    df_ca.dropDuplicates(["video_id"])
    .groupBy(year(df_ca["publish_date"]).alias("year_publish_date"))
    .count()
)
df_grouped.show()
most_videos_year = df_grouped.orderBy(desc("year_publish_date")).first()
print(most_videos_year)


############################
##       EJERCICIO 6      ##
############################
df_2017 = df_ca.where(year("publish_date") == 2017)

w = Window.partitionBy("video_id").orderBy(desc("statistics_date"))

filtered = df_2017.withColumn("row_number", row_number().over(w))
filtered = filtered.filter(column("row_number") == 1)
grouped = filtered.groupBy(month("publish_date").alias("month")).count()
grouped.show()
month_with_more_visits = grouped.orderBy(desc("count")).first()
############################
##       EJERCICIO 7      ##
############################
df_ca = spark.read.csv("./data/CAvideos.csv", schema=schema, header=True)
df_mx = spark.read.csv("./data/MXvideos.csv", schema=schema, header=True)
df_us = spark.read.csv("./data/USvideos.csv", schema=schema, header=True)
df_ca.createOrReplaceTempView("df_ca")
df_mx.createOrReplaceTempView("df_mx")
df_us.createOrReplaceTempView("df_us")

shared_videos = spark.sql(
    """SELECT 
        * 
    FROM df_ca 
    INNER JOIN df_mx 
        ON df_ca.video_id = df_mx.video_id
    INNER JOIN df_us
        ON df_mx.video_id = df_us.video_id"""
)
shared_videos.show()


###########################
##        RESULTADO      ##
###########################


############################
##       EJERCICIO 8     ##
############################


###########################
##        RESULTADO      ##
###########################
