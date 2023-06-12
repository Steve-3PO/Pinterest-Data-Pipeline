# pyspark functions
from pyspark.sql.types import StructType,StructField,IntegerType, TimestampType, FloatType
from pyspark.sql.functions import col, from_json, isnull, unix_timestamp, *
# URL processing
import urllib

# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load('/FileStore/tables/authentication_credentials.csv')

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

kinesisGeo = spark.readStream.format("kinesis").option("streamName", "streaming-12cc24ac7551-geo").option("region", 'us-east-1').option("initialPosition", '{"at_timestamp": "2023/06/12 15:18:00 GMT", "format": "yyyy/MM/dd HH:mm:ss ZZZ"}').option("awsAccessKey", ACCESS_KEY).option("awsSecretKey", SECRET_KEY).load()
#display(kinesisGeo)


from pyspark.sql.types import StructType,StructField,IntegerType, TimestampType, FloatType,col, from_json, isnull, unix_timestamp

schema = StructType([StructField("ind",IntegerType(),True),
                     StructField("timestamp",TimestampType(),True),
                     StructField("latitude",FloatType(),True),
                     StructField("longitude",FloatType(),True),
                     StructField("country",StringType(),True)
  ])

json_geo = kinesisGeo.select(from_json("data", schema).alias("json"))
countries = (json_geo.select(col("json.ind").alias("ind"),
                          col("json.timestamp").alias("timestamp"),
                          col("json.latitude").alias("latitude"),
                          col("json.longitude").alias("longitude"),
                          col("json.country").alias("country")
                          ))
#display(countries)  
countries = countries.withColumn("coordinates", array("latitude", "longitude"))
countries = countries.drop("latitude", "longitude")
column_order = ["ind","country","coordinates","timestamp"]
countries = countries.select(*column_order)
# Write the data to a table.
countries.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/delta/").toTable("12cc24ac7551_geo_table")




kinesisPin = spark.readStream.format("kinesis").option("streamName", "streaming-12cc24ac7551-pin").option("region", 'us-east-1').option("initialPosition", '{"at_timestamp": "2023/06/12 15:18:00 GMT", "format": "yyyy/MM/dd HH:mm:ss ZZZ"}').option("awsAccessKey", ACCESS_KEY).option("awsSecretKey", SECRET_KEY).load()
#display(kinesisPin)

kinesisPin = kinesisPin.withColumn('data', kinesisPin['data'].cast(StringType()))
schema = StructType([StructField("index",IntegerType(),True),
                     StructField("unique_id",StringType(),True),
                     StructField("title",StringType(),True),
                     StructField("description",StringType(),True),
                     StructField("poster_name",StringType(),True),
                     StructField("follower_count",StringType(),True),
                     StructField("tag_list",StringType(),True),
                     StructField("is_image_or_video",StringType(),True),
                     StructField("image_src",StringType(),True),
                     StructField("downloaded",IntegerType(),True),
                     StructField("save_location",StringType(),True),
                     StructField("category",StringType(),True)
  ])

json_pin = kinesisPin.select(from_json("data", schema).alias("json"))
posts = (json_pin.select(col("json.index").alias("ind"),
                          col("json.unique_id").alias("unique_id"),
                          col("json.title").alias("title"),
                          col("json.description").alias("description"),
                          col("json.poster_name").alias("poster_name"),
                          col("json.follower_count").alias("follower_count"),
                          col("json.tag_list").alias("tag_list"),
                          col("json.is_image_or_video").alias("is_image_or_video"),
                          col("json.image_src").alias("image_src"),
                          col("json.downloaded").alias("downloaded"),
                          col("json.save_location").alias("save_location"),
                          col("json.category").alias("category")
                          ))

# display(posts)  
from pyspark.sql.functions import when
posts = posts.withColumn('description', when((col('description')=='No description available Story format') | (col('description')==''), None).otherwise(col('description')))
posts = posts.withColumn('follower_count', when((col('follower_count')=='User Info Error') | (col('follower_count')==''), None).otherwise(col('follower_count')))
posts = posts.withColumn('image_src', when((col('image_src')=='Image src error.') | (col('image_src')==''), None).otherwise(col('image_src')))
posts = posts.withColumn('is_image_or_video', when((col('is_image_or_video')=='multi-video(story page format)') | (col('is_image_or_video')==''), None).otherwise(col('is_image_or_video')))
posts = posts.withColumn('poster_name', when((col('poster_name')=='User Info Error') | (col('poster_name')==''), None).otherwise(col('poster_name')))
posts = posts.withColumn('tag_list', when((col('tag_list')=='N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e') | (col('tag_list')==''), None).otherwise(col('tag_list')))
posts = posts.withColumn('title', when((col('title')=='No Title Data Available') | (col('title')==''), None).otherwise(col('title')))

def standardise_follower_count(follower_count):
    if not follower_count:
        return follower_count
    elif follower_count[-1] == 'k':
        follower_count = follower_count[:-1] + '000'
    elif follower_count[-1] == 'M':
        follower_count = follower_count[:-1] + '000000'
    # print(follower_count)
    return follower_count

from pyspark.sql.functions import udf
convertFC = udf(lambda z: standardise_follower_count(z))
posts = posts.withColumn('follower_count', convertFC(col('follower_count')))
posts = posts.withColumn('follower_count', posts['follower_count'].cast(IntegerType()))
# posts = posts.withColumn('ind', posts['ind'].cast(IntegerType()))
# posts = posts.withColumn('downloaded', posts['downloaded'].cast(IntegerType()))

from pyspark.sql.functions import regexp_replace
posts = posts.withColumn('save_location', regexp_replace(col('save_location'), 'Local save in ', ''))

column_order = ["ind","unique_id","title","description","follower_count","poster_name","tag_list","is_image_or_video","image_src","save_location","category",]
posts = posts.select(*column_order)
# display(posts)

# Write the data to a table.
posts.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/delta/").toTable("12cc24ac7551_pin_table")



kinesisUser = spark.readStream.format("kinesis").option("streamName", "streaming-12cc24ac7551-user").option("region", 'us-east-1').option("initialPosition", '{"at_timestamp": "2023/06/12 15:18:00 GMT", "format": "yyyy/MM/dd HH:mm:ss ZZZ"}').option("awsAccessKey", ACCESS_KEY).option("awsSecretKey", SECRET_KEY).load()
#display(kinesisUser)

kinesisUser = kinesisUser.withColumn('data', kinesisUser['data'].cast(StringType()))
schema = StructType([StructField("ind",IntegerType(),True),
                     StructField("first_name",StringType(),True),
                     StructField("last_name",StringType(),True),
                     StructField("age",IntegerType(),True),
                     StructField("date_joined",TimestampType(),True)
  ])

json_user = kinesisUser.select(from_json("data", schema).alias("json"))
users = (json_user.select(col("json.ind").alias("ind"),
                          col("json.first_name").alias("first_name"),
                          col("json.last_name").alias("last_name"),
                          col("json.age").alias("age"),
                          col("json.date_joined").alias("date_joined")
                          ))
# display(users)  

# Create a new column 'user_name' that concatenates the information found in the 'first_name' and 'last_name' columns
users = users.withColumn("user_name", concat_ws(" ", "first_name", "last_name"))

# Drop the 'first_name' and 'last_name' columns
users = users.drop("first_name", "last_name")

# Convert the 'date_joined' column from a string to a timestamp data type
users = users.withColumn("date_joined", to_timestamp("date_joined"))

# Reorder the DataFrame columns
users = users.select("ind", "user_name", "age", "date_joined")
# display(users)

# Write the data to a table.
users.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/delta/").toTable("12cc24ac7551_user_table")