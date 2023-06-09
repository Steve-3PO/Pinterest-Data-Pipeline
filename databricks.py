# dbutils.fs.ls('/FileStore/tables')

# pyspark functions
from pyspark.sql.functions import *
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

# AWS S3 bucket name
AWS_S3_BUCKET = 'user-12cc24ac7551-bucket'
# Mount name for the bucket
MOUNT_NAME = '/mnt/user-12cc24ac7551-bucket'
# Source url
SOURCE_URL = 's3n://{0}:{1}@{2}'.format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/user-12cc24ac7551-bucket/topics/12cc24ac7551.pin/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_pin)

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/user-12cc24ac7551-bucket/topics/12cc24ac7551.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_geo)

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/user-12cc24ac7551-bucket/topics/12cc24ac7551.user/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_user)



# Task 1: Cleaning df_pin

from pyspark.sql.functions import col,when
df_pin = df_pin.withColumn('description', when((col('description')=='No description available Story format') | (col('description')==''), None).otherwise(col('description')))
df_pin = df_pin.withColumn('follower_count', when((col('follower_count')=='User Info Error') | (col('follower_count')==''), None).otherwise(col('follower_count')))
df_pin = df_pin.withColumn('image_src', when((col('image_src')=='Image src error.') | (col('image_src')==''), None).otherwise(col('image_src')))
df_pin = df_pin.withColumn('is_image_or_video', when((col('is_image_or_video')=='multi-video(story page format)') | (col('is_image_or_video')==''), None).otherwise(col('is_image_or_video')))
df_pin = df_pin.withColumn('poster_name', when((col('poster_name')=='User Info Error') | (col('poster_name')==''), None).otherwise(col('poster_name')))
df_pin = df_pin.withColumn('tag_list', when((col('tag_list')=='N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e') | (col('tag_list')==''), None).otherwise(col('tag_list')))
df_pin = df_pin.withColumn('title', when((col('title')=='No Title Data Available') | (col('title')==''), None).otherwise(col('title')))

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
df_pin = df_pin.withColumn('follower_count', convertFC(col('follower_count')))

from pyspark.sql.types import IntegerType
df_pin = df_pin.withColumn('follower_count', df_pin['follower_count'].cast(IntegerType()))

df_pin = df_pin.withColumn('index', df_pin['index'].cast(IntegerType()))
df_pin = df_pin.withColumn('downloaded', df_pin['downloaded'].cast(IntegerType()))

from pyspark.sql.functions import regexp_replace
df_pin = df_pin.withColumn('save_location', regexp_replace(col('save_location'), 'Local save in ', ''))

df_pin = df_pin.withColumnRenamed('index', 'ind')

column_order = ["ind","unique_id","title","description","follower_count","poster_name","tag_list","is_image_or_video","image_src","save_location","category",]
df_pin = df_pin.select(*column_order)




# Task 2: Cleaning df_geo

df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))

df_geo = df_geo.drop("latitude", "longitude")

from pyspark.sql.functions import to_timestamp
df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))

column_order = ["ind","country","coordinates","timestamp"]
df_geo = df_geo.select(*column_order)



# Task 3: Cleaning df_user

# Create a new column 'user_name' that concatenates the information found in the 'first_name' and 'last_name' columns
df_user = df_user.withColumn("user_name", concat_ws(" ", "first_name", "last_name"))

# Drop the 'first_name' and 'last_name' columns
df_user = df_user.drop("first_name", "last_name")

# Convert the 'date_joined' column from a string to a timestamp data type
df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))

# Reorder the DataFrame columns
df_user = df_user.select("ind", "user_name", "age", "date_joined")




# Task 4: Find the most popular Pinterest category people post to based on their country
df_geo.createOrReplaceTempView("countries")
df_pin.createOrReplaceTempView("posts")

task_4 = spark.sql("""select countries.country, posts.category, count(*) 
                   from countries join posts on countries.ind = posts.ind 
                   group by category, country order by count(*) desc""")
display(task_4)


# Task 5: Find how many posts each category had between 2018 and 2022.

task_5 = spark.sql("""select year(countries.timestamp) as post_year, category, count(*) 
                   from countries join posts on countries.ind = posts.ind 
                   where (year(countries.timestamp) >= 2018 and year(countries.timestamp) <= 2022) 
                   group by category, year(countries.timestamp)""")
display(task_5)

# Task 6 part 1: For each country find the user with the most followers. 

df_user.createOrReplaceTempView("users")
task_6 = spark.sql("""select country, user_name, follower_count 
                   from (select *, row_number() OVER (PARTITION BY country ORDER BY follower_count DESC) as rn FROM countries 
                   join posts on countries.ind = posts.ind join users on countries.ind = users.ind) tmp where rn = 1""")
display(task_6)


# Task 6 part 2: Based on the above query, find the country with the user with most followers

task_6 = spark.sql("""select country, user_name, follower_count 
                   from (select *, row_number() OVER (PARTITION BY country ORDER BY follower_count DESC) as rn 
                   FROM countries join posts on countries.ind = posts.ind join users on countries.ind = users.ind) 
                   tmp where rn = 1 order by follower_count desc limit 1""")
display(task_6)

# Task 7: What is the most popular category people post to based on the following age groups?

task_7 = spark.sql("""WITH user_age_group AS (SELECT ind, 
                   CASE WHEN age BETWEEN 18 AND 24 THEN '18-24' 
                   WHEN age BETWEEN 25 AND 35 THEN '25-35' 
                   WHEN age BETWEEN 36 AND 50 THEN '36-50' ELSE '50+' END AS age_group FROM users), 
                   joined_data AS (SELECT user_age_group.age_group, posts.category FROM user_age_group 
                   JOIN posts ON user_age_group.ind = posts.ind), 
                   category_count AS (SELECT age_group, category, COUNT(*) AS category_count 
                   FROM joined_data GROUP BY age_group, category) 
                   SELECT age_group, category, category_count 
                   FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY age_group ORDER BY category_count DESC) 
                   AS rank FROM category_count) ranked_data WHERE rank = 1""")
display(task_7)

# Task 8: What is the median follower count for users in the following age groups?

from pyspark.sql.functions import percentile_approx
joinedDF = df_user.join(df_pin, df_user.ind == df_pin.ind)
joinedDF = joinedDF.withColumn(
    "age_group",
    when((joinedDF["age"] >= 18) & (joinedDF["age"] <= 24), "18-24")
    .when((joinedDF["age"] >= 25) & (joinedDF["age"] <= 35), "25-35")
    .when((joinedDF["age"] >= 36) & (joinedDF["age"] <= 50), "36-50")
    .otherwise("50+")
)
# Calculate the median follower count for each age group
task_8 = joinedDF.groupBy("age_group").agg(percentile_approx("follower_count", 0.5).alias("median_follower_count"))
# Show the result
task_8.show()


# Task 9: Find how many users have joined between 2015 and 2020.

from pyspark.sql.functions import year
task_9 = df_user.groupBy(year("date_joined")).agg(count("*").alias("number_users_joined"))
task_9.show()


# Task 10: Find the median follower count of users have joined between 2015 and 2020.

joinedDF = df_user.join(df_pin, df_user.ind == df_pin.ind)
task_10 = joinedDF.groupBy(year("date_joined")).agg(percentile_approx("follower_count", 0.5).alias("median_follower_count"))
task_10.show()


# Task 11: Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.

joinedDF = joinedDF.withColumn(
    "age_group",
    when((joinedDF["age"] >= 18) & (joinedDF["age"] <= 24), "18-24")
    .when((joinedDF["age"] >= 25) & (joinedDF["age"] <= 35), "25-35")
    .when((joinedDF["age"] >= 36) & (joinedDF["age"] <= 50), "36-50")
    .otherwise("50+")
)

# Calculate the median follower count for each age group
task_11 = joinedDF.groupBy("age_group", year("date_joined")).agg(percentile_approx("follower_count", 0.5).alias("median_follower_count")).sort("age_group", year("date_joined"))

# Show the result
task_11.show()