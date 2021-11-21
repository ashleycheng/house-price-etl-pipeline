from pyspark.sql.functions import col
from pyspark.sql.functions import format_string
from pyspark.sql.functions import udf
from pyspark.sql.functions import input_file_name
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType
from dateutil.parser import parse
import pyspark.sql.functions as F


def map_city_code(filename):
    # Use the alphabet in the file name to map the city or county name
    # For example, when the file name is "101S4_g_lvr_land_a.csv", the alphabet
    # used to map the name of city of country in the file name is "g"
    city_code = {
        'a': '台北市', 'b': '台中市', 'c': '基隆市', 'd': '台南市', 'e': '高雄市', 'f': '新北市',
        'g': '宜蘭縣', 'h': '桃園縣', 'j': '新竹縣', 'k': '苗栗縣', 'l': '臺中縣', 'm': '南投縣',
        'n': '彰化縣', 'p': '雲林縣', 'q': '嘉義縣', 'r': '臺南縣', 's': '高雄縣', 't': '屏東縣',
        'u': '花蓮縣', 'v': '臺東縣', 'x': '澎湖縣', 'y': '陽明山', 'w': '金門縣', 'z': '連江縣',
        'i': '嘉義市', 'o': '新竹市'}
    name = filename.split('/')[-1]
    city = city_code[name[6]]
    return city


def is_valid_date(date):
    if date:
        try:
            parse(str(date))
            return "True"
        except:
            return "False"
    return "False"

spark = SparkSession.builder \
  .appName('dataproc-pyspark')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()

df = spark.read.format("csv").option("header", "true") \
    .load("gs://BUCKET_NAME/land_data/*_a.csv")

eng_columns = ["city", "township_dist", "transaction_sign", "position",
               "building_area_m2", "completion_date", "transaction_date",
               "total_price", "unit_price_m2"]
add_city = udf(lambda x: map_city_code(x), StringType())
date_format = udf(lambda x: str(int(x[:-4])+1911) + '-' + x[-4:-2] + '-' + x[-2:], StringType())
check_date = udf(lambda x: is_valid_date(x), StringType())


result = (
    # Remove the English header field from row data
    df.filter(F.col("交易標的") != "transaction sign")
    .filter(df.交易標的.startswith("房地"))
    # Use the alphabet in the file name to map the city and county name
    .withColumn("檔名", input_file_name())
    .withColumn("縣市", add_city("檔名"))
    # Rename columns
    .select(["縣市", "鄉鎮市區", "交易標的", "土地位置建物門牌", "建物移轉總面積平方公尺", "建築完成年月",
             "交易年月日", "總價元", "單價元平方公尺"])
    .toDF(*eng_columns)
    # Change the date format
    .withColumn("transaction_date", date_format("transaction_date").cast(DateType()))
    # Check if the date is valid
    .withColumn("check_date", check_date("transaction_date"))
    .filter(F.col("check_date") == "True")
    .drop("check_date")
    # Change the DataFrame column data type
    .withColumn("building_area_m2", col("building_area_m2").cast('float'))
    .withColumn("unit_price_m2", col("unit_price_m2").cast('float'))
    .withColumn("total_price", col("total_price").cast('int'))
    # Data clean
    .withColumn("unit_price_m2", F.when(col('unit_price_m2') == 0, F.round(
        col("total_price") / col("unit_price_m2"), 2)).otherwise(col('unit_price_m2')))
    # Convert the area unit from square meter to ping
    .withColumn("building_area_m2", F.round(col("building_area_m2") / 3.30579, 2))
    .withColumnRenamed("building_area_m2", "building_area_ping")
    .withColumn("unit_price_m2", F.round(col("unit_price_m2") * 3.30579, 2))
    .withColumnRenamed("unit_price_m2", "unit_price_ping")
)

result.show(10)
# print((result.count(), len(result.columns)))

# write the result to Bigquery
result.write.format("bigquery") \
    .option("temporaryGcsBucket", "actural_price_project") \
    .mode("overwrite") \
    .save("project_id:dataset_id.table_id")  # To specify a Bigqeury table with a string
