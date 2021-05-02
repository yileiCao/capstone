import os
from pyspark.sql.functions import year, month
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, \
    DoubleType, ShortType, LongType, TimestampType, DateType

def create_spark_session():
    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .enableHiveSupport().getOrCreate()
    return spark


def process_tempereture_data(spark, input_path, output_path):
    tempereture_fname = "file:///home/hadoop/data/GlobalLandTemperaturesByCity.csv"
    schema = StructType([
        StructField("dt", DateType(), True),
        StructField("AverageTemperature", DoubleType(), True),
        StructField("AverageTemperatureUncertainty", DoubleType(), True),
        StructField("City", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Latitude", StringType(), True),
        StructField("Longitude", StringType(), True)
    ])
    tempereture_df = spark.read.format('csv').options(header='true').schema(schema).load(tempereture_fname)
    tempereture_df = tempereture_df.filter(tempereture_df.dt >= "2000-01-01")\
        .filter(tempereture_df.Country == "United States")\
        .withColumn("month", month(tempereture_df.dt))\
        .withColumn("year", year(tempereture_df.dt))
    tempereture_table = tempereture_df.select("year", "month", "City", "AverageTemperature")
    tempereture_table.write.csv(path = "hdfs:///immigration_data/US_tempereture.csv",mode='overwrite', header=True)
    
def main():        
    spark = create_spark_session()
    input_path = "file:///home/hadoop/data"
    output_path = "file:///home/hadoop/data"
    process_tempereture_data(spark, input_path, output_path)


if __name__ == "__main__":
    main()
