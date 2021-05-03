import os
from pyspark.sql.functions import year, month
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, \
    DoubleType, ShortType, LongType, TimestampType, DateType

def create_spark_session():
    
    """
    To create spark session which can read sas7bdat data.
    """
    

    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .enableHiveSupport().getOrCreate()
    return spark


def process_tempereture_data(spark, input_path, output_path):
    
    """
    Step 1: Load data from tempereture files,
    Step 2: Filter data with country united states and time after 2000
    Step 3: Extract useful columns.
    Step 4: Quality check: whether table contains rows.
    Step 5: Write data into HDFS file system.
    Parameters
    ----------
    spark: spark session
        This is the spark session that has been created
    input_path: path
        This is the path where original data resides(HDFS path).
    output_path: path
        This is the path to where the output files will be written.
    """

    tempereture_fname = f"{input_path}/GlobalLandTemperaturesByCity.csv"
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
    
    #quality check 
    if tempereture_table.count() < 1:
        raise Exception("Error, no data in tempereture table.")   
    if tempereture_table.filter(tempereture_table.month > 12).count() != 0:
        raise Exception("Error, some records in tempereture table have invalid values in month column.")
        
    tempereture_table.write.csv(path = f"{output_path}/US_tempereture.csv",mode='overwrite', header=True)
    
def main():        
    spark = create_spark_session()
    input_path = "hdfs:///tempereture_data"
    output_path = "hdfs:///tempereture_result"
    process_tempereture_data(spark, input_path, output_path)


if __name__ == "__main__":
    main()
