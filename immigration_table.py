from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import coalesce
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, \
    DoubleType, ShortType, LongType, TimestampType, DateType
import datetime
import glob
import os

def make_column_dic(content, column_name):
    
    """
    Build a dictionary to map the identifier in raw immigration table to meaningful
    data in I94_SAS_Labels_Descriptions.SAS.

    Parameters
    ----------
    content: file object
        content of I94_SAS_Labels_Descriptions.SAS
    column_name: str
        column_name of immigration table for which to build dictionary
    """
    
    content = content[content.index(column_name):]
    content = content[:content.index(';')].split('\n')
    content = [row.replace("'","") for row in content[1:]]
    content = [row.split("=") for row in content]
    content = [[row[0].strip(),row[1].strip()] for row in content if len(row)==2]
    column_dict = dict(content)
    return column_dict

def convert_datetime(x):
    
    """
    Lambda function. To convert date column to standard datetime.
    """
    
    try:
        start = datetime.datetime(1960, 1, 1)
        return start + datetime.timedelta(days=int(float(x)))
    except:
        return None
    
def city_port(port):
    """
    Lambda function. To split i94 port to two columns, city and state.
    """
    try:
        return port.split(',')[0]
    except:
        return None
    
def state_port(port):
    
    """
    Lambda function. To split i94 port to two columns, city and state.
    """
    
    try:
        return port.split(',')[1].strip()
    except:
        return None


def create_spark_session():
    
    """
    To create spark session which can read sas7bdat data.
    """
    
    spark = SparkSession.builder\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    return spark
    


def process_immigration_data(spark, input_path, output_path):

    """
    Step 1: Load data from immigration files,
    Step 2: Replace identifier with data.
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

    with open("/home/hadoop/I94_SAS_Labels_Descriptions.SAS") as f:
        f_content = f.read()
        
    i94cit_res = make_column_dic(f_content, "i94cntyl")
    i94port = make_column_dic(f_content, "i94prtl")
    i94mode = make_column_dic(f_content, "i94model")
    i94addr = make_column_dic(f_content, "i94addrl")
    i94visa = make_column_dic(f_content, "I94VISA")
    
    
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), DateType())
    spark.udf.register("udf_datetime_from_sas", udf_datetime_from_sas)
    udf_city = udf(lambda x: city_port(x), StringType())
    spark.udf.register("udf_city", udf_city)
    udf_state = udf(lambda x: state_port(x), StringType())
    spark.udf.register("udf_state", udf_city)
    

    files = [f"{input_path}/i94_{i}16_sub.sas7bdat" for i in \
         ["jan", "feb", "mar", "apr", "may", "jun","jul","aug","sep","oct","nov","dec"]]


    #process the first data file
    records_df = spark.read.format('com.github.saurfang.sas.spark').load(files[0])
    records_df = records_df.withColumn("i94_res",records_df["i94res"].cast(IntegerType()).cast(StringType()))\
                    .withColumn("i94_cit",records_df["i94cit"].cast(IntegerType()).cast(StringType()))\
                    .withColumn("i94_mode",records_df["i94mode"].cast(IntegerType()).cast(StringType()))\
                    .withColumn("i94_visa",records_df["i94visa"].cast(IntegerType()).cast(StringType()))\
                    .withColumn("day_stayed",(records_df["depdate"]-records_df["arrdate"]).cast(IntegerType()))\
                    .withColumn("arrival_date", udf_datetime_from_sas("arrdate"))\
                    .withColumn("departure_date", udf_datetime_from_sas("depdate"))

    records_df = records_df.replace(to_replace=i94cit_res, subset=['i94_res'])\
            .replace(to_replace=i94cit_res, subset=['i94_cit'])\
            .replace(to_replace=i94port, subset=['i94port'])\
            .replace(to_replace=i94mode, subset=['i94_mode'])\
            .replace(to_replace=i94addr, subset=['i94addr'])\
            .replace(to_replace=i94visa, subset=['i94_visa'])\
            .withColumn("port_city", udf_city("i94port"))\
            .withColumn("port_state", udf_state("i94port"))
        
    records_df = records_df.replace(to_replace=i94addr, subset=['port_state'])
    records_df = records_df.withColumn("address_new", coalesce(records_df["i94addr"], records_df["port_state"]))

    record_table = records_df.selectExpr("cast(cicid as int) id", "cast(i94yr as int) year", "cast(i94mon as int) month",\
                    "port_city", "port_state", "i94_mode as model", "address_new as address", "i94_res as resident",\
                    "cast(i94bir as int) age", "gender", "i94_visa as visa", "airline", "day_stayed",\
                    "arrival_date", "departure_date")

    #process the rest data file
    for i in range(len(files)-1):
        records_df = spark.read.format('com.github.saurfang.sas.spark').load(files[i+1])
        records_df = records_df.withColumn("i94_res",records_df["i94res"].cast(IntegerType()).cast(StringType()))\
                    .withColumn("i94_cit",records_df["i94cit"].cast(IntegerType()).cast(StringType()))\
                    .withColumn("i94_mode",records_df["i94mode"].cast(IntegerType()).cast(StringType()))\
                    .withColumn("i94_visa",records_df["i94visa"].cast(IntegerType()).cast(StringType()))\
                    .withColumn("day_stayed",(records_df["depdate"]-records_df["arrdate"]).cast(IntegerType()))\
                    .withColumn("arrival_date", udf_datetime_from_sas("arrdate"))\
                    .withColumn("departure_date", udf_datetime_from_sas("depdate"))

        records_df = records_df.replace(to_replace=i94cit_res, subset=['i94_res'])\
            .replace(to_replace=i94cit_res, subset=['i94_cit'])\
            .replace(to_replace=i94port, subset=['i94port'])\
            .replace(to_replace=i94mode, subset=['i94_mode'])\
            .replace(to_replace=i94addr, subset=['i94addr'])\
            .replace(to_replace=i94visa, subset=['i94_visa'])\
            .withColumn("port_city", udf_city("i94port"))\
            .withColumn("port_state", udf_state("i94port"))
        
        records_df = records_df.replace(to_replace=i94addr, subset=['port_state'])
        records_df = records_df.withColumn("address_new", coalesce(records_df["i94addr"], records_df["port_state"]))
    
        
        record_table = record_table.union(records_df.selectExpr("cast(cicid as int) id", "cast(i94yr as int) year", "cast(i94mon as int) month",\
                    "port_city", "port_state", "i94_mode as model", "address_new as address", "i94_res as resident",\
                    "cast(i94bir as int) age", "gender", "i94_visa as visa", "airline", "day_stayed",\
                    "arrival_date", "departure_date"))
    #quality check        
    if record_table.count() < 1:
        raise Exception("Wrong, no data in this table")
    record_table.write.csv(path = f"{output_path}/test.csv",mode='overwrite', header=True)

        
def main():        
    spark = create_spark_session()
    input_path = "hdfs:///immigration_data"
    output_path = "hdfs:///immigration_result"
    process_immigration_data(spark, input_path, output_path)


if __name__ == "__main__":
    main()
