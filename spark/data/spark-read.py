import argparse
from pyspark.sql import SparkSession

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("Import csv") \
      .getOrCreate() 

parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline.'))

parser.add_argument(
    '--input_file',
    required=True,
    help='CSV filename to read from data folder')

args = parser.parse_args()

dataset = args.input_file

# Read CSV as a dataframe

df = spark.read.csv(f'/home/iceberg/data/{dataset}.csv',header=True)

# Create a table with CSV file

df.writeTo(f'my_catalog.{dataset}').create()
