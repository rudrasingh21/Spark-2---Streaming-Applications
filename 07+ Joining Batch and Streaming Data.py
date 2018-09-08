#/home/ubuntu/datasets/droplocation

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import explode

if __name__ == "__main__":

	sparkSession = SparkSession.builder.appName("join").getOrCreate()
	
	sparkSession.sparkContext.setLogLevel("ERROR")
	
	personal_details_schema = StructType([StructField('Customer_ID', StringType(), True),\
										StructField('Gender', StringType(), True),\
										StructField('Age', StringType(), True)])
	
	customerDF = sparkSession.read\
							.format("csv")\
							.option("header","true")\
							.schema(personal_details_schema)\
							.load("/home/ubuntu/datasets/customerDatasets/static_datasets/join_static_personal_details.csv")
							
	transaction_data_schema = StructType([StructField('Customer_ID', StringType(), True),\
										StructField('Transation_Amount', StringType(), True),\
										StructField('Transation_Rating', StringType(), True)])
										
	fileStreamDF = sparkSession.readStream.option("header","true").option("maxFilesPerTrigger", 1).schema(transaction_data_schema).csv("/home/ubuntu/datasets/customerDatasets/streaming_datasets/join_streaming_transaction_details")
	
	joinedDF = customerDF.join(fileStreamDF,"Customer_ID")
	
	query = joinedDF\
			.writeStream\
			.outputMode('append')\
			.format('console')\
			.start()\
			.awaitTermination()