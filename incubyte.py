from pyspark.sql.types import StructType,StructField, StringType, IntegerType , FloatType
from pyspark.sql.functions import *

def create_schema(spark):
	from_pattern1 = 'yyyymmdd'
	from_pattern2 = 'ddmmyyyy'

	#Creating Manual schema

    Schema = StructType([ StructField("Name", StringType(), True),
                            StructField("Cust_I", StringType(), True),
                            StructField("Open_Date",StringType(),True),
                            StructField("Consul_Date",StringType(),True),
                            StructField("VAC_ID",StringType(),True),
                            StructField("DR_Name",StringType(),True),
                            StructField("State",StringType(),True),
                            StructField("Country",StringType(),True),
                            StructField("Dateof_Birth",StringType(),True),
                            StructField("Flag",StringType(),True)
                          ])
    #Reading Data

    health_df = (spark.read
                 .option("header",True)
                 .option("delimiter"='|')
                 .schema(Schema) # Use the schema defined in previous cell
                 .csv("#Path to csv file")
                 )

    #Transformations

    health_df = (health_df
  	.withColumn('Open_Dt', unix_timestamp(health_df['Open_Date'], from_pattern1).cast("timestamp"))
  	.withColumn('Consul_Dt', unix_timestamp(health_df['Consul_Date'], from_pattern1).cast("timestamp"))
  	.withColumn('DOB',unix_timestamp(heath_df['Dateof_Birth'],from_pattern2).cast("timestamp")))
  	





if __name__ == '__main__':
	spark = SparkSession \
    .builder \
    .master("yarn") \
    .appName('Incubyte') \
    .getOrCreate()


    create_schema(spark)



