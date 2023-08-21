# from Assignments.src.Assignment1 import util_assignment1

from pyspark.sql import SparkSession
from util_assignment1 import *
spark = SparkSession.builder.getOrCreate()

data = [('Washing Machine', "1648770933000", 20000, 'samsung', 'India', '0001'),
        ('Refrigerator', "1648770999000", 35000, 'LG', None, '0002'),
        ('Air Cooler', "1648770948000", 45000, 'Voltas', None, '0003')]
schema = ['Product Name', 'Issue_Date', 'Price', 'Brand', 'Country', 'product_number']
df = spark.createDataFrame(data, schema)
# time_stamp(df)
# output1 = time_stamp(df)


data1 = [(150711, 123456, 'EN', 456789, '2021-12-27T08:20:29.842+0000', '0001'),
                 (150439, 234567, 'UK', 345678, '2021-12-27T08:21:14.645+0000', '0002'),
                 (150647, 345678, 'ES', 234567, '2021-12-27T08:22:42.445+0000', '0003')]
schema1 = ['SourceId', 'TransactionNumber', 'Language', 'ModelNumber', 'StartTime', 'Product_Number']
df1 = spark.createDataFrame(data1, schema1)
# transform(df1,output1)

timeStamp = time_stamp(df)
dateType = date_type(timeStamp)
trim = trim11(dateType)
replaceNull = replace_null(trim)
output= replace_null(replaceNull)

snakeCase = snake_case(df1)
startTime = start_time(snakeCase)
join1 = join11(startTime,output)
filter1 = filter11(join1)


