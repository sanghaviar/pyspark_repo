import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.Assignment1 import util_assignment1



def test_time_stamp(df_test):
    timestamp = df_test.withColumn('NewTime', from_unixtime(col('Issue_Date') / 1000, "yyyy-MM-dd 'T' HH:mm:ssZZZZ"))

    datetype = timestamp.withColumn("Date", split(col('NewTime'), 'T')[0])

    trim1 = datetype.withColumn('Brands', ltrim(col('Brand')))

    tr = trim1.fillna("Empty")
    return tr


def test_transform(df1_test, output1):
    rename = df1_test.withColumnRenamed('SourceId', 'source_id') \
        .withColumnRenamed('TransactionNumber', 'transaction_number') \
        .withColumnRenamed('Language', 'language') \
        .withColumnRenamed('StartTime', 'start_time') \
        .withColumnRenamed('Product_Number', 'product_number')
    # rename.show(truncate=False)
    df4 = rename.withColumn("start_time_ms", unix_timestamp(col("start_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ") * 1000)
    # df4.show(truncate=False)
    df5 = df4.join(output1, df4.product_number == output1.product_number, 'inner')
    df6 = df5.filter(df5.language == 'EN')
    return df6


class MyTestCase(unittest.TestCase):
    def test_case1(self):
        spark = SparkSession.builder.getOrCreate()
        # Test case input dataframe
        data = [('Washing Machine', "1648770933000", 20000, 'samsung', 'India', '0001'),
                ('Refrigerator', "1648770999000", 35000, 'LG', None, '0002'),
                ('Air Cooler', "1648770948000", 45000, 'Voltas', None, '0003')]
        schema = ['Product Name', 'Issue_Date', 'Price', 'Brand', 'Country', 'product_number']
        df_test = spark.createDataFrame(data, schema)

        output1 = test_time_stamp(df_test)
        data1 = [(150711, 123456, 'EN', 456789, '2021-12-27T08:20:29.842+0000', '0001'),
                 (150439, 234567, 'UK', 345678, '2021-12-27T08:21:14.645+0000', '0002'),
                 (150647, 345678, 'ES', 234567, '2021-12-27T08:22:42.445+0000', '0003')]
        schema1 = ['SourceId', 'TransactionNumber', 'Language', 'ModelNumber', 'StartTime', 'Product_Number']
        df1_test = spark.createDataFrame(data1, schema1)
        test_transform(df1_test, output1)

        # # output dataframe
        # dataoutput1= [('Washing Machine',"1648770933000",20000,'samsung','India','0001','2022-04-01 T 05:25:33GMT+05:30','2022-04-01 ','samsung'),
        #         ('Refrigerator',"1648770999000",35000, 'LG','Empty','0002','2022-04-01 T 05:26:39GMT+05:30','2022-04-01 ','LG'),
        #         ('Air Cooler',"1648770948000",45000,'Voltas','Empty','0003','2022-04-01 T 05:25:48GMT+05:30','2022-04-01 ','Voltas')]
        #
        # # Define the schema for the DataFrame
        # schemaoutput1 = ['Product Name','Issue_Date','Price','Brand','Country','product_number','NewTime','Date','Brands']
        # test1output_df =spark.createDataFrame(dataoutput1,schemaoutput1)
        output_df = util_assignment1.transform(df1_test,output1)
        self.assertEqual(output_df.collect(),test_transform(df1_test,output1).collect())

    def test_case2(self):
        spark = SparkSession.builder.getOrCreate()
        # Test case input dataframe
        data = [('Mobile', "1948770833000", 40000, 'oppo', 'USA', '0005'),
                ('Television', "1048770899000", 35900, 'tcl', None, '0006'),
                ('watch', "1748770988000", 45800, 'boat', None, '0007')]
        schema = ['Product Name', 'Issue_Date', 'Price', 'Brand', 'Country', 'product_number']
        df_test = spark.createDataFrame(data, schema)

        output1 = test_time_stamp(df_test)
        data1 = [(190781, 123456, 'EN', 123789, '2021-12-27T08:20:29.842+0000', '0005'),
                 (345499, 167897, 'UK', 987898, '2021-12-27T08:21:14.645+0000', '0006'),
                 (134567, 998767, 'ES', 987123, '2021-12-27T08:22:42.445+0000', '0007')]
        schema1 = ['SourceId', 'TransactionNumber', 'Language', 'ModelNumber', 'StartTime', 'Product_Number']
        df1_test = spark.createDataFrame(data1, schema1)
        test_transform(df1_test, output1)
        output_df = util_assignment1.transform(df1_test, output1)
        self.assertEqual(output_df.collect(), test_transform(df1_test, output1).collect())


    def test_case3(self):
        spark = SparkSession.builder.getOrCreate()
        # Test case input dataframe
        data = [('Laptop', "1888870833999", 900000, 'oppo', None, '0008'),
                ('Television', "1048770899000", 35900, 'tcl', 'USA', '0009'),
                ('watch', "1748770988000", 45800, 'boat', None, '0010')]
        schema = ['Product Name', 'Issue_Date', 'Price', 'Brand', 'Country', 'product_number']
        df_test = spark.createDataFrame(data, schema)
        output1 = test_time_stamp(df_test)
        data1 = [(280780, 923458, 'EN', 178967, '2021-12-27T08:20:29.842+0000', '0008'),
                 (145498, 767899, 'UK', 767867, '2021-12-27T08:21:14.645+0000', '0009'),
                 (284570, 598758, 'ES', 127111, '2021-12-27T08:22:42.445+0000', '00010')]
        schema1 = ['SourceId', 'TransactionNumber', 'Language', 'ModelNumber', 'StartTime', 'Product_Number']
        df1_test = spark.createDataFrame(data1, schema1)
        test_transform(df1_test, output1)
        output_df = util_assignment1.transform(df1_test, output1)
        self.assertEqual(output_df.collect(), test_transform(df1_test, output1).collect())


if __name__ == '__main__':
    unittest.main()
