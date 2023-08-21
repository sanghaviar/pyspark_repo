import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.Assignment1 import util_assignment1
# testCases

def test_time_stamp(df):
    """
    Convert the Issue Date with the timestamp format
    """
    # df.select(col('Issue Date'), to_timestamp(col('Issue Date'))).show(truncate=False)
    timestamp = df.withColumn('NewTime', from_unixtime(col('Issue_Date') / 1000, "yyyy-MM-dd 'T' HH:mm:ssZZZZ"))
    # """
    # timestamp.show()
    return timestamp


def test_date_type(timestamp):
    # Convert timestamp to date type
    # """
    datetype = timestamp.withColumn("Date", split(col('NewTime'), 'T')[0])
    # datetype.show()
    return datetype


def test_trim(datetype):
    # """
    # Remove extra space
    # """
    trim1 = datetype.withColumn('Brands', ltrim(col('Brand')))
    # trim1.show(truncate=False)
    return trim1


def test_replace_null(trim1):
    # """
    # Replace null values with empty values in Country column
    # """
    tr = trim1.fillna("Empty")
    # tr.show(truncate = False)
    return tr


def test_snake_case(df1):
    rename = df1.withColumnRenamed('SourceId', 'source_id') \
        .withColumnRenamed('TransactionNumber', 'transaction_number') \
        .withColumnRenamed('Language', 'language') \
        .withColumnRenamed('StartTime', 'start_time') \
        .withColumnRenamed('Product_Number', 'product_number')
    # rename.show(truncate = False)
    return rename


def test_start_time(rename):
    # rename.show(truncate=False)
    df4 = rename.withColumn("start_time_ms", unix_timestamp(col("start_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ") * 1000)
    # df4.show(truncate=False)
    return df4


def test_join(df4, output1):
    # Joining two dataframes
    df5 = df4.join(output1, df4.product_number == output1.product_number, 'inner')
    # df5.show(truncate=False)
    return df5


def test_filter(df5):
    # filtering dataFrame
    df6 = df5.filter(df5.language == 'EN')
    # df6.show(truncate=False)
    return df6


spark = SparkSession.builder.getOrCreate()
# Test case input dataframe
data = [('Washing Machine', "1648770933000", 20000, 'samsung', 'India', '0001'),
        ('Refrigerator', "1648770999000", 35000, 'LG', None, '0002'),
        ('Air Cooler', "1648770948000", 45000, 'Voltas', None, '0003')]
schema = ['Product Name', 'Issue_Date', 'Price', 'Brand', 'Country', 'product_number']
df_test = spark.createDataFrame(data, schema)

data1 = [(150711, 123456, 'EN', 456789, '2021-12-27T08:20:29.842+0000', '0001'),
         (150439, 234567, 'UK', 345678, '2021-12-27T08:21:14.645+0000', '0002'),
         (150647, 345678, 'ES', 234567, '2021-12-27T08:22:42.445+0000', '0003')]
schema1 = ['SourceId', 'TransactionNumber', 'Language', 'ModelNumber', 'StartTime', 'Product_Number']
df1_test = spark.createDataFrame(data1, schema1)

timeStamp = test_time_stamp(df_test)
dateType = test_date_type(timeStamp)
trim = test_trim(dateType)
replaceNull = test_replace_null(trim)
output = test_replace_null(replaceNull)

snakeCase = test_snake_case(df1_test)
startTime = test_start_time(snakeCase)
join1 = test_join(startTime, output)
filter1 = test_filter(join1)


class MyTestCase(unittest.TestCase):

    def test_case1(self):
        output1 = test_time_stamp(df_test)
        x = util_assignment1.time_stamp(df_test)
        self.assertEqual(output1.collect(), x.collect())

    def test_case2(self):
        output1 = test_date_type(timeStamp)
        x = util_assignment1.date_type(timeStamp)
        self.assertEqual(output1.collect(), x.collect())

    def test_case3(self):
        output1 = test_trim(dateType)
        x = util_assignment1.trim11(dateType)
        self.assertEqual(output1.collect(), x.collect())

    def test_case4(self):
        output1 = test_replace_null(trim)
        x = util_assignment1.replace_null(trim)
        self.assertEqual(output1.collect(), x.collect())

    def test_case5(self):
        output1 = test_snake_case(df1_test)
        x = util_assignment1.snake_case(df1_test)
        self.assertEqual(output1.collect(), x.collect())

    def test_case6(self):
        output1 = test_start_time(snakeCase)
        x = util_assignment1.start_time(snakeCase)
        self.assertEqual(output1.collect(), x.collect())

    def test_Case7(self):
        output1 = test_join(startTime, output)
        x = util_assignment1.join11(startTime, output)
        self.assertEqual(output1.collect(), x.collect())

    def test_case8(self):
        output1 = test_filter(join1)
        x = util_assignment1.filter11(join1)
        self.assertEqual(output1.collect(), x.collect())


if __name__ == '__main__':
    unittest.main()
