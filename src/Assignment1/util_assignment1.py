from pyspark.sql.functions import *


def time_stamp(df):
    """
    Convert the Issue Date with the timestamp format
    """
    # df.select(col('Issue Date'), to_timestamp(col('Issue Date'))).show(truncate=False)
    timestamp = df.withColumn('NewTime', from_unixtime(col('Issue_Date') / 1000, "yyyy-MM-dd 'T' HH:mm:ssZZZZ"))
    # """
    # timestamp.show()
    return timestamp


def date_type(timestamp):
    # Convert timestamp to date type
    # """
    datetype = timestamp.withColumn("Date", split(col('NewTime'), 'T')[0])
    # datetype.show()
    return datetype


def trim11(datetype):
    # """
    # Remove extra space
    # """
    trim1 = datetype.withColumn('Brands', ltrim(col('Brand')))
    # trim1.show(truncate=False)
    return trim1


def replace_null(trim1):
    # """
    # Replace null values with empty values in Country column
    # """
    tr = trim1.fillna("Empty")
    # tr.show(truncate = False)
    return tr


def snake_case(df1):
    rename = df1.withColumnRenamed('SourceId', 'source_id') \
        .withColumnRenamed('TransactionNumber', 'transaction_number') \
        .withColumnRenamed('Language', 'language') \
        .withColumnRenamed('StartTime', 'start_time') \
        .withColumnRenamed('Product_Number', 'product_number')
    # rename.show(truncate = False)
    return rename


def start_time(rename):
    # rename.show(truncate=False)
    df4 = rename.withColumn("start_time_ms", unix_timestamp(col("start_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ") * 1000)
    # df4.show(truncate=False)
    return df4


def join11(df4, output):
    # Joining two dataframes
    df5 = df4.join(output, df4.product_number == output.product_number, 'inner')
    # df5.show(truncate=False)
    return df5


def filter11(df5):
    # filtering dataFrame
    df6 = df5.filter(df5.language == 'EN')
    # df6.show(truncate=False)
    return df6
