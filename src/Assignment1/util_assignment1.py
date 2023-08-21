from pyspark.sql.functions import *


def time_stamp(df):
    """
    Convert the Issue Date with the timestamp format
    # """
    # df.select(col('Issue Date'), to_timestamp(col('Issue Date'))).show(truncate=False)
    timestamp = df.withColumn('NewTime', from_unixtime(col('Issue_Date') / 1000, "yyyy-MM-dd 'T' HH:mm:ssZZZZ"))
    # """
    # Convert timestamp to date type
    # """
    datetype = timestamp.withColumn("Date", split(col('NewTime'), 'T')[0])
    # datetype.show()
    # """
    # Remove extra space
    # """
    trim1 = datetype.withColumn('Brands', ltrim(col('Brand')))
    # trim1.show(truncate=False)
    # """
    # Replace null values with empty values in Country column
    # """
    tr = trim1.fillna("Empty")
    return tr


def transform(df1,output1):
    rename = df1.withColumnRenamed('SourceId', 'source_id') \
        .withColumnRenamed('TransactionNumber', 'transaction_number') \
        .withColumnRenamed('Language', 'language') \
        .withColumnRenamed('StartTime', 'start_time') \
        .withColumnRenamed('Product_Number', 'product_number')
    # rename.show(truncate=False)
    df4 = rename.withColumn("start_time_ms", unix_timestamp(col("start_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ") * 1000)
    # df4.show(truncate=False)

    #Joining two dataframes
    df5 = df4.join(output1, df4.product_number == output1.product_number,'inner')

    #filtering dataFrame
    df6 = df5.filter(df5.language == 'EN')
    return df6








