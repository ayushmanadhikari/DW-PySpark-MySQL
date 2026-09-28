from pyspark.sql import functions as F
from pyspark.sql.functions import col
from spark_session import spark

## reading silver layer transactions table and loading into a dataframe
read_url = 'jdbc:mysql://localhost:3306/silver'

conn_properties = {
    'user': 'root',
    'password': 'macintosh',
    'driver': 'com.mysql.cj.jdbc.Driver'
}


def read_data_frame():
    silver_df = spark.read.jdbc(url=read_url, properties=conn_properties, table='s_transaction')
    print(f"{silver_df.count()} number of rows imported from silver transactions!")
    return silver_df


## sets connection properties and url
def write_conn_prop_url():
    write_conn_prop = {
        'user': 'root',
        'password': 'macintosh',
        'driver': 'com.mysql.cj.jdbc.Driver',
        'rewriteBatchedStatements': 'true',
        'batchSize': '3000'
    }
    write_url = f'jdbc:mysql://localhost:3306/gold'
    return (write_conn_prop, write_url)


## saves the dataframe
def save_df(dataframe, tbl_name):
    write_conn_prop, write_url = write_conn_prop_url()
    # repartition for faster write
    dataframe = dataframe.repartition(4)
    try:
        print(f"writing into gold layer's {tbl_name} table.")
        dataframe.write.jdbc(url=write_url, properties=write_conn_prop, table=tbl_name, mode='append')
        print(f"write sucessfull! {dataframe.count()} number of rows added!")
    except Exception as e:
        print(f"could not write into the database. Error: {e}")


## populates category dimension
def create_dim_category():
    silver_df = read_data_frame()    
    cat_df = silver_df.select(F.col('category'))
    cat_df = cat_df.withColumnRenamed('category', 'cat_name')
    save_df(cat_df, 'dim_category')

# populated customer dimension table
def create_dim_cust():
    silver_df = read_data_frame()
    cust_df = silver_df.select(col('cust_id'), col('card_last4'), col('first'), col('last'), col('cust_dob'), col('gender'), col('cust_lat')\
                            , col('cust_long'), col('dist'), col('street'), col('city'), col('state'), col('zip'),col('job'))
    cust_df = cust_df.withColumnRenamed('card_last4', 'card_lastfour')\
                    .withColumnRenamed('first', 'cust_fname')\
                    .withColumnRenamed('last', 'cust_lname')\
                    .withColumnRenamed('gender', 'cust_gender')\
                    .withColumnRenamed('dist', 'cust_distance')\
                    .withColumnRenamed('gender', 'cust_gender')\
                    .withColumnRenamed('street', 'cust_street')\
                    .withColumnRenamed('city', 'cust_city')\
                    .withColumnRenamed('state', 'cust_state')\
                    .withColumnRenamed('zip', 'cust_zip')\
                    .withColumnRenamed('job', 'cust_job')
    save_df(cust_df, 'dim_customer')

# creates merchant dimension table
def create_dim_merch():
    silver_df = read_data_frame()
    merch_df = silver_df.select(col('merchant'), col('merch_lat'), col('merch_long'), col('is_fraud'))
    merch_df = merch_df.withColumnRenamed('merchant','merch_name')
    save_df(merch_df, 'dim_merchant')

# creates date dimention table
def create_dim_date():
    silver_df = read_data_frame()
    date_df = silver_df.select(col('trans_ts'))
    date_df = date_df.withColumnRenamed('trans_ts', 'date_full')\
                .withColumn('date_year', F.year('date_full'))\
                .withColumn('date_month', F.month('date_full'))\
                .withColumn('day_of_month', F.dayofmonth('date_full'))\
                .withColumn('day_of_week', F.dayofweek('date_full'))\
                .withColumn('month_name', F.monthname('date_full'))\
                .withColumn('week_name', F.dayname('date_full'))\
                .withColumn('is_weekend', F.when(col('day_of_week') == 1 | 7, 1)\
                            .otherwise(0))
                
    save_df(date_df, 'dim_date')
    

def create_fact_trans():
    silver_df = read_data_frame()
    df = silver_df.select(col('trans_num'), col('trans_ts'), col('card_last4'), col('amount'), col(''))
    pass


def main():
    ##create_dim_category()
    ##create_dim_cust()
    #create_dim_merch()
    create_dim_date()



if __name__ == '__main__':
    main()



