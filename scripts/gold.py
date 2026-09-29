from spark_session import spark
from pyspark.sql import functions as F
from pyspark.sql.functions import col


conn_props = {
    'user': 'root',
    'password': 'macintosh',
    'driver': 'com.mysql.cj.jdbc.Driver'
}

silver_url = 'jdbc:mysql://localhost:3306/silver'
gold_url = 'jdbc:mysql://localhost:3306/gold?rewriteBatchedStatements=true'

# reads silver layer transaction table and returns the dataframe
def read_silver():
    df = spark.read.jdbc(url=silver_url, properties=conn_props, table='s_transaction')
    return df

# read particular table from gold layer and returns the dataframe
def read_gold(table_name):
    df = spark.read.jdbc(url=gold_url, properties=conn_props, table=table_name)
    return df

# writes a dataframe to the passed table in gold layer
def write_gold(dataframe, table_name):
    batchSize = 4000
    print(f"writing to {table_name}...")
    dataframe.write.option('batchsize', batchSize).jdbc(url=gold_url, properties=conn_props, table=table_name, mode='append')
    print(f"write complete on {table_name}.")

# populates the pre-created category dimension table
def pop_dim_category(silver_df):
    category_df2 = silver_df.select(col('category_name').alias('cat_name')).distinct()
    
    ## ensuring idempotent load
    category_df1 = read_gold('dim_category')
    category_df2 = category_df2.join(category_df1, on='cat_name', how='left_anti')
    if not category_df2.isEmpty():
        print("adding new rows!")
        write_gold(category_df2, 'dim_category')
    else:
        print("no new rows to be added to dim_category")
    return read_gold('dim_category')

# populates merchant dimension table
def pop_dim_merchant(silver_df):
    merchant_df2 = silver_df.select('merchant').distinct()
    merchant_df2 = merchant_df2.withColumnRenamed('merchant', 'merch_name')
    existing_merch = read_gold('dim_merchant')
    existing_merch = existing_merch.withColumnRenamed('merchant', 'merch_name')
    merchant_df2 = merchant_df2.join(existing_merch, on='merch_name', how='left_anti')

    ## idempotency check
    if not merchant_df2.isEmpty():
        print("appending new rows to dim_merchant table")
        write_gold(merchant_df2, 'dim_merchant')
    else:
        print("no new rows to append to dim_merchant")

    return read_gold('dim_merchant')


def pop_dim_customer(silver_df):
    silver_df = silver_df.select('cust_id', 'card_last4', 'fname', 'lname', 'cust_lat', 'cust_long', 'cust_job', 'dist', 'gender', 'street', 'city', 'state', 'zip', 'cust_dob').distinct()
    cust_new_df = silver_df.withColumnRenamed('card_last4', 'card_lastfour')\
                .withColumnRenamed('fname', 'cust_fname')\
                .withColumnRenamed('lname', 'cust_lname')\
                .withColumnRenamed('gender', 'cust_gender')\
                .withColumnRenamed('dist', 'cust_distance')\
                .withColumnRenamed('street', 'cust_street')\
                .withColumnRenamed('city', 'cust_city')\
                .withColumnRenamed('state', 'cust_state')\
                .withColumnRenamed('zip', 'cust_zip')\
                .dropDuplicates(['cust_id'])
    existing_cust_df = read_gold('dim_customer')
    cust_new_df = cust_new_df.join(existing_cust_df, on='cust_id', how='left_anti')
    # idempotency check
    if not cust_new_df.isEmpty():
        print("appending new rows to dim_customer!")
        write_gold(cust_new_df, 'dim_customer')
    else:
        print("no new rows to append to dim_customer")
    return read_gold('dim_customer')

def pop_dim_date(silver_df):
    silver_df = silver_df.select('trans_ts').distinct()
    date_new_df = silver_df.withColumn('date_full', F.to_date('trans_ts'))\
                .withColumn('date_year', F.year('trans_ts'))\
                .withColumn('date_month', F.month('trans_ts'))\
                .withColumn('day_of_month', F.dayofmonth('trans_ts'))\
                .withColumn('day_of_week', F.dayofweek('trans_ts'))\
                .withColumn('month_name', F.monthname('trans_ts'))\
                .withColumn('week_name', F.dayname('trans_ts'))\
                .withColumn('is_weekend', F.when(col('day_of_week').isin(1,7), 1).otherwise(0)).drop('trans_ts').distinct()
    # idempotency check
    existing_date_df = read_gold('dim_date')
    date_new_df = date_new_df.join(existing_date_df, on='date_full', how='left_anti')
    if not date_new_df.isEmpty():
        print("adding new rows to dim_date")
        write_gold(date_new_df, 'dim_date')
    else:
        print("no new rows to append to dim_date")
    return read_gold('dim_date')

# populates the pre-created transaction fact table
## left to rewrite. doesn't work currently
def pop_fact_transaction(silver_df, category_df, merchant_df, customer_df, date_df):
    silver_df = silver_df.select('trans_num', 'trans_ts', 'card_last4', 'amount', 'is_fraud', 'cust_id', 'merchant', 'category')
    silver_df = silver_df.withColumnRenamed('card_last4', 'cc_num')\
                .withColumnRenamed('merchant', 'merch_name').withColumnRenamed('category', 'cat_name')\
                .withColumns({'date_full': F.to_date('trans_ts'), 'trans_ts': F.col('trans_ts')})\
                .dropDuplicates(['trans_num'])

    ## now joining with dimension tables
    df_joined = silver_df.join(F.broadcast(category_df), on='cat_name', how='left')\
                        .join(F.broadcast(merchant_df), on='merch_name', how='left')\
                        .join(customer_df, on='cust_id', how='left')\
                        .join(F.broadcast(date_df), on='date_full', how='left')

    new_trans_df = df_joined.select('trans_num', 'cc_num', 'amount', 'is_fraud', 'trans_ts', 'cust_key', 'date_key', 'merch_key', 'cat_id').dropDuplicates(['trans_num'])

    ##print("caching fact transaction dataframe...")
    ##new_trans_df = new_trans_df.cache() # caching
    ##new_trans_df.count()                # caching execution by calling an action
    ## ensuring idempotent load 
    existing_trans_df = read_gold('fact_transaction')
    new_trans_df = new_trans_df.join(existing_trans_df, on='trans_num', how='left_anti')
    if not new_trans_df.isEmpty():
        print("adding new rows to fact_transaction!")
        new_trans_df = new_trans_df.repartition(8, 'trans_num') # repartition for write efficiency
        write_gold(new_trans_df, 'fact_transaction')
    else:
        print("no new rows to be added to fact_transaction")
    ##new_trans_df.unpersist() ## removing from cache
    return read_gold('fact_transaction')


def main():
    silver_df = read_silver().cache() # caching
    silver_df.count()                   # caching triggered by calling action
    category_df = pop_dim_category(silver_df)
    merchant_df = pop_dim_merchant(silver_df)
    customer_df = pop_dim_customer(silver_df)
    date_df = pop_dim_date(silver_df)
    print("calling populate transaction function")
    pop_fact_transaction(silver_df, category_df, merchant_df, customer_df, date_df)

    silver_df.unpersist()


if __name__ == '__main__':
    main()