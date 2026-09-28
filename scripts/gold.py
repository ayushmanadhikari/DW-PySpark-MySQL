from spark_session import spark
from pyspark.sql import functions as F
from pyspark.sql.functions import col


conn_props = {
    'user': 'root',
    'password': 'macintosh',
    'driver': 'com.mysql.cj.jdbc.Driver'
}

silver_url = 'jdbc:mysql://localhost:3306/silver'
gold_url = 'jdbc:mysql://localhost:3306/gold'

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
    print(f"writing to {table_name}...")
    dataframe.write.jdbc(url=gold_url, properties=conn_props, table=table_name, mode='append')
    print(f"write complete on {table_name}. {dataframe.count()} number of rows added!")

# populates the pre-created category dimension table
def pop_dim_category(silver_df):
    category_df2 = silver_df.select(col('category_name').alias('cat_name')).distinct()
    
    ## ensuring idempotent load
    category_df1 = read_gold('dim_category')
    category_df2 = category_df2.join(category_df1, on='cat_name', how='left_anti')
    if category_df2.head(1):
        print("adding new rows!")
        write_gold(category_df2, 'dim_category')
    else:
        print("no new rows to be added to dim_category")
    return read_gold('dim_category')

# populates merchant dimension table
def pop_dim_merchant(silver_df):
    merchant_df2 = silver_df.select('merchant', 'merch_lat', 'merch_long').distinct()
    merchant_df2 = merchant_df2.withColumnRenamed('merchant', 'merch_name')
    existing_merch = read_gold('dim_merchant')
    existing_merch = existing_merch.withColumnRenamed('merchant', 'merch_name')
    merchant_df2 = merchant_df2.join(existing_merch, on='merch_name', how='left_anti')

    ## idempotency check
    if merchant_df2.head(1):
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
    if cust_new_df.head(1):
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
                .withColumn('is_weekend', F.when(col('day_of_week').isin(1,7), 1).otherwise(0)).drop('trans_ts')
    # idempotency check
    existing_date_df = read_gold('dim_date')
    date_new_df = date_new_df.join(existing_date_df, on='date_full', how='left_anti')
    if date_new_df.head(1):
        print("adding new rows to dim_date")
        write_gold(date_new_df, 'dim_date')
    else:
        print("no new rows to append to dim_date")
    return read_gold('dim_date')

# populates the pre-created transaction fact table
## left to rewrite. doesn't work currently
def pop_fact_transaction(silver_df, category_df, merchant_df, customer_df, date_df):
    staged_silver = silver_df.select(col('trans_num'), col('trans_ts').alias('trans_dt_ts'), col('card_last4').alias('cc_num'), col('amount'), col('is_fraud'), col('category_name'),
                                    'merchant', 'cust_id', 'trans_ts')
    df_join = staged_silver.join(category_df, staged_silver['category_name'] == category_df['cat_name'], 'left')\
                        .join(merchant_df, on='merch_name', how='left')\
                        .join(customer_df, on='cust_id', how='left')\
                        .join(date_df, on='date_full', how='left')
    
    df_join = df_join.select('trans_num', 'trans_dt_ts', 'cc_num', 'amount', 'is_fraud', 'cat_id', 'merch_key', 'cust_key', 'date_key')
    
    ## ensuring idempotent load 
    existing_trans_df = read_gold('fact_transaction')
    df_join = df_join.join(existing_trans_df, on='trans_num', how='left_anti')
    if df_join.head(1):
        print("adding new rows to fact_transaction!")
        write_gold(df_join, 'fact_transaction')
    else:
        print("no new rows to be added to fact_transaction")
    return read_gold('fact_transaction')

def main():
    silver_df = read_silver()
    category_df = pop_dim_category(silver_df)
    merchant_df = pop_dim_merchant(silver_df)
    customer_df = pop_dim_customer(silver_df)
    date_df = pop_dim_date(silver_df)
    pop_fact_transaction(silver_df, category_df, merchant_df, customer_df, date_df)


if __name__ == '__main__':
    main()