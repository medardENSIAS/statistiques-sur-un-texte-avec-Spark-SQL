from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import udf ,col
from pyspark.sql.functions import regexp_replace
import json
import math

schema = StructType([StructField('datehour', StringType(), False),
               StructField('domain', StringType(), False),
               StructField('country', StringType(), False),
               StructField('pageviews', IntegerType(), False),
               StructField('pageviews_with_consent', IntegerType(), False),
               StructField('consents_asked', IntegerType(), False),
               StructField('consents_asked_with_consent', IntegerType(), False),
               StructField('consent_given', IntegerType(), False),
               StructField('consents_given_with_consent', IntegerType(), False),
               StructField('avg_pageviews_per_user', StringType(), True)])

def get_df_type(spark_session = None , df = None , type_ = ''):
    # This function return some metrics for specific event type take in input.
    #     type_count_by_domain_by_country : is a number of type per datehour / domain /country
    #     Consent_by_domain_by_country: is a number of type of Consent_status group by  datehour /domain /country
    #     Consent_by_type_domain_by_country: is a number of type of Consent_status group by  Consent_status / datehour /domain /country
    
    distinct_df = df.filter(col('type') == type_)
    distinct_df.createOrReplaceTempView("distinct_df_table")

    string = """SELECT DISTINCT * FROM
            (SELECT t.datehour , 
            t.domain,
            t.type,
            t.country,
            count(t.type) OVER (PARTITION BY t.datehour , t.domain , t.country) as type_count_by_domain_by_country,
            Consent_status,
            COUNT(Consent_status) OVER (PARTITION BY  t.datehour , t.domain , t.country) as Consent_by_domain_by_country,
            COUNT(Consent_status) OVER (PARTITION BY  t.datehour , t.Consent_status, t.domain , t.country) as Consent_by_type_domain_by_country
            FROM distinct_df_table t) tb ORDER BY tb.Consent_status DESC"""
    df_interm = spark_session.sql(string)
    return df_interm.dropDuplicates(["datehour","domain","country"])

def get_metric(row = None , datehour = ''):

    if row['type'] == 'pageview' and row['datehour'] == datehour:
        return {'pageviews': row['type_count_by_domain_by_country'] ,
                'pageviews_with_consent': row['Consent_by_type_domain_by_country'] if row['Consent_status'] == 'enabled' else 0,
                'avg_pageviews_per_user': math.ceil(row['type_count_by_domain_by_country'] / row['count_distinct_user'])}
    elif row['type'] == 'consent.asked' and row['datehour'] == datehour:
        return {'consents_asked': row['type_count_by_domain_by_country'] ,
                'consents_asked_with_consent': row['Consent_by_type_domain_by_country'] if row['Consent_status'] == 'enabled' else 0}
    elif row['type'] == 'consent.given' and row['datehour'] == datehour:
        return {'consent_given': row['type_count_by_domain_by_country'] ,
                'consents_given_with_consent': row['Consent_by_type_domain_by_country'] if row['Consent_status'] == 'enabled' else 0}
    else:
        return None
    
def consent_status(user = {}):
#     I consider an event as Negative consent when the variable user -> token -> purposes -> enabled 
#     is missing or empty 
    if 'token' in user:
        token = json.loads(user['token'])
        if 'purposes' in token and 'enabled' in token['purposes'] and len(token['purposes']['enabled']):
            return 'enabled'
        else:
            return 'disabled'
    else:
        return 'disabled'
    
def get_average_pageview(spark_session =  None , df = None):

    #the function return distinct user number of event type pageview 
    #group by datehour/domain/country
    
    df.createOrReplaceTempView("distinct_df_table")
    string = """SELECT t.datehour as datehour ,
                t.country as country , t.domain as domain,
                COUNT( DISTINCT t.user.id) as count_distinct_user FROM distinct_df_table t
                WHERE t.type = 'pageview' GROUP BY t.datehour , t.domain ,t.country ;"""
    return spark_session.sql(string)

def get_list_of_metric(df = None):
    list_raw = df.collect()
    for row in list_raw:
        value = {'datehour': row['datehour'],
                'domain': row['domain'],
                'country': row['country'],
                'pageviews': 0,
                'pageviews_with_consent': 0,
                'consents_asked': 0,
                'consents_asked_with_consent': 0,
                'consent_given': 0 ,
                'consents_given_with_consent': 0 ,
                'avg_pageviews_per_user': ''}
        value.update(get_metric(row,row['datehour']))
        for row1 in df.collect():
            if (row['datehour'] == row1['datehour']) and (row['domain'] == row1['domain'])                 and (row['country'] == row1['country'])                and (row['type'] != row1['type']):
                metric = get_metric(row1,row['datehour'])
                if metric:
                    value.update(metric)
                    list_raw.remove(row1)
        yield value
        
def write(df = None , parquet_path = '' ):
    #write parquet file partitioned by datehour variable 
    try:
        df.write.partitionBy("datehour").mode("overwrite").parquet(parquet_path)
        print("----------------------------------------Parquet file path--------------------------\n")
        print(f'File write in  {parquet_path}')
        print("------------------------------------------------------------------------------------\n")
    except Exception as e:
        print('File writing error ')
        raise e

def main(spark_session = None, list_path = [] , parquet_path = "/Users/tchamdja/Documents/PySpark/Didomi/output/metric.parquet"):
    #create Spark session
    df = spark_session.read.json(list_path)
    #Preprossing 
    Consent_status = udf(lambda user: consent_status(user.asDict()),StringType())
    df = df.withColumn("Consent_status", Consent_status(col("user")))
    df = df.withColumn("country", col('user.country'))
    df = df.withColumn("datehour", regexp_replace(col('datetime').substr(1,13),' ','-'))# add Date and hour (YYYY-MM-DD-HH) dimension
    
    #print Deduplication of events
    print("------------------------------Deduplication of events-----------------------------------\n")
    print(df.groupBy("datehour","id").count().where("count > 1").toPandas())
    print("------------------------------"*3 + "\n")
    
    #select distinct events 
    distinct_df = df.dropDuplicates(["datehour","id"])
    
    # Events type pageview 
    df1 = get_df_type(spark_session = spark_session , df = distinct_df , type_ = 'pageview')
    df_interm1 = get_average_pageview(spark_session = spark_session , df = distinct_df)
    df1 = (df1.join(df_interm1,(df1.domain ==  df_interm1.domain)\
                    &(df1.country ==  df_interm1.country)\
                    &(df1.datehour ==  df_interm1.datehour)\
                    &(df1.type == 'pageview'),"left"))\
        .drop(df_interm1.domain).drop(df_interm1.country).drop(df_interm1.datehour)
    # Events type consent.given 
    df2 = get_df_type(spark_session = spark_session, df = distinct_df , type_ = 'consent.given')
    # Events type consent.asked
    df3 = get_df_type(spark_session = spark_session, df = distinct_df , type_ = 'consent.asked')
    # Union all events types
    df_interm = (df2.union(df3)).unionByName(df1 , allowMissingColumns=True)
    #
    list_of_metric = list(get_list_of_metric(df = df_interm))
    #create dataframe
    output_df = spark_session.createDataFrame(list_of_metric,schema)
    write(df = output_df , parquet_path = parquet_path )
    return output_df


