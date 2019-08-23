import pandas as pd
import zipfile,os,sys
import re
from os import path
from sqlalchemy import create_engine
from functools import reduce
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import pyspark as p
import random
import logging
#logging.getLogger('py4j.java_gateway').setLevel(logging.INFO)
#logging.getLogger("py4j").setLevel(logging.INFO)
#logger = logging.getLogger('fraud_analytics')
#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filename='/tmp/fraud_analytics.log', filemode='w')
#hand=logging.FileHandler('fraud_analytics_{:%Y-%m-%d}.log'.format(datetime.now()))
#logger.addHandler(hand)


app_path = path.split(path.dirname(path.abspath(__file__)))[0]
print(app_path)
sys.path.append(app_path)

#logger.debug('setting spark configuration')
conf_values = [
 ('spark.executor.instances',"2"),
 ('spark.executor.cores',"1"),
 ('spark.exector.memory',"500")
]

conf = p.SparkConf().setAll(conf_values)
spark = SparkSession.builder.appName("fraud_analytics").config(conf=conf).getOrCreate()


Logger= spark._jvm.org.apache.log4j.Logger
logger = Logger.getLogger(__name__)
logger.error("some error trace")
logger.info("some info trace")


def mask_card(card_no):
    msk_card = card_no[:-9] + re.sub('.', '*', card_no[-9:])
    return msk_card

def get_col_size(st_nm):
    sz = sys.getsizeof(st_nm)
    return sz

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

fraud_file = os.environ.get('FRAUD')
trans1_file = os.environ.get('TRANS1')
trans2_file = os.environ.get('TRANS2')
post_user = os.environ.get('POSTUSER')
post_pwd = os.environ.get('POSTPWD')
#zip = zipfile.ZipFile('fraud.zip')
zip = zipfile.ZipFile(fraud_file)
df = pd.read_csv(zip.open('fraud'))
#engine = create_engine('postgres+psycopg2://postgres:pass123$@my_postgres:5432/creditdb')
engine = create_engine('postgres+psycopg2://%s:%s@my_postgres:5432/creditdb' % (post_user, post_pwd))
engine.execute("DROP TABLE IF EXISTS fraudtrans1" )
df.to_sql('fraudtrans1',engine)
maestro = ['5018', '5020', '5038', '56##']
mastercard = ['51', '52', '54', '55', '222%']
visa = ['4']
amex = ['34', '37']
discover = ['6011', '65']
diners = ['300', '301', '304', '305', '36', '38']
jcb16 = ['35']
jcb15 = ['2131', '1800']
columns = ['credit_card_number', 'ipv4', 'state']

zip_trans1 = zipfile.ZipFile(trans1_file)
#zip_trans1 = zipfile.ZipFile('transaction-001.zip')
df_iterator1 = pd.read_csv(zip_trans1.open('transaction-001'),sep=',',chunksize=50000)
ct=1
df_list1=[]
for df1 in df_iterator1:
    globals()["spdf" + str(ct)] = spark.createDataFrame(df1,columns)
    df_list1.append(globals()["spdf" + str(ct)])
    ct+=1

df_trans1 = unionAll(*df_list1)

cnt = 1
#zip_trans2 = zipfile.ZipFile('transaction-002.zip')
zip_trans2 = zipfile.ZipFile(trans2_file)
df_iterator2 = pd.read_csv(zip_trans2.open('transaction-002'),sep=',',chunksize=50000)
df_list2 = []
for df2 in df_iterator2:
    globals()["sp2df" + str(cnt)] = spark.createDataFrame(df2,columns)
    df_list2.append(globals()["sp2df" + str(cnt)])
    cnt+=1

df_trans2 = unionAll(*df_list2)

df_trans = unionAll(df_trans1,df_trans2)

df_trans_sub = df_trans.withColumn('card_4', df_trans['credit_card_number'].substr(1, 4)).withColumn('card_3', df_trans['credit_card_number'].substr(1, 3)).withColumn('card_2', df_trans['credit_card_number'].substr(1, 2)).withColumn('card_1', df_trans['credit_card_number'].substr(1, 1))

df_sntzed = df_trans_sub.filter(df_trans_sub.card_4.isin(maestro)|df_trans_sub.card_1.isin(visa)|df_trans_sub.card_2.isin(mastercard)|df_trans_sub.card_3.isin(mastercard)|df_trans_sub.card_2.isin(amex)|df_trans_sub.card_4.isin(discover)|df_trans_sub.card_2.isin(discover)|df_trans_sub.card_3.isin(diners)|df_trans_sub.card_2.isin(diners)|df_trans_sub.card_2.isin(jcb16)|df_trans_sub.card_4.isin(jcb15))

df_fraud_post = pd.read_sql_query('select * from "fraudtrans1"',engine)

spdf_fraud = spark.createDataFrame(df_fraud_post)

fraud_trans = df_sntzed.join(spdf_fraud, df_sntzed.credit_card_number == spdf_fraud.credit_card_number).select(df_sntzed["*"])

fraud_trans_by_state = fraud_trans.groupby(fraud_trans.state).count()

fraud_trans_vendor = fraud_trans.select(fraud_trans.credit_card_number,F.when(fraud_trans.card_4.isin(maestro),'maestro').when(fraud_trans.card_4.isin(jcb15),'jcb15').when(fraud_trans.card_2.isin(mastercard),'mastercard').when(fraud_trans.card_3.isin(mastercard),'mastercard').when(fraud_trans.card_1.isin(visa),'visa').when(fraud_trans.card_2.isin(amex),'amex').when(fraud_trans.card_4.isin(discover),'discover').when(fraud_trans.card_2.isin(discover),'discover').when(fraud_trans.card_3.isin(diners),'diners').when(fraud_trans.card_2.isin(diners),'diners').when(fraud_trans.card_2.isin(jcb16),'jcb16').alias("vendor"))

fraud_trans_by_vendor = fraud_trans_vendor.groupby(fraud_trans_vendor.vendor).count()

logger.info('fraud transactions by state...........')
output_dir_fbv = 'file:///tmp/fraudbyvendor' + str(random.randint(1,10000))
fraud_trans_by_vendor.coalesce(1).write.format('json').save(output_dir_fbv)
#logger.info(fraud_trans_by_state.show(10))

logger.info('fraud transactions by vendor............')
output_dir_fbs = 'file:///tmp/fraudbystate' + str(random.randint(1,10000))
fraud_trans_by_state.coalesce(1).write.format('json').save(output_dir_fbs)

#logger.info(fraud_trans_by_vendor.show(10))

mask_card_data = udf(lambda z: mask_card(z), StringType())

get_dat_sz = udf(lambda z: get_col_size(z), IntegerType())

trans_data_masked = df_trans.select(mask_card_data(df_trans.credit_card_number.cast('string')).alias('card_no'),df_trans.ipv4,df_trans.state,(get_dat_sz(df_trans.state)+get_dat_sz(df_trans.credit_card_number)+get_dat_sz(df_trans.ipv4)).alias('data_size_bytes'))

#output_dir = 'file:///Users/shankarsatheeshkumar/assignment/pci/outputjson' + str(random.randint(1,10000))
output_dir = 'file:///tmp/outputjson' + str(random.randint(1,10000))

output_dir_prq = 'file:///tmp/parq' + str(random.randint(1,10000))
#output_dir_prq = '/pci/parq' + str(random.randint(1,10000))

logger.info('Masked DataSet.............')

trans_data_masked.coalesce(1).write.format('json').save(output_dir)

trans_data_masked.coalesce(1).write.parquet(output_dir_prq)



logger.info('process complete!')
