set -x
current_dir=/tmp/src
log4j_setting="-Dlog4j.configuration=file:log4j.properties"


cd /tmp/src/
spark-submit --conf "spark.driver.extraJavaOptions=${log4j_setting}" --conf "spark.executor.extraJavaOptions=${log4j_setting}" fraud_analytics.py --files ${current_dir}/log4j.properties
#spark-submit fraud_analytics.py 
