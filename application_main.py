import sys
from lib import DataManipulation,DataReader,Utils
from pyspark.sql.functions import *
from lib.logger import Log4j



if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]
    
    print("Creating SparkSession...")
    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)
    logger.info("SparkSession created..")

    customers_df = DataReader.read_customers(spark,job_run_env)
    orders_df = DataReader.read_orders(spark,job_run_env)

    orders_filtered = DataManipulation.filter_closed_orders(orders_df)

    joined_df = DataManipulation.join_order_customers(orders_filtered,customers_df)

    aggregated_results=DataManipulation.count_orders_state(joined_df)

    aggregated_results.show()

    logger.warn("End of main..")
