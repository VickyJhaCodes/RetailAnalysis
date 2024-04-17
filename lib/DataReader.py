from lib import ConfigReader

# defining customers schema
def get_customers_schema():
    schema = "customer_id int,customer_fname string,customer_lname string,username string,password string,address string,city string,state string,pincode string"
    return schema

#creating customers dataframe
def read_customers(spark,env):
    conf=ConfigReader.get_app_config(env)
    customer_file_path=conf["customers.file.path"]
    customers_df=spark.read. \
        format("csv").option("header","true"). \
        schema(get_customers_schema()).load(customer_file_path)
    return customers_df


# defining orders schema
def get_orders_schema():
    schema = "order_id string,order_date string,customer_id string,order_status string"
    return schema

#creating orders dataframe
def read_orders(spark,env):
    conf=ConfigReader.get_app_config(env)
    orders_file_path=conf["orders.file.path"]
    orders_df=spark.read. \
        format("csv").option("header","true"). \
        schema(get_orders_schema()).load(orders_file_path)
    return orders_df