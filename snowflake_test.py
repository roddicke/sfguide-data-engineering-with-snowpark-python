import os
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

connection_parameters = {"account":"cf15705.ap-northeast-1.aws",
                         "user":"roddicke",
                         "password":"Doris0718!",
                         "role":"ACCOUNTADMIN",
                         "warehouse":"COMPUTE_WH",
                         "database":"DEMO_DB",
                         "schema":"PUBLIC"}

session = Session.builder.configs(connection_parameters).create()
session.sql("USE WAREHOUSE COMPUTE_WH").collect()

df_customer_info = session.table("snowflake_sample_data.tpch_sf1.customer")
df_customer_filter = df_customer_info.filter(col("C_MKTSEGMENT") == 'HOUSEHOLD')
df_customer_select = df_customer_info.select(col("C_MKTSEGMENT"),col("C_ADDRESS"))
df_customer_select.show()
df_customer_select.count()

df_customer_select.describe().sort("SUMMARY").show()
