from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import avg, sum, col,lit
import configparser
import pandas as pd

parser = configparser.ConfigParser()
parser.read("config.yml")
user = parser.get("Credentials", "username")
password = parser.get("Credentials", "password")
acctName = parser.get("Credentials", "account")
wh = parser.get("Credentials", "warehouse")
dbname = parser.get("Credentials", "database")
schema = parser.get("Credentials", "schema")
role = parser.get("Credentials", "role")

import streamlit as st
st.set_page_config(
page_title="COVID-19 Epidemiological Data",
page_icon="🧊",
layout="wide",
initial_sidebar_state="expanded",
menu_items={
'Get Help': 'https://developers.snowflake.com',
'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and Snowflake Data Marketplace"
}
)

connection_parameters = {
   "account": acctName,
   "user": user,
   "password": password,
   "warehouse": wh,
   "role": role,
   "database": dbname,
   "schema": schema
}
session = Session.builder.configs(connection_parameters).create()


# test if we have a connection
session.sql("select current_warehouse() wh, current_database() db, current_schema() schema, current_version() v,current_role() role").show()

snow_df_covid_cases = session.table("ECDC_GLOBAL")
snow_df_covid_cases = snow_df_covid_cases.group_by('COUNTRY_REGION').agg(sum('CASES').alias("Total Number of COVID Cases")).sort('COUNTRY_REGION')
snow_df_covid_cases.show()


list_df_covid_cases = snow_df_covid_cases.collect()

pandas_df = pd.DataFrame(list_df_covid_cases, columns=["COUNTRY","Total Number of COVID Cases"])

print(type(pandas_df))

print(pandas_df)
st.header("Starschema: COVID-19 Epidemiological Data")
st.subheader("Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit")

with st.container():
   st.subheader('Number of Covid Cases by Country')
   st.dataframe(pandas_df)


with st.container():
   st.subheader('Number of COVID Cases in Top 10 Countries')
   with st.expander(""):
      pd_top_n = pandas_df.sort_values('Total Number of COVID Cases', ascending=False).head(10)
      st.bar_chart(data=pd_top_n.set_index('COUNTRY'), width=850, height=500, use_container_width=True)


filtered_df=snow_df_covid_cases.filter(col('COUNTRY_REGION')=="India")

filtered_df.show()