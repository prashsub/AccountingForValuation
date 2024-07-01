# Databricks notebook source
# MAGIC %md
# MAGIC Setting Environment

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# dbutils.widgets.text(name="env",defaultValue="",label=" Enter the environment in lower case")
# env = dbutils.widgets.get("env")

# COMMAND ----------

!pip install simfin


# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

key = dbutils.secrets.get(scope = "SimFinAPIKey", key = "SimFinAPIKey") 

# COMMAND ----------

import simfin as sf
from simfin.names import *

# Set your API-key for downloading data.
# Replace YOUR_API_KEY with your actual API-key.
sf.set_api_key(key)

# Set the local directory where data-files are stored.
# The dir will be created if it does not already exist.
sf.set_data_dir('~/simfin_data/')

# COMMAND ----------



# Load the annual Income Statements for all companies in the US.
# The data is automatically downloaded if you don't have it already.
df = sf.load_income(variant='annual', market='us')

# Print all Revenue and Net Income for Microsoft (ticker MSFT).
print(df.loc['MSFT', [REVENUE, NET_INCOME]])

# COMMAND ----------

# Load daily share-prices for all companies in USA.
# The data is automatically downloaded if you don't have it already.
df_prices = sf.load_shareprices(market='us', variant='daily')

# Plot the closing share-prices for ticker MSFT.
df_prices.loc['MSFT', CLOSE].plot(grid=True, figsize=(20,10), title='MSFT Close')

# COMMAND ----------

print(df_prices)

# COMMAND ----------

df_prices.reset_index(drop=False,inplace=True)
df_spark_prices = spark.createDataFrame(df_prices)

# COMMAND ----------

df_spark_prices.printSchema()

# COMMAND ----------

df_spark_prices = df_spark_prices.withColumnRenamed("Adj. Close", "AdjClose",)
df_spark_prices = df_spark_prices.withColumnRenamed("Shares Outstanding", "SharesOutstanding",)

# COMMAND ----------

df_spark_prices.printSchema()

# COMMAND ----------

# spark.sql("CREATE TABLE accountingforvaluation.prices "
#   "("
#   "SimFinId STRING,"
#   "Open DECIMAL,"
#   "High DECIMAL,"
#   "Low DECIMAL,"
#   "Close DECIMAL,"
#   "AdjClose DECIMAL,"
#   "Volume STRING,"
#   "Dividend DECIMAL,"
#   "SharesOutstanding DECIMAL"
#   ") "
#   "LOCATION 'abfss://raw@formula1dludemyprashanth.dfs.core.windows.net/accountingforvaluation'")

# COMMAND ----------

# Load daily share-prices for all companies in USA.
# The data is automatically downloaded if you don't have it already.
df_spark_prices.writeTo(f"{env}_catalog.bronze.prices").createOrReplace()

# COMMAND ----------


df_income = sf.load(dataset='income', variant='annual', market='us')
print(df_income)


# COMMAND ----------

df_income.reset_index(drop=False,inplace=True)


# COMMAND ----------

df_spark_income = spark.createDataFrame(df_income)

# COMMAND ----------

df_spark_income.printSchema()

# COMMAND ----------

df_spark_income = df_spark_income \
                    .withColumnRenamed("Shares (Basic)", "Shares_Basic") \
                    .withColumnRenamed("Shares (Diluted)", "Shares_Diluted") \
                    .withColumnRenamed("Selling, General & Administrative", "Selling_General_Administrative") \
                    .withColumnRenamed("Operating Income (Loss)", "Operating_Income_Loss") \
                    .withColumnRenamed("Non-Operating Income (Loss)", "Non_Operating_Income_Loss") \
                    .withColumnRenamed("Interest Expense, Net", "Interest_Expense_Net") \
                    .withColumnRenamed("Abnormal Gains (Losses)", "Abnormal_Gains_Losses") \
                    .withColumnRenamed("Pretax Income (Loss), Adj.", "Pretax_Income_Loss_Adj") \
                    .withColumnRenamed("Pretax Income (Loss)", "Pretax_Income_Loss") \
                    .withColumnRenamed("Income Tax (Expense) Benefit, Net", "Income_Tax_Expense_Benefit_Net") \
                    .withColumnRenamed("Income (Loss) from Continuing Operations", "Income_Loss_from_Continuing_Operations") \
                    .withColumnRenamed("Net Extraordinary Gains (Losses)", "Net_Extraordinary_Gains_Losses") \
                    .withColumnRenamed("Net Income (Common)", "Net_Income_Common")

# COMMAND ----------

df_spark_income.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Replace Spaces by Underscore

# COMMAND ----------

df_spark_income = df_spark_income.select([F.col(col).alias(col.replace(' ', '_')) for col in df_spark_income.columns])

# COMMAND ----------

df_spark_income.printSchema()

# COMMAND ----------

df_spark_income.writeTo(f"{env}_catalog.bronze.income").createOrReplace()

# COMMAND ----------

# df_balance_test = sf.load(dataset='balance', variant='annual', market=  'us')
df_balance = sf.load_balance(variant='annual', market='us')
print(df_balance_test)

# COMMAND ----------

df_balance.reset_index(drop=False,inplace=True)

# COMMAND ----------

df_spark_balance = spark.createDataFrame(df_balance)

# COMMAND ----------

df_spark_balance.printSchema()

# COMMAND ----------

df_spark_balance = df_spark_balance \
.withColumnRenamed("Shares (Basic)", "Shares_Basic") \
.withColumnRenamed("Shares (Diluted)", "Shares_Diluted") \
.withColumnRenamed("Cash, Cash Equivalents & Short Term Investments", "Cash Equivalents & Short Term Investments") \
.withColumnRenamed("Property, Plant & Equipment, Net", "Property_Plant_Equipment_Net")

# COMMAND ----------

df_spark_balance = df_spark_balance.select([F.col(col).alias(col.replace(' ', '_') ) for col in df_spark_balance.columns])

# COMMAND ----------

df_spark_balance.printSchema()

# COMMAND ----------

df_spark_balance.writeTo(f"{env}_catalog.bronze.balance").createOrReplace()

# COMMAND ----------

# df_cashflow = sf.load_cashflow(variant='annual', market='us')
df_cashflow = sf.load(dataset='cashflow', variant='annual', market='us')
print(df_cashflow)

# COMMAND ----------

df_cashflow.reset_index(drop=False,inplace=True)

# COMMAND ----------

df_spark_cashflow = spark.createDataFrame(df_cashflow)

# COMMAND ----------

df_spark_cashflow.printSchema()

# COMMAND ----------



# COMMAND ----------

df_spark_cashflow = df_spark_cashflow.select([F.col(col).alias(col.replace(' ', '_') ) for col in df_spark_cashflow.columns])
