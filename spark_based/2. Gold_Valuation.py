# Databricks notebook source
# MAGIC %md
# MAGIC # Valuation using Fundamental Analysis
# MAGIC
# MAGIC ## This notebook is based on the book "Accounting for Value by Stephen H. Penman"
# MAGIC
# MAGIC This notebook performs fundamental valuation of stocks and tries to calculate a value of the stock based on it's publicly reported numbers in Form 10K
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp, col, when

# COMMAND ----------

# MAGIC %md
# MAGIC # Set Risk Free Rate

# COMMAND ----------


#Risk Free Rate + Equity Premium
rfr_equity = 0.06

#Estimated Growth over next year
growth_next_year = 0.0

# COMMAND ----------

# MAGIC %md
# MAGIC #Import data from Balance Sheet

# COMMAND ----------

bronze = spark.sql("DESCRIBE EXTERNAL LOCATION `bronze`").select("url").collect()[0][0]

# COMMAND ----------

df_balance = spark.read.format("delta").table("dev_catalog.bronze.balance")

# COMMAND ----------

operational_col_BS = []
financial_col_BS = []
financial_list = ['Cash', 'Debt', 'Marketable', 'Securities', 'Fixed Income Investments', 'Long_Term']

for col in df_balance.columns: 
  if any(xs in col for xs in financial_list):
    financial_col_BS.append(col)
  else:
    operational_col_BS.append(col)

print(operational_col_BS)
print(financial_col_BS)

# COMMAND ----------

operational_df_BS = df_balance[operational_col_BS]
financial_df_BS = df_balance[financial_col_BS]

# COMMAND ----------

display(operational_df_BS)

# COMMAND ----------

operational_df_pandas_BS = operational_df_BS.toPandas()
operational_df_pandas_BS['Total_Operating_Assets'] = operational_df_pandas_BS.loc[:, : 'Total_Assets'].sum(axis=1)
print (operational_df_pandas_BS)

# COMMAND ----------

operational_df_pandas_BS['Total_Operating_Liability'] = (operational_df_pandas_BS.loc[:, 'Total_Assets':'Total_Liabilities'].sum(axis=1)) - operational_df_pandas_BS['Total_Assets']
print (operational_df_pandas_BS)

# COMMAND ----------

operational_df_pandas_BS['Net_Operating_Assets'] = operational_df_pandas_BS['Total_Operating_Assets'] - operational_df_pandas_BS['Total_Operating_Liability']
print (operational_df_pandas_BS)

# COMMAND ----------

financial_df_pandas_BS = financial_df_BS.toPandas()

# COMMAND ----------

financial_df_pandas_BS['Financial_Assets'] = financial_df_pandas_BS.iloc[:, 0:2].sum(axis=1)
print (financial_df_pandas_BS)


# COMMAND ----------

financial_df_pandas_BS['Financial_Debts'] = financial_df_pandas_BS.iloc[:, 2:4].sum(axis=1)
print (financial_df_pandas_BS)


# COMMAND ----------

financial_df_pandas_BS['Net_Debt'] = financial_df_pandas_BS['Financial_Assets'] - financial_df_pandas_BS['Financial_Debts']
print (financial_df_pandas_BS)

# COMMAND ----------

df_income = spark.read.format("delta").table("dev_catalog.bronze.income")

# COMMAND ----------

operational_col_PL = []
financial_col_PL = []
financial_list = ['Cash', 'Debt', 'Marketable', 'Securities', 'Fixed_Income_Investments', 'Non_Operating', 'Abnormal_Gains_Losses', 'Net_Extraordinary_Gains_Losses']

for col in df_income.columns: 
  if any(xs in col for xs in financial_list):
    financial_col_PL.append(col)
  else:
    operational_col_PL.append(col)

print(operational_col_PL)
print(financial_col_PL)

# COMMAND ----------

operational_df_PL = df_income[operational_col_PL]
financial_df_PL = df_income[financial_col_PL]

# COMMAND ----------

operational_df_pandas_PL = operational_df_PL.toPandas()

# COMMAND ----------

operational_df_pandas_PL.fillna(0, inplace=True)
operational_df_pandas_PL.replace(',','', regex=True, inplace=True)
operational_df_pandas_PL.replace('',0, inplace=True)

# COMMAND ----------

display(operational_df_pandas_PL)

# COMMAND ----------

operational_df_pandas_PL['Net_Operating_Income'] = operational_df_pandas_PL['Revenue'] + operational_df_pandas_PL['Cost_of_Revenue'] + operational_df_pandas_PL['Operating_Expenses'] + operational_df_pandas_PL['Income_Tax_Expense_Benefit_Net']


# COMMAND ----------

operational_df_pandas_PL['Net_Operating_Assets'] = operational_df_pandas_BS['Net_Operating_Assets']

# COMMAND ----------

# MAGIC %md
# MAGIC #Residual Operating Income without Leverage

# COMMAND ----------

operational_df_pandas_PL['Residual_Operating_Income'] = operational_df_pandas_PL['Net_Operating_Income'] - (operational_df_pandas_BS['Net_Operating_Assets'] * rfr_equity)

# COMMAND ----------

operational_df_pandas_PL['Expected_Residual_Operating_Income_Next_Year'] = operational_df_pandas_PL['Residual_Operating_Income'] + (operational_df_pandas_PL['Residual_Operating_Income'] * growth_next_year)

# COMMAND ----------

operational_df_pandas_PL['Discounted_Present_Value'] = operational_df_pandas_PL['Expected_Residual_Operating_Income_Next_Year'] / (((1+rfr_equity)**2) * rfr_equity)

# COMMAND ----------

operational_df_pandas_PL['Value_of_Operations'] = operational_df_pandas_BS['Net_Operating_Assets'] + operational_df_pandas_PL['Residual_Operating_Income'] + operational_df_pandas_PL['Expected_Residual_Operating_Income_Next_Year'] + operational_df_pandas_PL['Discounted_Present_Value']

# COMMAND ----------

operational_df_pandas_PL['Equity_Value'] = operational_df_pandas_PL['Value_of_Operations'] + financial_df_pandas_BS['Net_Debt']

# COMMAND ----------

operational_df_pandas_PL.head()

# COMMAND ----------

df_prices = spark.read.format("delta").table("dev_catalog.bronze.prices")

# COMMAND ----------

df_prices_pandas = df_prices.toPandas()

# COMMAND ----------

df_prices_pandas

# COMMAND ----------

df_prices_pandas.dropna(how='all', inplace=True)
df_prices_pandas.head()

# COMMAND ----------

df_prices_pandas.fillna(0, inplace=True)
df_prices_pandas.replace(',','', regex=True, inplace=True)
df_prices_pandas.replace('',0, inplace=True)

# COMMAND ----------

df_prices_pandas.head()

# COMMAND ----------

df_prices_pandas["SharesOutstanding"].head()

# COMMAND ----------

df_prices_pandas = df_prices_pandas[df_prices_pandas["SharesOutstanding"]>0]

# COMMAND ----------

df_prices_pandas.head()

# COMMAND ----------

df_prices_by_year = df_prices_pandas \
                            .groupby(
                                [
                                    df_prices_pandas.Date.dt.year, 
                                    df_prices_pandas["SimFinId"],
                                    df_prices_pandas["Ticker"]
                                ]
                                ) \
                            .agg(
                                    {
                                        'AdjClose': 'mean',
                                        'SharesOutstanding': 'mean'
                                    }
                                )

# COMMAND ----------

df_prices_by_year.head()

# COMMAND ----------

df_prices_by_year = df_prices_by_year.reset_index()


# COMMAND ----------

df_prices_by_year["Date"].head()

# COMMAND ----------

df_prices_by_year["Market_Cap"] = df_prices_by_year["AdjClose"] * df_prices_by_year["SharesOutstanding"]

# COMMAND ----------

df_prices_by_year.head()

# COMMAND ----------

df_valuation = operational_df_pandas_PL.merge(df_prices_by_year, left_on=['Ticker', 'Fiscal_Year'], right_on=['Ticker', 'Date'], how='inner')


# COMMAND ----------

df_valuation['Equity_Value_Per_Share'] = df_valuation['Equity_Value'] / (df_valuation['Market_Cap']/(df_valuation['AdjClose'] ))
# * 1000000


# COMMAND ----------

df_valuation.head()

# COMMAND ----------

df_spark = spark.createDataFrame(df_valuation)

# COMMAND ----------

df_spark = df_spark.withColumnRenamed("AdjClose", "Avg_Yearly_Market_Price")

# COMMAND ----------

df_spark = df_spark.withColumn("Valuation", when(col("Equity_Value_Per_Share") > col("Avg_Yearly_Market_Price"), "Undervalued").otherwise("Overvalued"))

# COMMAND ----------

display(df_spark)

# COMMAND ----------

df_spark.write.mode("overwrite").format("delta").saveAsTable("`dev_catalog`.`gold`.`gold_valuation`")

# COMMAND ----------


