#!/usr/bin/env python
# coding: utf-8

# ## bronze_to_silver
# 
# null

# In[1]:


customers_raw = spark.read.parquet("abfss://ecommerce@onelake.dfs.fabric.microsoft.com/ecommerce_lakehouse.Lakehouse/Files/bronze/customers.parquet")
orders_raw = spark.read.parquet("abfss://ecommerce@onelake.dfs.fabric.microsoft.com/ecommerce_lakehouse.Lakehouse/Files/bronze/orders.parquet")
payments_raw = spark.read.parquet("abfss://ecommerce@onelake.dfs.fabric.microsoft.com/ecommerce_lakehouse.Lakehouse/Files/bronze/payments.parquet")
support_raw = spark.read.parquet("abfss://ecommerce@onelake.dfs.fabric.microsoft.com/ecommerce_lakehouse.Lakehouse/Files/bronze/support_tickets.parquet")
web_raw = spark.read.parquet("abfss://ecommerce@onelake.dfs.fabric.microsoft.com/ecommerce_lakehouse.Lakehouse/Files/bronze/web_activities.parquet")


# In[2]:


display(customers_raw)


# ### _**SAVE THE DATA INTO DELTA BRONZE FORMAT IN BRONZE TABLE**_

# In[3]:


# Save as Bronze Delta Tables
customers_raw.write.format("delta").mode("overwrite").saveAsTable("customers")
orders_raw.write.format("delta").mode("overwrite").saveAsTable("orders")
payments_raw.write.format("delta").mode("overwrite").saveAsTable("payments")
support_raw.write.format("delta").mode("overwrite").saveAsTable("support")
web_raw.write.format("delta").mode("overwrite").saveAsTable("web")


# ### _**DATA CLEANING FOR SILVER STAGE**_

# In[4]:


display(customers_raw)


# - ## _**clean customer table**_

# In[5]:


from pyspark.sql.functions import *
from pyspark.sql.types import *


customers_clean = (
    customers_raw
    .withColumn("email", lower(trim(col("EMAIL"))))
    .withColumn("name", initcap(trim(col("name"))))
    .withColumn("gender", when(lower(col("gender")).isin("f", "female"), "Female")
                          .when(lower(col("gender")).isin("m", "male"), "Male")
                          .otherwise("Other"))
    .withColumn("dob", to_date(regexp_replace(col("dob"), "/", "-")))
    .withColumn("location", initcap(col("location")))
    .dropDuplicates(["customer_id"])
    .dropna(subset=["customer_id", "email"])
)

display(customers_clean.limit(6))


# In[10]:


customers_clean.write.format("delta").mode("overwrite").saveAsTable("silver_customers")


# -  ## _**clean orders table**_

# In[7]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT*
# FROM orders
# LIMIT 5


# In[8]:


orders = spark.table("orders")
orders_clean = (
    orders
    .withColumn("order_date", 
                when(col("order_date").rlike("^\d{4}/\d{2}/\d{2}$"), to_date(col("order_date"), "yyyy/MM/dd"))
                .when(col("order_date").rlike("^\d{2}-\d{2}-\d{4}$"), to_date(col("order_date"), "dd-MM-yyyy"))
                .when(col("order_date").rlike("^\d{8}$"), to_date(col("order_date"), "yyyyMMdd"))
                .otherwise(to_date(col("order_date"), "yyyy-MM-dd")))
    .withColumn("amount", col("amount").cast(DoubleType()))
    .withColumn("amount", when(col("amount") < 0, None).otherwise(col("amount")))
    .withColumn("status", initcap(col("status")))
    .dropna(subset=["customer_id", "order_date"])
    .dropDuplicates(["order_id"])
)
display(orders_clean.limit(5))


# In[9]:


orders_clean.write.format("delta").mode("overwrite").saveAsTable("silver_orders")


# - ## _**clean payment table**_

# In[12]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT*
# FROM payments
# LIMIT 5


# In[13]:


payments = spark.table("payments")
payments_clean = (
    payments
    .withColumn("payment_date", to_date(regexp_replace(col("payment_date"), "/", "-")))
    .withColumn("payment_method", initcap(col("payment_method")))
    .replace({"creditcard": "Credit Card"}, subset=["payment_method"])
    .withColumn("payment_status", initcap(col("payment_status")))
    .withColumn("amount", col("amount").cast(DoubleType()))
    .withColumn("amount", when(col("amount") < 0, None).otherwise(col("amount")))
    .dropna(subset=["customer_id", "payment_date", "amount"])
)
display(payments_clean.limit(5))

payments_clean.write.format("delta").mode("overwrite").saveAsTable("silver_payments")


# - ## _**clean support table**_

# In[15]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT*
# FROM support
# LIMIT 5


# In[16]:


support = spark.table("support")
support_clean = (
    support
    .withColumn("ticket_date", to_date(regexp_replace(col("ticket_date"), "/", "-")))
    .withColumn("issue_type", initcap(trim(col("issue_type"))))
    .withColumn("resolution_status", initcap(trim(col("resolution_status"))))
    .replace({"NA": None, "": None}, subset=["issue_type", "resolution_status"])
    .dropDuplicates(["ticket_id"])
    .dropna(subset=["customer_id", "ticket_date"])
)
display(support_clean.limit(5))

support_clean.write.format("delta").mode("overwrite").saveAsTable("silver_support")


# - ## _**clean web table**_

# In[18]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT*
# FROM web
# LIMIT 5 


# In[19]:


web = spark.table("web")
web_clean = (
    web
    .withColumn("session_time", to_date(regexp_replace(col("session_time"), "/", "-")))
    .withColumn("page_viewed", lower(col("page_viewed")))
    .withColumn("device_type", initcap(col("device_type")))
    .dropDuplicates(["session_id"])
    .dropna(subset=["customer_id", "session_time", "page_viewed"])
)
display(web_clean.limit(5))

web_clean.write.format("delta").mode("overwrite").saveAsTable("silver_web")


# ### _**GOLD TABLE FOR AGRREGATION FOR POWER BI REPORTING**_

# In[20]:


cust = spark.table("silver_customers").alias("c")
orders = spark.table("silver_orders").alias("o")
payments = spark.table("silver_payments").alias("p")
support = spark.table("silver_support").alias("s")
web = spark.table("silver_web").alias("w")

customer360 = (
    cust
    .join(orders, "customer_id", "left")
    .join(payments, "customer_id", "left")
    .join(support, "customer_id", "left")
    .join(web, "customer_id", "left")
    .select(
        col("c.customer_id"),
        col("c.name"),
        col("c.email"),
        col("c.gender"),
        col("c.dob"),
        col("c.location"),

        col("o.order_id"),
        col("o.order_date"),
        col("o.amount").alias("order_amount"),
        col("o.status").alias("order_status"),

        col("p.payment_method"),
        col("p.payment_status"),
        col("p.amount").alias("payment_amount"),

        col("s.ticket_id"),
        col("s.issue_type"),
        col("s.ticket_date"),
        col("s.resolution_status"),

        col("w.page_viewed"),
        col("w.device_type"),
        col("w.session_time")
    )
)
display(customer360.limit(10))

customer360.write.format("delta").mode("overwrite").saveAsTable("gold_customer360")

