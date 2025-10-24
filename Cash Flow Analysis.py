def analyze_cash_flow_patterns(transactions_df, accounts_df, customers_df):
    """Analyze incoming/outgoing cash flows with seasonality"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    cash_flow_analysis = transactions_df.join(accounts_df, "account_id") \
        .withColumn("transaction_month", F.date_trunc("month", "transaction_date")) \
        .withColumn("flow_type", 
                   F.when(F.col("transaction_type").isin("DEPOSIT", "TRANSFER"), "INFLOW")
                    .when(F.col("transaction_type").isin("WITHDRAWAL", "PAYMENT"), "OUTFLOW")
                    .otherwise("OTHER")) \
        .groupBy("customer_id", "transaction_month", "flow_type") \
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount")
        ) \
        .groupBy("customer_id", "flow_type") \
        .agg(
            F.avg("total_amount").alias("avg_monthly_flow"),
            F.stddev("total_amount").alias("flow_volatility"),
            F.sum("total_amount").alias("total_flow"),
            F.count("*").alias("months_analyzed")
        ) \
        .withColumn("net_flow_ratio", 
                   F.when(F.col("flow_type") == "INFLOW", 1)
                    .when(F.col("flow_type") == "OUTFLOW", -1)
                    .otherwise(0) * F.col("avg_monthly_flow")) \
        .groupBy("customer_id") \
        .pivot("flow_type") \
        .sum("avg_monthly_flow", "flow_volatility") \
        .fillna(0)
    
    return cash_flow_analysis