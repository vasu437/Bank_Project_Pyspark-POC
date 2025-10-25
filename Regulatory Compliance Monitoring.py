def monitor_regulatory_compliance(transactions_df, accounts_df, customers_df):
    """Monitor transactions for regulatory compliance (AML/KYC)"""
    from pyspark.sql import functions as F
    
    compliance_monitoring = transactions_df.join(accounts_df, "account_id") \
        .join(customers_df, "customer_id") \
        .withColumn("is_high_value", F.col("amount") > 10000) \
        .withColumn("is_suspicious_pattern",
                   F.when(
                       (F.col("amount") > 5000) & 
                       (F.col("transaction_type") == "CASH_DEPOSIT"), True
                   ).when(
                       (F.col("amount") > 8000) & 
                       (F.col("transaction_type") == "TRANSFER") &
                       (F.col("description").like("%offshore%")), True
                   ).otherwise(False)) \
        .withColumn("compliance_alert_level",
                   F.when(F.col("is_high_value") & F.col("is_suspicious_pattern"), "HIGH")
                    .when(F.col("is_high_value"), "MEDIUM")
                    .when(F.col("is_suspicious_pattern"), "MEDIUM")
                    .otherwise("LOW")) \
        .filter(F.col("compliance_alert_level").isin("HIGH", "MEDIUM")) \
        .groupBy("customer_id", "compliance_alert_level") \
        .agg(
            F.count("*").alias("alert_count"),
            F.sum("amount").alias("total_alert_amount"),
            F.collect_list("transaction_id").alias("suspicious_transactions")
        )
    
    return compliance_monitoring