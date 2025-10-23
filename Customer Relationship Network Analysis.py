def analyze_customer_relationships(customers_df, account_beneficiaries_df, accounts_df):
    """Analyze customer relationships through beneficiaries and joint accounts"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Beneficiary relationships
    beneficiary_relationships = account_beneficiaries_df \
        .join(accounts_df, "account_id") \
        .groupBy("customer_id", "relationship") \
        .agg(F.count("*").alias("relationship_count")) \
        .groupBy("customer_id") \
        .pivot("relationship") \
        .sum("relationship_count") \
        .fillna(0)
    
    # Financial network strength
    network_strength = account_beneficiaries_df \
        .join(accounts_df, "account_id") \
        .groupBy("customer_id") \
        .agg(
            F.countDistinct("account_id").alias("connected_accounts"),
            F.sum("balance").alias("total_network_balance"),
            F.countDistinct("beneficiary_id").alias("unique_connections")
        ) \
        .withColumn("network_influence", 
                   F.col("total_network_balance") * F.col("unique_connections"))
    
    customer_network = customers_df \
        .join(beneficiary_relationships, "customer_id", "left") \
        .join(network_strength, "customer_id", "left") \
        .fillna(0) \
        .withColumn("relationship_diversity",
                   F.when(F.col("SPOUSE") > 0, "FAMILY")
                    .when(F.col("BUSINESS_PARTNER") > 0, "BUSINESS")
                    .when(F.col("unique_connections") > 5, "EXTENSIVE")
                    .otherwise("LIMITED"))
    
    return customer_network