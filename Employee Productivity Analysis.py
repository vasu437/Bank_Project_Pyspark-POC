def analyze_employee_productivity(employees_df, loans_df, accounts_df, branches_df):
    """Comprehensive productivity metrics across positions"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    employee_metrics = employees_df \
        .join(loans_df.groupBy("approved_by")
              .agg(F.count("*").alias("loans_approved"),
                   F.sum("loan_amount").alias("total_loan_volume")), 
              employees_df.employee_id == loans_df.approved_by, "left") \
        .join(accounts_df.groupBy("branch_id")
              .agg(F.count("*").alias("branch_accounts")), "branch_id") \
        .withColumn("tenure_months", 
                   F.months_between(F.current_date(), "hire_date")) \
        .withColumn("productivity_score",
                   (F.coalesce(F.col("loans_approved"), F.lit(0)) * 0.5 + 
                    F.coalesce(F.col("total_loan_volume"), F.lit(0)) * 0.3 + 
                    F.col("tenure_months") * 0.2)) \
        .withColumn("position_percentile",
                   F.percent_rank().over(Window.partitionBy("position").orderBy("productivity_score"))) \
        .withColumn("performance_category",
                   F.when(F.col("position_percentile") > 0.8, "TOP_PERFORMER")
                    .when(F.col("position_percentile") > 0.6, "HIGH_PERFORMER")
                    .when(F.col("position_percentile") > 0.4, "AVERAGE")
                    .when(F.col("position_percentile") > 0.2, "LOW_PERFORMER")
                    .otherwise("UNDER_PERFORMER")) \
        .withColumn("salary_to_productivity_ratio",
                   F.col("salary") / F.col("productivity_score"))
    
    return employee_metrics