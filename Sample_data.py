# 1. CUSTOMERS Data
customers_data = [
    (1, "John", "Smith", "john.smith@email.com", "555-0101", date(1985, 3, 15), "123 Main St", "New York", "NY", "10001", date(2018, 5, 20), "INDIVIDUAL"),
    (2, "Sarah", "Johnson", "sarah.j@email.com", "555-0102", date(1990, 7, 22), "456 Oak Ave", "Los Angeles", "CA", "90210", date(2019, 2, 15), "INDIVIDUAL"),
    (3, "Michael", "Brown", "m.brown@email.com", "555-0103", date(1978, 11, 5), "789 Pine St", "Chicago", "IL", "60601", date(2017, 8, 10), "INDIVIDUAL"),
    (4, "TechCorp Inc", None, "info@techcorp.com", "555-0104", None, "101 Business Blvd", "San Francisco", "CA", "94105", date(2020, 1, 15), "BUSINESS"),
    (5, "Emily", "Davis", "emily.davis@email.com", "555-0105", date(1992, 4, 30), "222 Elm St", "Houston", "TX", "77001", date(2021, 3, 22), "INDIVIDUAL"),
    (6, "Robert", "Wilson", "r.wilson@email.com", "555-0106", date(1982, 9, 18), "333 Maple Dr", "Phoenix", "AZ", "85001", date(2019, 11, 5), "INDIVIDUAL"),
    (7, "Global Retail Ltd", None, "contact@globalretail.com", "555-0107", None, "444 Commerce St", "Miami", "FL", "33101", date(2022, 6, 10), "BUSINESS"),
    (8, "Lisa", "Anderson", "lisa.a@email.com", "555-0108", date(1988, 12, 12), "555 Cedar Ln", "Seattle", "WA", "98101", date(2020, 9, 18), "INDIVIDUAL"),
    (9, "David", "Martinez", "d.martinez@email.com", "555-0109", date(1975, 6, 25), "666 Birch Rd", "Boston", "MA", "02101", date(2018, 12, 1), "INDIVIDUAL"),
    (10, "Innovate Solutions", None, "admin@innovates.com", "555-0110", None, "777 Tech Park", "Austin", "TX", "73301", date(2023, 1, 8), "BUSINESS")
]

customers_df = spark.createDataFrame(customers_data, customers_schema)

# 2. BRANCHES Data
branches_data = [
    (1, "New York Downtown", "NY001", "123 Financial St", "New York", "NY", "555-1001", 1),
    (2, "Los Angeles West", "LA002", "456 Sunset Blvd", "Los Angeles", "CA", "555-1002", 2),
    (3, "Chicago Central", "CH003", "789 Michigan Ave", "Chicago", "IL", "555-1003", 3),
    (4, "San Francisco Bay", "SF004", "101 Market St", "San Francisco", "CA", "555-1004", 4),
    (5, "Houston Main", "HO005", "222 Texas Ave", "Houston", "TX", "555-1005", 5),
    (6, "Phoenix Desert", "PH006", "333 Camelback Rd", "Phoenix", "AZ", "555-1006", 6),
    (7, "Miami Beach", "MI007", "444 Ocean Dr", "Miami", "FL", "555-1007", 7),
    (8, "Seattle Pacific", "SE008", "555 Pioneer Sq", "Seattle", "WA", "555-1008", 8),
    (9, "Boston Historic", "BO009", "666 Freedom Tr", "Boston", "MA", "555-1009", 9),
    (10, "Austin Tech", "AU010", "777 Innovation Way", "Austin", "TX", "555-1010", 10)
]

branches_df = spark.createDataFrame(branches_data, branches_schema)

# 3. EMPLOYEES Data
employees_data = [
    (1, 1, "James", "Williams", "Branch Manager", "j.williams@bank.com", "555-2001", date(2015, 3, 15), 85000.00, None),
    (2, 2, "Jennifer", "Taylor", "Branch Manager", "j.taylor@bank.com", "555-2002", date(2016, 6, 20), 82000.00, None),
    (3, 3, "Christopher", "Lee", "Branch Manager", "c.lee@bank.com", "555-2003", date(2014, 1, 10), 87000.00, None),
    (4, 4, "Amanda", "Garcia", "Branch Manager", "a.garcia@bank.com", "555-2004", date(2017, 8, 5), 83000.00, None),
    (5, 5, "Daniel", "Rodriguez", "Loan Officer", "d.rodriguez@bank.com", "555-2005", date(2019, 2, 15), 65000.00, 1),
    (6, 6, "Michelle", "Hernandez", "Loan Officer", "m.hernandez@bank.com", "555-2006", date(2018, 4, 22), 62000.00, 2),
    (7, 7, "Kevin", "Lopez", "Teller", "k.lopez@bank.com", "555-2007", date(2020, 7, 10), 45000.00, 3),
    (8, 8, "Stephanie", "Gonzalez", "Teller", "s.gonzalez@bank.com", "555-2008", date(2021, 1, 18), 42000.00, 4),
    (9, 9, "Matthew", "Wilson", "Financial Advisor", "m.wilson@bank.com", "555-2009", date(2017, 11, 30), 72000.00, 1),
    (10, 10, "Jessica", "Adams", "Customer Service", "j.adams@bank.com", "555-2010", date(2022, 3, 5), 38000.00, 2)
]

employees_df = spark.createDataFrame(employees_data, employees_schema)

# 4. ACCOUNTS Data
accounts_data = [
    (1, 1, "ACC10001", "CHECKING", 12500.50, date(2020, 1, 15), None, "ACTIVE", 1),
    (2, 1, "ACC10002", "SAVINGS", 45500.75, date(2020, 1, 15), None, "ACTIVE", 1),
    (3, 2, "ACC10003", "CHECKING", 7800.25, date(2019, 5, 20), None, "ACTIVE", 2),
    (4, 3, "ACC10004", "SAVINGS", 32000.00, date(2018, 8, 10), None, "ACTIVE", 3),
    (5, 4, "ACC10005", "BUSINESS", 150000.00, date(2021, 2, 1), None, "ACTIVE", 4),
    (6, 5, "ACC10006", "CHECKING", 5500.80, date(2022, 4, 15), None, "ACTIVE", 5),
    (7, 6, "ACC10007", "SAVINGS", 18750.40, date(2020, 11, 5), None, "ACTIVE", 6),
    (8, 7, "ACC10008", "BUSINESS", 275000.00, date(2023, 1, 10), None, "ACTIVE", 7),
    (9, 8, "ACC10009", "CHECKING", 9200.60, date(2021, 9, 18), None, "ACTIVE", 8),
    (10, 9, "ACC10010", "SAVINGS", 42500.25, date(2019, 12, 1), None, "ACTIVE", 9),
    (11, 10, "ACC10011", "BUSINESS", 89000.00, date(2023, 3, 8), None, "ACTIVE", 10),
    (12, 2, "ACC10012", "LOAN", -25000.00, date(2022, 6, 15), None, "ACTIVE", 2),
    (13, 3, "ACC10013", "LOAN", -15000.00, date(2021, 9, 10), None, "ACTIVE", 3)
]

accounts_df = spark.createDataFrame(accounts_data, accounts_schema)

# 5. TRANSACTIONS Data
transactions_data = [
    (1, 1, "DEPOSIT", 5000.00, datetime(2024, 1, 15, 9, 30, 0), "Initial deposit", None, "COMPLETED"),
    (2, 1, "WITHDRAWAL", 500.00, datetime(2024, 1, 16, 14, 15, 0), "ATM withdrawal", None, "COMPLETED"),
    (3, 2, "DEPOSIT", 10000.00, datetime(2024, 1, 17, 10, 0, 0), "Salary deposit", None, "COMPLETED"),
    (4, 3, "TRANSFER", 1500.00, datetime(2024, 1, 18, 11, 45, 0), "Transfer to savings", 2, "COMPLETED"),
    (5, 4, "WITHDRAWAL", 2000.00, datetime(2024, 1, 19, 16, 20, 0), "Cash withdrawal", None, "COMPLETED"),
    (6, 5, "DEPOSIT", 50000.00, datetime(2024, 1, 20, 13, 10, 0), "Business revenue", None, "COMPLETED"),
    (7, 6, "PAYMENT", 350.75, datetime(2024, 1, 21, 8, 45, 0), "Utility bill payment", None, "COMPLETED"),
    (8, 7, "TRANSFER", 5000.00, datetime(2024, 1, 22, 15, 30, 0), "Vendor payment", 5, "COMPLETED"),
    (9, 8, "DEPOSIT", 75000.00, datetime(2024, 1, 23, 12, 0, 0), "Investment return", None, "COMPLETED"),
    (10, 9, "WITHDRAWAL", 1200.00, datetime(2024, 1, 24, 17, 15, 0), "Shopping", None, "COMPLETED")
]

transactions_df = spark.createDataFrame(transactions_data, transactions_schema)

# 6. LOANS Data
loans_data = [
    (1, 2, 12, "PERSONAL", 25000.00, 7.500, 36, date(2022, 6, 15), date(2025, 6, 15), "ACTIVE", 5),
    (2, 3, 13, "AUTO", 15000.00, 5.250, 48, date(2021, 9, 10), date(2025, 9, 10), "ACTIVE", 6),
    (3, 1, 14, "MORTGAGE", 300000.00, 4.125, 360, date(2020, 3, 20), date(2050, 3, 20), "ACTIVE", 5),
    (4, 5, 15, "PERSONAL", 10000.00, 8.750, 24, date(2023, 1, 15), date(2025, 1, 15), "ACTIVE", 6),
    (5, 4, 16, "BUSINESS", 50000.00, 6.500, 60, date(2022, 8, 1), date(2027, 8, 1), "ACTIVE", 5),
    (6, 6, 17, "AUTO", 20000.00, 5.750, 60, date(2023, 3, 10), date(2028, 3, 10), "ACTIVE", 6),
    (7, 8, 18, "PERSONAL", 8000.00, 9.250, 18, date(2024, 1, 5), date(2025, 7, 5), "ACTIVE", 5),
    (8, 7, 19, "BUSINESS", 100000.00, 5.900, 84, date(2023, 6, 20), date(2030, 6, 20), "ACTIVE", 6),
    (9, 9, 20, "MORTGAGE", 450000.00, 4.250, 360, date(2021, 11, 15), date(2051, 11, 15), "ACTIVE", 5),
    (10, 10, 21, "BUSINESS", 75000.00, 7.100, 48, date(2023, 9, 1), date(2027, 9, 1), "ACTIVE", 6)
]

loans_df = spark.createDataFrame(loans_data, loans_schema)

# 7. LOAN_PAYMENTS Data
loan_payments_data = [
    (1, 1, date(2024, 1, 15), date(2024, 1, 14), 785.42, 785.42, 625.00, 160.42, "PAID"),
    (2, 1, date(2024, 2, 15), date(2024, 2, 16), 785.42, 785.42, 628.91, 156.51, "PAID"),
    (3, 1, date(2024, 3, 15), None, 785.42, 0.00, 632.84, 152.58, "PENDING"),
    (4, 2, date(2024, 1, 10), date(2024, 1, 9), 347.85, 347.85, 281.25, 66.60, "PAID"),
    (5, 2, date(2024, 2, 10), date(2024, 2, 8), 347.85, 347.85, 282.70, 65.15, "PAID"),
    (6, 2, date(2024, 3, 10), None, 347.85, 0.00, 284.16, 63.69, "PENDING"),
    (7, 3, date(2024, 1, 20), date(2024, 1, 19), 1452.63, 1452.63, 1041.67, 410.96, "PAID"),
    (8, 3, date(2024, 2, 20), date(2024, 2, 21), 1452.63, 1452.63, 1045.24, 407.39, "PAID"),
    (9, 3, date(2024, 3, 20), None, 1452.63, 0.00, 1048.83, 403.80, "PENDING"),
    (10, 4, date(2024, 1, 15), date(2024, 1, 16), 453.68, 453.68, 364.58, 89.10, "PAID")
]

loan_payments_df = spark.createDataFrame(loan_payments_data, loan_payments_schema)

# 8. CREDIT_CARDS Data
credit_cards_data = [
    (1, 1, 22, "4532-7890-1234-5678", "VISA", 10000.00, 2450.75, date(2022, 1, 15), date(2026, 1, 31), "ACTIVE"),
    (2, 2, 23, "5500-1234-5678-9012", "MASTERCARD", 15000.00, 7800.25, date(2021, 8, 20), date(2025, 8, 31), "ACTIVE"),
    (3, 3, 24, "3712-456789-12345", "AMEX", 20000.00, 12500.50, date(2020, 3, 10), date(2024, 3, 31), "ACTIVE"),
    (4, 4, 25, "4532-9012-3456-7890", "VISA", 50000.00, 32500.00, date(2023, 2, 1), date(2027, 2, 28), "ACTIVE"),
    (5, 5, 26, "5421-2345-6789-0123", "MASTERCARD", 8000.00, 3200.80, date(2022, 5, 15), date(2026, 5, 31), "ACTIVE"),
    (6, 6, 27, "4024-0071-2345-6789", "VISA", 12000.00, 6500.40, date(2021, 11, 5), date(2025, 11, 30), "ACTIVE"),
    (7, 7, 28, "5500-9876-5432-1098", "MASTERCARD", 75000.00, 45000.75, date(2023, 6, 10), date(2027, 6, 30), "ACTIVE"),
    (8, 8, 29, "3456-789012-34567", "AMEX", 15000.00, 9200.60, date(2022, 9, 18), date(2026, 9, 30), "ACTIVE"),
    (9, 9, 30, "4916-2801-2345-6789", "VISA", 25000.00, 12500.25, date(2020, 12, 1), date(2024, 12, 31), "ACTIVE"),
    (10, 10, 31, "5421-8765-4321-0987", "MASTERCARD", 30000.00, 18900.00, date(2023, 3, 8), date(2027, 3, 31), "ACTIVE")
]

credit_cards_df = spark.createDataFrame(credit_cards_data, credit_cards_schema)

# 9. CARD_TRANSACTIONS Data
card_transactions_data = [
    (1, 1, datetime(2024, 1, 15, 14, 30, 0), "Amazon", 125.50, "PURCHASE", "RETAIL", "Seattle", "USA"),
    (2, 1, datetime(2024, 1, 16, 19, 45, 0), "Starbucks", 8.75, "PURCHASE", "DINING", "New York", "USA"),
    (3, 2, datetime(2024, 1, 17, 11, 20, 0), "Apple Store", 999.00, "PURCHASE", "RETAIL", "Los Angeles", "USA"),
    (4, 2, datetime(2024, 1, 18, 20, 15, 0), "United Airlines", 450.25, "PURCHASE", "TRAVEL", "Chicago", "USA"),
    (5, 3, datetime(2024, 1, 19, 9, 30, 0), "Whole Foods", 185.40, "PURCHASE", "RETAIL", "San Francisco", "USA"),
    (6, 3, datetime(2024, 1, 20, 12, 45, 0), "Marriott Hotel", 320.00, "PURCHASE", "TRAVEL", "Miami", "USA"),
    (7, 4, datetime(2024, 1, 21, 16, 20, 0), "Office Depot", 1250.75, "PURCHASE", "UTILITIES", "Houston", "USA"),
    (8, 4, datetime(2024, 1, 22, 8, 15, 0), "Shell Gas Station", 65.80, "PURCHASE", "UTILITIES", "Phoenix", "USA"),
    (9, 5, datetime(2024, 1, 23, 18, 30, 0), "Best Buy", 899.99, "PURCHASE", "RETAIL", "Boston", "USA"),
    (10, 5, datetime(2024, 1, 24, 13, 10, 0), "AT&T", 95.50, "PURCHASE", "UTILITIES", "Austin", "USA")
]

card_transactions_df = spark.createDataFrame(card_transactions_data, card_transactions_schema)

# 10. ACCOUNT_BENEFICIARIES Data
account_beneficiaries_data = [
    (1, 1, 2, "SPOUSE", 50.00, date(2020, 1, 20)),
    (2, 1, 5, "CHILD", 50.00, date(2020, 1, 20)),
    (3, 2, 2, "SPOUSE", 100.00, date(2020, 1, 20)),
    (4, 3, 1, "PARENT", 100.00, date(2019, 5, 25)),
    (5, 4, 3, "CHILD", 100.00, date(2018, 8, 15)),
    (6, 5, 7, "BUSINESS_PARTNER", 50.00, date(2021, 2, 5)),
    (7, 5, 8, "BUSINESS_PARTNER", 50.00, date(2021, 2, 5)),
    (8, 6, 5, "SPOUSE", 100.00, date(2022, 4, 20)),
    (9, 7, 6, "CHILD", 50.00, date(2020, 11, 10)),
    (10, 7, 9, "CHILD", 50.00, date(2020, 11, 10))
]

account_beneficiaries_df = spark.createDataFrame(account_beneficiaries_data, account_beneficiaries_schema)