from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("User Purchases Analysis").getOrCreate()

users_df = spark.read.csv("users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv("purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)

users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()


users_df.createOrReplaceTempView("users")
purchases_df.createOrReplaceTempView("purchases")
products_df.createOrReplaceTempView("products")


total_sales_by_category_sql = spark.sql("""
    SELECT 
        p.category,
        SUM(p.price * pr.quantity) AS total_sales
    FROM products p
    JOIN purchases pr ON p.product_id = pr.product_id
    GROUP BY p.category
""")

total_sales_by_category_sql.show()

sales_by_category_age_group_sql = spark.sql("""
    SELECT 
        p.category,
        SUM(p.price * pr.quantity) AS total_sales
    FROM products p
    JOIN purchases pr ON p.product_id = pr.product_id
    JOIN users u ON pr.user_id = u.user_id
    WHERE u.age >= 18 AND u.age <= 25
    GROUP BY p.category
""")

sales_by_category_age_group_sql.show()

category_share_sql = spark.sql("""
    WITH category_sales AS (
        SELECT 
            p.category,
            SUM(p.price * pr.quantity) AS total_sales
        FROM products p
        JOIN purchases pr ON p.product_id = pr.product_id
        JOIN users u ON pr.user_id = u.user_id
        WHERE u.age >= 18 AND u.age <= 25
        GROUP BY p.category
    ),
    total_sales_sum AS (
        SELECT SUM(total_sales) AS grand_total
        FROM category_sales
    )
    SELECT 
        cs.category,
        cs.total_sales,
        (cs.total_sales / ts.grand_total) * 100 AS percentage_share
    FROM category_sales cs
    CROSS JOIN total_sales_sum ts
""")

category_share_sql.show()

top_3_categories_sql = spark.sql("""
    WITH category_sales AS (
        SELECT 
            p.category,
            SUM(p.price * pr.quantity) AS total_sales
        FROM products p
        JOIN purchases pr ON p.product_id = pr.product_id
        JOIN users u ON pr.user_id = u.user_id
        WHERE u.age >= 18 AND u.age <= 25
        GROUP BY p.category
    ),
    total_sales_sum AS (
        SELECT SUM(total_sales) AS grand_total
        FROM category_sales
    )
    SELECT 
        cs.category,
        cs.total_sales,
        (cs.total_sales / ts.grand_total) * 100 AS percentage_share
    FROM category_sales cs
    CROSS JOIN total_sales_sum ts
    ORDER BY percentage_share DESC
    LIMIT 3
""")

top_3_categories_sql.show()

