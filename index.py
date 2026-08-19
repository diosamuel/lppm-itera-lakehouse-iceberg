from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .remote("sc://localhost:15002")
    .appName("book_app")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS default.books")

spark.sql("""CREATE TABLE IF NOT EXISTS default.books.list_books (
    book_id INT,
    title STRING,
    price DECIMAL(10,2),
    stock INT,
    created_at TIMESTAMP
)
USING ICEBERG
PARTITIONED BY (months(created_at))
""")

spark.sql("SELECT count(*) FROM default.books.list_books").show()
