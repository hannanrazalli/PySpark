# List of tuples:
data = [("Data Engineer","RM 6500"),("Design Engineer","RM 4400")]
df = spark.createDataFrame(data, ["Jobs","Salary"])
display(df)

1) Create dataframe:
spark.createDataFrame

data = [('John', 21), ('Amy', 25), ('Anita', 41), ('Rohan', 25), ('Maria', 37)]
df = spark.createDataFrame(data) *Create dataframe*
df.show() *Print dataframe*

df = spark.createDataFrame(data, 'name string, age int') *Set column data type*
df = spark.createDataFrame(data, ["Name","Age"]) *Set column name*
df.show() *Print dataframe*


# 1. Dahulukan dengan import
from pyspark.sql.types import *

# 2. Siapkan "Almari" (Schema)
schema_jualan = StructType([
    StructField("Tarikh", DateType(), True),
    StructField("Produk", StringType(), True),
    StructField("Harga", DoubleType(), True)
])

# 3. "Sedut" file guna Schema tadi
df = spark.read.format("csv") \ *Penggunaan "\" utk bgtahu sambung ke next line*
    .option("header", "true") \ *Kalau file ada header, wajib ada .option("header", "true")*
    .schema(schema_jualan) \
    .load("/path/ke/file/jualan.csv")

df.show()


# df.show(n=200, truncate=3)
limit = 200
setiap value limit first 3 letters

# WRITE DF TO CSV:

data = .....

df = spark.createDataFrame(data, schema=schema)

volume_path = "/Volumes/workspace/default/hannan_files/test_csv_195"

df.write.format("csv") \ *1*
  .mode("overwrite") \ *2*
  .option("header", "true") \ *3*
  .save(volume_path) *4*