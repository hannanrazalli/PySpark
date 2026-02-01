# TIER 1 To Read CSV & JSON:
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Step 1: Settings & Paths
raw_path = "/Volumes/workspace/default/hannan_files/raw_data"
clean_path = "/Volumes/workspace/default/hannan_files/clean_data"
quarantine_path = "/Volumes/workspace/default/hannan_files/quarantine_data"

# Step 2: Read raw data with schema (Verify)
df_raw = (spark.read.format("json")
                    .schema(schema)
                    .load(raw_path)
)

# Step 3: Process raw data
df_processed = (df_raw.withColumn("invalid", when(
                                (col("area_km2")==0) | (col("area_km2").isNull()), True).otherwise(False))
                    .withColumn("pop_density", round(expr("try_divide(population, area_km2)"),1))
                    .withColumn("processed_at", current_timestamp())
)

# Step 4: Split (Clean & Quarantine)
df_clean = df_processed.filter(col("invalid")==False).drop("invalid")
df_quarantine = df_processed.filter(col("invalid")==True).drop("invalid")

# Step 5: Write to paths
(df_clean.write.format("json")
                .mode("overwrite")
                .option("compression", "gzip")
                .save(clean_path)
)

(df_quarantine.write.format("json")
                .mode("overwrite")
                .option("compression", "gzip")
                .save(quarantine_path)
)


# partitionBy("column_name"):
# Contoh guna format Delta (Highest Tier)
(df_clean.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("continent")  *Kita asingkan folder ikut Benua*
    .saveAsTable("country_stats_partitioned"))

# DELTA HISTORY:
display(spark.sql("DESCRIBE HISTORY country_clean"))

*To read previous version*
ori_df = (spark.read.option("versionAsOf", 0)
                    .table("country_clean")
)
display(ori_df)

# PENDING LESSONS TIER 1:
1) Rename column
2) DF Transformation - maths, upper, lower. (withColumn not efficient, directly use select)(sometimes)


*---------------------------------*
*BRONZE TO SILVER (JSON TO DELTA):*
*---------------------------------*
# Step 1: Settings & Paths
table_clean_name = "delta_clean_1"
table_quarantine_name = "delta_quarantine_1"

# Step 2: Read files
df_raw = (
    spark.read.format("json")
                .schema(schema)
                .load(json_path)
)

# Step 3: Process
df_processed = (df_raw
    .withColumn("population", F.expr(r"try_cast(regexp_replace(population, '[,\\s]', '') AS bigint)"))
    .withColumn("area_km2", F.expr("try_cast(area_km2 as double)"))
    .withColumn("pop_density",F.expr("try_divide(population, area_km2)"))
    .withColumn("is_invalid",
                F.when(
                    (F.col("area_km2")<=0) |
                    (F.col("area_km2").isNull()) |
                    (F.col("population").isNull())
                    , True).otherwise(False))
    .withColumn("_load_timestamp", F.current_timestamp())
)

# Step 4: Split
df_clean = (
    df_processed.filter(F.col("is_invalid")==False)
                .drop("is_invalid")
)

df_quarantine = (
    df_processed.filter(F.col("is_invalid")==True)
)
""""
# Step 5: Write
(df_clean.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", True)
                .option("description", "Silver table with cleaned country data ready for Gold Layer")
                .saveAsTable(table_clean_name)
)

(df_quarantine.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", True)
                .option("description", "Silver table with quarantined country data(area_km2 = 0)")
                .saveAsTable(table_quarantine_name)
)
"""
# Step 5: Write
def write_delta(df, table_name, description):
    print(f"Writing data into {table_name}...")

    (
        df.write.format("delta")
                .mode("overwrite")
                .option("overwritSchema", "true")
                .option("description", description)
                .saveAsTable(table_name)
    )

    print(f"Successfully ingested {table_name}!")

write_delta(
    df = df_clean,
    table_name = table_clean_name,
    description = "Silver table with cleaned country data ready for Gold Layer"
)

write_delta(
    df = df_quarantine,
    table_name = table_quarantine_name,
    description = "Silver table with quarantined country data(area_km2 = 0)"
)


*---------------------*
*PYSPARK TRIM FUNCTION:*
*---------------------*
1) trim():
Buang space depan/belakang. Cuci input user yang suka tekan spacebar.
2) lower() / upper():
Tukar saiz huruf. Standardkan ID atau Nama untuk buat Join.
3) concat():
Gabung dua kolum jadi satu. Gabung first_name + last_name.
4) regexp_replace():
Ganti perkataan guna pattern. Buang simbol matawang atau karakter khas.
5) split():
Pecahkan string jadi Array.	Pecah kolum tags yang dipisahkan oleh koma.


*CHAPTER 23: MATHS FUNCTIONS*