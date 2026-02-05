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
# Step 1: Paths
json_path = "/Volumes/workspace/default/delta_practice_files/json_195"
bronze_table = "bronze_countries"
silver_table_clean = "silver_countries_clean"
silver_table_quarantine = "silver_countries_quarantine"

# Step 2: Read, timstamp, call table, merge
df_incoming = spark.read.format("json").schema(schema).load(json_path)
df_incoming = df_incoming.withColumn("_time_stamp", F.current_timestamp())

if not spark.catalog.tableExists(bronze_table):
    (df_incoming.write.format("delta").saveAsTable(bronze_table))

else:
    dt_bronze = DeltaTable.forName(spark, bronze_table)

    (dt_bronze.alias("t")
              .merge(df_incoming.alias("s"), "s.country_name = t.country_name")
              .whenMatchedUpdateAll()
              .whenNotMatchedInsertAll()
              .execute()
    )

# Step 3: Source, process
df_bronze = spark.read.table(bronze_table)
df_processed = (df_bronze.withColumn("population", F.expr("try_cast(regexp_replace(population, '[^0-9]', '') AS bigint)"))
                       .withColumn("area_km2", F.expr("try_cast(regexp_replace(area_km2, '[^0-9.]', '') AS double)"))
                       .withColumn("pop_density", F.expr("try_divide(population, area_km2)"))
                       .withColumn("is_invalid", F.when(
                           (F.col("area_km2") <= 0) |
                           (F.col("area_km2").isNull()) |
                           (F.col("population").isNull()), True
                       ).otherwise(False))
)

# Step 4: Split
df_clean = df_processed.filter(F.col("is_invalid") == False).drop(F.col("is_invalid"))
df_quarantine = df_processed.filter(F.col("is_invalid") == True)

# Step 5: Write
def delta_write(df, table_name, description):
    (df.write.format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .option("description", description)
             .saveAsTable(table_name))
    print(f"Successfully update {table_name}")

delta_write(df_clean, silver_table_clean, "Clean country silver table")
delta_write(df_quarantine, silver_table_quarantine, "Quarantine country silver table")


*---------------------------------*
*BRONZE TO SILVER (ARCHITECT TIER)*
*---------------------------------*
from pyspark.sql import functions as F

# ==========================================
# STEP 1: TOOLBOX (Functions / SOP)
# ==========================================

def clean_geographic_logic(df):
    """
    SOP untuk cuci data Geografi.
    Menggunakan logic try_cast dan regex dari Step 3 asal kau.
    """
    return (df.withColumn("population", F.expr("try_cast(regexp_replace(population, '[^0-9]', '') AS bigint)"))
              .withColumn("area_km2", F.expr("try_cast(regexp_replace(area_km2, '[^0-9.]', '') AS double)"))
              .withColumn("pop_density", F.expr("try_divide(population, area_km2)")))

def validate_quality_logic(df):
    """
    SOP untuk tentukan kualiti data (Is Invalid?).
    Sama macam Step 4 asal kau.
    """
    return df.withColumn("is_invalid", 
        (F.col("area_km2") <= 0) | 
        F.col("area_km2").isNull() | 
        F.col("population").isNull())

def add_audit_metadata(df):
    """SOP untuk tambah rekod masa proses (Audit Trail)."""
    return df.withColumn("_processed_at", F.current_timestamp())

# ==========================================
# STEP 2: EXECUTION PIPELINE (The Flow)
# ==========================================

# 1. Load data dari Bronze
df_bronze = spark.read.table("bronze_countries")

# 2. Main Transformation (Method Chaining)
# Inilah gaya Highest Tier 1. Data mengalir melalui SOP yang kita dah buat.
df_silver_processed = (df_bronze
    .transform(clean_geographic_logic)
    .transform(validate_quality_logic)
    .transform(add_audit_metadata)
)

# 3. Splitting Clean vs Quarantine
df_clean = df_silver_processed.filter("is_invalid = False").drop("is_invalid")
df_quarantine = df_silver_processed.filter("is_invalid = True")

# ==========================================
# STEP 3: WRITING (Persistence)
# ==========================================

def write_to_silver(df, table_name, description):
    """Helper function untuk tulis data dengan Metadata."""
    (df.write.format("delta")
             .mode("overwrite") # Kau boleh tukar ke 'append' atau guna 'merge' nanti
             .option("overwriteSchema", "true")
             .option("description", description)
             .saveAsTable(table_name))
    print(f"Successfully updated {table_name}")

# Panggil fungsi tulis
write_to_silver(df_clean, "silver_countries_clean", "Table untuk data yang dah cuci.")
write_to_silver(df_quarantine, "silver_countries_quarantine", "Table untuk data yang bermasalah.")


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


CHAPTER 22: MATHS FUNCTIONS
TRIM, LENGTH, CHAPTER Z, CONCAT_WS, LIT, INITCAP, UPPER

*CHAPTER 23: MATHS FUNCTIONS*

*CHAPTER 24: DateTime functions*

CHAPTER 26: 

CHAPTER 27: Grouping & Aggregations
- Multiple aggregations

CHAPTER 28: Sorting & limiting

CHAPTER 29: 


*--------------*
*SILVER TO GOLD*
*--------------*
# Step 1: The Definition (Business Logic)
from pyspark.sql import functions as F

def transform_gold_metrics(df):
    """
    BUSINESS LOGIC LAYER
    Audit Note: Aggregating population by region for Executive Dashboard.
    """
    return df.groupBy("region").agg(
        F.count("country_name").alias("total_countries"),
        F.sum("population").alias("total_population"),
        F.avg("pop_density").alias("avg_density"),
        # Highest Tier 1: Sentiasa simpan audit timestamp di Gold
        F.max("processed_at").alias("data_freshness_check")
    )

# Step 2: The Audit Enrichment (Metadata)
def add_business_tier(df):
    """
    ENRICHMENT LAYER
    Audit Note: Categorizing regions based on population scale.
    """
    return df.withColumn("market_tier", 
        F.expr("""
            CASE 
                WHEN total_population > 1000000000 THEN 'Tier 1 Market'
                WHEN total_population > 500000000 THEN 'Tier 2 Market'
                ELSE 'Emerging Market'
            END
        """))

# Step 3: Execution (The Main Pipe)
1. Load data Silver (Source of Truth)
df_silver = spark.read.table("silver_countries_clean")

2. Run Aggregation
df_aggregated = transform_gold_metrics(df_silver)

3. Run Enrichment (SQL Expressions style)
df_final_gold = add_business_tier(df_aggregated)

4. Sorting for Presentation
df_final_gold = df_final_gold.orderBy(F.col("total_population").desc())