from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

HOME = os.path.expanduser("~")
DATA_DIR = f"{HOME}/capstone/data/tpch-sf1"   
WAREHOUSE = f"file://{HOME}/capstone/warehouse"

spark = SparkSession.builder \
    .appName("tpch-load") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE) \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.tpch")

def load_table(name, schema):
    path = f"{DATA_DIR}/{name}.tbl"
    df = spark.read.option("delimiter", "|").schema(schema).csv(path)
    # drop the trailing empty column caused by TPC-H's trailing "|"
    df = df.select(df.columns[:len(schema.fields)])
    spark.sql(f"DROP TABLE IF EXISTS local.tpch.{name}")
    df.writeTo(f"local.tpch.{name}").using("iceberg").create()
    count = spark.sql(f"SELECT COUNT(*) AS c FROM local.tpch.{name}").collect()[0]["c"]
    print(f"{name}: loaded {count} rows")

schemas = {
    "region": StructType([
        StructField("r_regionkey", IntegerType()),
        StructField("r_name", StringType()),
        StructField("r_comment", StringType()),
    ]),
    "nation": StructType([
        StructField("n_nationkey", IntegerType()),
        StructField("n_name", StringType()),
        StructField("n_regionkey", IntegerType()),
        StructField("n_comment", StringType()),
    ]),
    "supplier": StructType([
        StructField("s_suppkey", LongType()),
        StructField("s_name", StringType()),
        StructField("s_address", StringType()),
        StructField("s_nationkey", IntegerType()),
        StructField("s_phone", StringType()),
        StructField("s_acctbal", DecimalType(15,2)),
        StructField("s_comment", StringType()),
    ]),
    "customer": StructType([
        StructField("c_custkey", LongType()),
        StructField("c_name", StringType()),
        StructField("c_address", StringType()),
        StructField("c_nationkey", IntegerType()),
        StructField("c_phone", StringType()),
        StructField("c_acctbal", DecimalType(15,2)),
        StructField("c_mktsegment", StringType()),
        StructField("c_comment", StringType()),
    ]),
    "part": StructType([
        StructField("p_partkey", LongType()),
        StructField("p_name", StringType()),
        StructField("p_mfgr", StringType()),
        StructField("p_brand", StringType()),
        StructField("p_type", StringType()),
        StructField("p_size", IntegerType()),
        StructField("p_container", StringType()),
        StructField("p_retailprice", DecimalType(15,2)),
        StructField("p_comment", StringType()),
    ]),
    "partsupp": StructType([
        StructField("ps_partkey", LongType()),
        StructField("ps_suppkey", LongType()),
        StructField("ps_availqty", IntegerType()),
        StructField("ps_supplycost", DecimalType(15,2)),
        StructField("ps_comment", StringType()),
    ]),
    "orders": StructType([
        StructField("o_orderkey", LongType()),
        StructField("o_custkey", LongType()),
        StructField("o_orderstatus", StringType()),
        StructField("o_totalprice", DecimalType(15,2)),
        StructField("o_orderdate", DateType()),
        StructField("o_orderpriority", StringType()),
        StructField("o_clerk", StringType()),
        StructField("o_shippriority", IntegerType()),
        StructField("o_comment", StringType()),
    ]),
    "lineitem": StructType([
        StructField("l_orderkey", LongType()),
        StructField("l_partkey", LongType()),
        StructField("l_suppkey", LongType()),
        StructField("l_linenumber", IntegerType()),
        StructField("l_quantity", DecimalType(15,2)),
        StructField("l_extendedprice", DecimalType(15,2)),
        StructField("l_discount", DecimalType(15,2)),
        StructField("l_tax", DecimalType(15,2)),
        StructField("l_returnflag", StringType()),
        StructField("l_linestatus", StringType()),
        StructField("l_shipdate", DateType()),
        StructField("l_commitdate", DateType()),
        StructField("l_receiptdate", DateType()),
        StructField("l_shipinstruct", StringType()),
        StructField("l_shipmode", StringType()),
        StructField("l_comment", StringType()),
    ]),
}

# Load in FK-safe order (doesn't strictly matter for Iceberg, but keeps output readable)
for name in ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]:
    load_table(name, schemas[name])

print("All tables loaded.")
spark.stop()
