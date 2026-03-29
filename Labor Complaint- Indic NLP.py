# Databricks notebook source
# DBTITLE 1,Labor Complaint Pipeline Overview
# MAGIC %md
# MAGIC # Labor Complaint Pipeline — Hackathon Demo
# MAGIC **Clone Vibhor (6 calls) → indic_nlp.py → Labor Complaint → Clone Legal RAG**
# MAGIC
# MAGIC ## 🚀 Hackathon Demo Mode
# MAGIC
# MAGIC This pipeline processes **6 diverse complaints** for fast hackathon presentation (~2 minutes total vs 30+ minutes with 400 calls).
# MAGIC
# MAGIC ### Demo Scenarios:
# MAGIC 1. **Rajesh @ 400053**: 3 complaints (WAGE_THEFT + ASSAULT + UNSAFE) = **HIGH threat** ⚠️
# MAGIC 2. **Suresh @ 110001**: 1 complaint, 15 workers = **HIGH threat** ⚠️
# MAGIC 3. **Amit @ 560001**: 1 complaint = LOW threat
# MAGIC 4. **Prakash @ 400053**: 1 complaint = LOW threat
# MAGIC
# MAGIC ### Pipeline Steps:
# MAGIC 1. **Reads from Vibhor's bronze_calls**: `nyaya_hackathon.shramik_grid.bronze_calls` (6 calls)
# MAGIC 2. **Processes via indic_nlp.py**: Member 2's Sarvam API-based translation and entity extraction
# MAGIC 3. **Creates Silver layer**: `nyaya_hackathon.default.silver_complaints` (6 structured complaints)
# MAGIC 4. **Aggregation**: Groups by pincode + contractor
# MAGIC 5. **Rule-based threat classification**: LOW/MEDIUM/HIGH
# MAGIC 6. **Creates Gold layer**: `nyaya_hackathon.default.gold_heatmap` (~4 contractor records)
# MAGIC 7. **Feeds into Legal RAG**: ~2 HIGH-priority legal actions
# MAGIC
# MAGIC ### Expected Results:
# MAGIC * Bronze: 6 calls
# MAGIC * Silver: 6 processed complaints
# MAGIC * Gold: ~4 contractor records (2 HIGH, 2 LOW)
# MAGIC * Legal: ~2 legal actions

# COMMAND ----------

# DBTITLE 1,Imports and Spark Session
# Import Member 2's Indic NLP processing function (NOT the sample data)
from indic_nlp import process_hindi_audio_text

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import uuid

spark = SparkSession.builder.appName("LaborComplaintPipeline").getOrCreate()
print("✓ Imported indic_nlp processing function (Member 2)")
print("✓ Will read 6 demo calls from Vibhor's bronze_calls table")
print("✓ Fast processing for hackathon presentation (~2 minutes)")

# COMMAND ----------

# DBTITLE 1,Raw Incoming Complaints
# Load Raw Incoming Complaints from Vibhor's Bronze Table
# Vibhor generated 6 demo calls for hackathon:
#   - 3 calls: Rajesh @ 400053 (HIGH threat hotspot)
#   - 1 call: Suresh @ 110001 (15 workers, HIGH threat)
#   - 2 calls: Amit & Prakash (LOW threat)
# Read from: nyaya_hackathon.shramik_grid.bronze_calls

df_bronze = spark.read.table("nyaya_hackathon.shramik_grid.bronze_calls")

# Convert Spark DataFrame to list of dicts for processing
# Each record has: call_id, pincode, hindi_audio_text, timestamp, call_duration_seconds
raw_complaints = [
    {
        "call_id": str(row.call_id),
        "pincode": row.pincode,
        "raw_text": row.hindi_audio_text  # Map hindi_audio_text to raw_text
    }
    for row in df_bronze.collect()
]

print(f"[DATA] Loaded {len(raw_complaints)} demo complaints from Vibhor's bronze_calls")
print(f"       Table: nyaya_hackathon.shramik_grid.bronze_calls")
print(f"\nSample complaint:")
print(f"  call_id: {raw_complaints[0]['call_id']}")
print(f"  pincode: {raw_complaints[0]['pincode']}")
print(f"  raw_text: {raw_complaints[0]['raw_text'][:50]}...")
print(f"\n✓ Ready to process through indic_nlp (~2 minutes)")

# COMMAND ----------

# DBTITLE 1,Step 1: Run Indic NLP Pipeline
# Step 1: Run Each Complaint Through Indic NLP Pipeline
print("[1/5] Running Indic NLP pipeline...")
processed_rows = []
for complaint in raw_complaints:
    nlp_result = process_hindi_audio_text(complaint["raw_text"])
    processed_rows.append({
        "call_id":                complaint["call_id"],
        "pincode":                complaint["pincode"],
        "raw_text":               complaint["raw_text"],
        "english_text":           nlp_result["english_text"],
        "contractor_name":        nlp_result["contractor_name"],
        "work_site":              nlp_result["work_site"],
        "issue_type":             nlp_result["issue_type"],
        "violence_threatened":    nlp_result["violence_threatened"],
        "worker_count_mentioned": nlp_result["worker_count_mentioned"],
    })
print(f"    ✓ Processed {len(processed_rows)} complaints")

# COMMAND ----------

# DBTITLE 1,Step 2: Build Silver Layer
# Step 2: Build Silver DataFrame
print("[2/5] Building Silver layer...")
silver_schema = StructType([
    StructField("call_id",                StringType(),  True),
    StructField("pincode",                StringType(),  True),
    StructField("raw_text",               StringType(),  True),
    StructField("english_text",           StringType(),  True),
    StructField("contractor_name",        StringType(),  True),
    StructField("work_site",              StringType(),  True),
    StructField("issue_type",             StringType(),  True),
    StructField("violence_threatened",    BooleanType(), True),
    StructField("worker_count_mentioned", IntegerType(), True),
])

silver_df = spark.createDataFrame(processed_rows, silver_schema)
silver_df.write.format("delta").mode("overwrite").saveAsTable("nyaya_hackathon.default.silver_complaints")
print("    ✓ Silver table saved → nyaya_hackathon.default.silver_complaints")

# COMMAND ----------

# DBTITLE 1,Step 3: Aggregate by Pincode and Contractor
# Step 3: Aggregate
print("[3/5] Aggregating by pincode + contractor...")
aggregated_df = silver_df.groupBy("pincode", "contractor_name") \
    .agg(
        F.count("call_id").alias("total_complaints"),
        F.collect_set("issue_type").alias("issue_types_reported"),
        F.max(F.col("violence_threatened").cast("int")).cast("boolean").alias("violence_flagged"),
        F.sum("worker_count_mentioned").alias("total_workers_affected"),
        F.collect_set("work_site").alias("work_sites")
    )
print("    ✓ Aggregation complete")

# COMMAND ----------

# DBTITLE 1,Step 4: KMeans Clustering
# Step 4: Rule-Based Threat Classification
print("[4/5] Applying rule-based threat classification...")

# Define threat level based on complaint count and worker impact
classified_df = aggregated_df.withColumn(
    "threat_label",
    F.when(
        (F.col("total_complaints") >= 3) | (F.col("total_workers_affected") >= 10),
        "HIGH"
    ).when(
        (F.col("total_complaints") >= 2) | (F.col("total_workers_affected") >= 5),
        "MEDIUM"
    ).otherwise("LOW")
)

print("    ✓ Threat classification complete (LOW/MEDIUM/HIGH)")
print("    Rules: HIGH = 3+ complaints OR 10+ workers | MEDIUM = 2+ complaints OR 5+ workers")

# COMMAND ----------

# DBTITLE 1,Step 5: Build Gold Layer and Display
# Step 5: Build & Save Gold Table
print("[5/5] Building Gold layer...")

# Map threat levels to numeric scores for severity calculation
threat_score_map = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
mapping_expr = F.create_map([F.lit(x) for pair in threat_score_map.items() for x in pair])

gold_df = classified_df \
    .withColumn(
        "severity_score",
        F.col("total_complaints") * mapping_expr[F.col("threat_label")]
    ) \
    .withColumn(
        "escalate_immediately",
        (F.col("threat_label") == "HIGH") | (F.col("violence_flagged") == True)
    )

gold_df.write.format("delta").mode("overwrite").saveAsTable("nyaya_hackathon.default.gold_heatmap")
print("    ✓ Gold table saved → nyaya_hackathon.default.gold_heatmap")

# Final Display
print("\n══ FINAL GOLD TABLE ══")
display(gold_df)