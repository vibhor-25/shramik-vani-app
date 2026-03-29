# Databricks notebook source
# DBTITLE 1,Complete Data Flow Overview
# MAGIC %md
# MAGIC # 🔗 Shramik Vani: Data Extraction Pipeline
# MAGIC
# MAGIC ## ⚠️ PREREQUISITE: Run Audio_to_text notebook FIRST!
# MAGIC
# MAGIC This notebook is **MEMBER 2 (Data_Extraction)** - the JSON ingestion layer.
# MAGIC
# MAGIC **This notebook does NOT transcribe audio.** That happens in Audio_to_text notebook.
# MAGIC
# MAGIC ## 📊 Correct Running Order
# MAGIC ```
# MAGIC 1. Audio_to_text notebook     → Audio files → English text → JSON files
# MAGIC 2. THIS notebook              → JSON files → bronze_calls table
# MAGIC 3. Labor Complaint Pipeline   → bronze_calls → indic_nlp → silver + gold
# MAGIC 4. Legal RAG Pipeline         → gold_heatmap HIGH → legal actions
# MAGIC ```
# MAGIC
# MAGIC ## 📌 What This Notebook Does
# MAGIC 1. Checks for JSON files created by Audio_to_text
# MAGIC 2. Auto Loader ingests JSON into `bronze_calls` table
# MAGIC 3. Verifies data ready for handoff to Labor Pipeline
# MAGIC
# MAGIC ## 🚫 What This Notebook Does NOT Do
# MAGIC * NO audio transcription (done by Audio_to_text)
# MAGIC * NO entity extraction (done by Labor Pipeline via indic_nlp)
# MAGIC * NO classification or aggregation
# MAGIC
# MAGIC ## Expected Results
# MAGIC * **Bronze**: N calls with transcribed text from Audio_to_text
# MAGIC * **Silver**: (Labor Pipeline) N processed complaints with entities
# MAGIC * **Gold**: (Labor Pipeline) Aggregated contractor hotspots
# MAGIC * **Legal**: (Legal RAG) Legal actions for HIGH threats
# MAGIC
# MAGIC ✅ **First run Audio_to_text notebook, then run cells 2-10 here**

# COMMAND ----------

# DBTITLE 1,🧹 Cleanup Old Data (Run First)
# Clean up old data for fresh demo start
import shutil
import os

print("[CLEANUP] Starting fresh demo setup...\n")

# 1. Delete old JSON files
folder_path = "/Volumes/nyaya_hackathon/shramik_grid/incoming_calls"
if os.path.exists(folder_path):
    for file in os.listdir(folder_path):
        if file.endswith('.json'):
            os.remove(os.path.join(folder_path, file))
    print("✓ Deleted old JSON files")

# 2. Drop the bronze_calls table to start fresh
spark.sql("DROP TABLE IF EXISTS nyaya_hackathon.shramik_grid.bronze_calls")
print("✓ Dropped old bronze_calls table")

# 3. Delete checkpoint and schema directories
for dir_name in ['_checkpoint_bronze', '_schema']:
    dir_path = f"/Volumes/nyaya_hackathon/shramik_grid/incoming_calls/{dir_name}"
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
        print(f"✓ Deleted {dir_name} directory")

print("\n✅ Cleanup complete! Ready for 6 demo complaints.\n")

# COMMAND ----------

# Create the volume (folder) where fake call JSON files will be dropped
spark.sql("CREATE VOLUME IF NOT EXISTS nyaya_hackathon.shramik_grid.incoming_calls")

# COMMAND ----------

import json
import os

print("✓ Data_Extraction - Waiting for JSON files from Audio_to_text notebook...\n")

# ============================================
# IMPORTANT: RUN AUDIO_TO_TEXT NOTEBOOK FIRST!
# ============================================
# 
# Data Flow:
#   1. Audio_to_text notebook transcribes audio → creates JSON files
#   2. THIS notebook (Data_Extraction) reads those JSON files
#   3. Auto Loader ingests JSON → bronze_calls table
#   4. Labor Pipeline reads bronze_calls → entity extraction
# 
# This cell just verifies that Audio_to_text created JSON files.
# ============================================

OUTPUT_PATH = "/Volumes/nyaya_hackathon/shramik_grid/incoming_calls"

print("Checking for JSON files created by Audio_to_text notebook...\n")

# Check if JSON files exist
json_files = [f for f in os.listdir(OUTPUT_PATH) if f.endswith('.json')]

if not json_files:
    print("❌ NO JSON FILES FOUND!")
    print("\n⚠️  You need to run Audio_to_text notebook FIRST!")
    print("\nCorrect order:")
    print("  1. Run Audio_to_text notebook → creates JSON files")
    print("  2. Run THIS notebook (Data_Extraction) → ingests JSON files")
    print("  3. Run Labor Complaint Pipeline → entity extraction")
    print("  4. Run Legal RAG Pipeline → legal actions\n")
else:
    print(f"✅ Found {len(json_files)} JSON files from Audio_to_text notebook\n")
    
    # Show sample
    sample_file = os.path.join(OUTPUT_PATH, json_files[0])
    with open(sample_file, 'r', encoding='utf-8') as f:
        sample_data = json.load(f)
    
    print("Sample JSON structure:")
    print(f"  - call_id: {sample_data.get('call_id')}")
    print(f"  - hindi_audio_text: \"{sample_data.get('hindi_audio_text', '')[:50]}...\"")
    print(f"  - audio_source: {sample_data.get('audio_source')}")
    print(f"  - timestamp: {sample_data.get('timestamp')}")
    
    print("\n✓ Ready to ingest into bronze_calls table (run cell 7)")
    print("\n📋 Clean Data Flow:")
    print("  1. ✓ Audio_to_text: Audio → Text → JSON files")
    print("  2. → Data_Extraction (THIS): JSON → bronze_calls table")
    print("  3. → Labor Pipeline: bronze_calls → indic_nlp → entities")
    print("  4. → Legal RAG: HIGH priority → legal actions\n")

# COMMAND ----------

import os
files = os.listdir("/Volumes/nyaya_hackathon/shramik_grid/incoming_calls")
print(f"Files created: {len(files)}")
print("First 3 files:", files[:3])

# COMMAND ----------

# MAGIC %md
# MAGIC Part 2 — The Bronze Table (Auto Loader) What is Auto Loader? It's Databricks' magic tool that watches a folder and automatically ingests any new files that appear there into a Delta table. It's like a conveyor belt — you drop files at one end, and they appear in the table at the other end. What is a Delta Table? It's like a supercharged database table — it has versioning, supports streaming, and is stored as efficient Parquet files. It's the foundation of the Bronze/Silver/Gold pattern.
# MAGIC
# MAGIC Creating a Bronze table now

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType

# SIMPLIFIED schema - just transcribed text, no entity extraction
# Labor Complaint Pipeline will extract entities via indic_nlp
json_schema = StructType([
    StructField("call_id", LongType(), True),
    StructField("pincode", StringType(), True),
    StructField("hindi_audio_text", StringType(), True),  # Just the transcribed text
    StructField("timestamp", StringType(), True),
    StructField("call_duration_seconds", LongType(), True),
    StructField("audio_source", StringType(), True)
])

print("✓ Simplified schema for transcribed text only\n")
print("📋 Clean data flow:")
print("  Audio → Text (Data_Extraction) → Entities (Labor Pipeline via indic_nlp)\n")

# Auto Loader with simplified schema
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(json_schema)
    .load("/Volumes/nyaya_hackathon/shramik_grid/incoming_calls")
)

# Write to bronze_calls table
(
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option(
        "checkpointLocation",
        "/Volumes/nyaya_hackathon/shramik_grid/incoming_calls/_checkpoint_bronze"
    )
    .toTable("nyaya_hackathon.shramik_grid.bronze_calls")
    .awaitTermination()
)

print("✓ Bronze table loaded with transcribed audio!\n")
print("Fields:")
print("  - call_id: Unique identifier")
print("  - pincode: Will be extracted downstream")
print("  - hindi_audio_text: Transcribed English text from Hindi audio")
print("  - timestamp: Call time")
print("  - audio_source: Original audio filename\n")
print("→ Labor Complaint Pipeline will extract contractor, issue_type, etc. using indic_nlp")

# COMMAND ----------

# Verify the simplified bronze table with transcribed text
df_bronze = spark.read.table("nyaya_hackathon.shramik_grid.bronze_calls")

print("="*70)
print("BRONZE TABLE - TRANSCRIBED AUDIO TEXT")
print("="*70)
print(f"\nTotal calls: {df_bronze.count()}")
print("\nSimplified schema (text only, no entities):")
df_bronze.printSchema()

print("\n" + "="*70)
print("SAMPLE RECORDS - Just transcribed text")
print("="*70)
df_bronze.select(
    "call_id", 
    "hindi_audio_text",
    "audio_source"
).show(10, truncate=False)

print("\n→ Entity extraction (contractor, issue_type, etc.) will be done by:")
print("   Labor Complaint Pipeline using indic_nlp module")

# COMMAND ----------

# DBTITLE 1,Handoff to Labor Complaint Pipeline
# MAGIC %md
# MAGIC # 🔗 Handoff to Next Pipeline
# MAGIC
# MAGIC **Vibhor's bronze_calls table is ready!**
# MAGIC
# MAGIC The **Labor Complaint Pipeline** will now:
# MAGIC 1. Read from `nyaya_hackathon.shramik_grid.bronze_calls`
# MAGIC 2. Process each call through **indic_nlp** (Member 2's real NLP)
# MAGIC 3. Create `nyaya_hackathon.default.silver_complaints`
# MAGIC 4. Create `nyaya_hackathon.default.gold_heatmap`
# MAGIC 5. Feed HIGH-priority complaints to the **Legal RAG Pipeline**

# COMMAND ----------

# DBTITLE 1,Verify Bronze Table Ready for Handoff
# Verify the bronze_calls table is ready for handoff
df_bronze = spark.read.table("nyaya_hackathon.shramik_grid.bronze_calls")

print("="*70)
print("VIBHOR PIPELINE - DATA READY FOR HANDOFF")
print("="*70)
print(f"\n✓ Total calls transcribed: {df_bronze.count()}")
print(f"\n✓ Table location: nyaya_hackathon.shramik_grid.bronze_calls")

# Show sample data
print("\n✓ Sample transcribed text:")
df_bronze.select("call_id", "hindi_audio_text", "audio_source").show(3, truncate=False)

print("\n" + "="*70)
print("✅ READY FOR LABOR COMPLAINT PIPELINE")
print("="*70)
print("\n📋 Clean Data Flow:")
print("  1. ✓ Audio → Text (THIS NOTEBOOK)")
print("  2. → Labor Pipeline reads bronze_calls")
print("  3. → Labor Pipeline calls indic_nlp.process_hindi_audio_text()")
print("     (extracts: contractor, work_site, issue_type, violence, worker_count)")
print("  4. → Creates silver_complaints with entities")
print("  5. → Creates gold_heatmap with aggregation")
print("  6. → Legal RAG processes HIGH priority cases\n")

# COMMAND ----------

# DBTITLE 1,Complete End-to-End Data Flow
print("""
╭──────────────────────────────────────────────────────────────────╮
│          SHRAMIK VANI - COMPLETE END-TO-END DATA FLOW            │
╰──────────────────────────────────────────────────────────────────╯

╭──────────────────────────────────────────────────────────────────╮
│ STEP 1: Audio_to_text - Audio Transcription (RUN FIRST!)       │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ✓ Read audio files from /audd folder                          │
│  ✓ Transcribe Hindi audio → English text via Sarvam STT       │
│  ✓ Save as JSON files in incoming_calls volume                  │
│                                                                  │
│  OUTPUT: JSON files with transcribed text                        │
│  Fields: {call_id, pincode, hindi_audio_text, timestamp}         │
│                                                                  │
╰──────────────────────────────────────────────────────────────────╯
                           |
                           ↓ JSON files ready
                           |
╭──────────────────────────────────────────────────────────────────╮
│ STEP 2: Data_Extraction - JSON Ingestion (THIS NOTEBOOK)        │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ✓ Read JSON files from incoming_calls volume                  │
│  ✓ Auto Loader ingests JSON → bronze_calls Delta table        │
│                                                                  │
│  OUTPUT: nyaya_hackathon.shramik_grid.bronze_calls               │
│  Schema: {call_id, pincode, hindi_audio_text, timestamp}         │
│                                                                  │
│  📌 NO ENTITY EXTRACTION - just data ingestion!                │
│                                                                  │
╰──────────────────────────────────────────────────────────────────╯
                           |
                           ↓ reads bronze_calls
                           |
╭──────────────────────────────────────────────────────────────────╮
│ STEP 3: indic_nlp.py - NLP Processing Module                    │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  process_hindi_audio_text(english_text) → {                     │
│    "english_text": "...",                                        │
│    "contractor_name": "...",                                    │
│    "work_site": "...",                                          │
│    "issue_type": "WAGE_THEFT/ASSAULT/...",                      │
│    "violence_threatened": true/false,                           │
│    "worker_count_mentioned": int                                │
│  }                                                               │
│                                                                  │
│  Uses Sarvam API for entity extraction from English text        │
│                                                                  │
╰──────────────────────────────────────────────────────────────────╯
                           |
                           ↓ called by
                           |
╭──────────────────────────────────────────────────────────────────╮
│ STEP 4: Labor Complaint Pipeline - Entity Extraction & Aggreg   │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  [1] Read bronze_calls (transcribed text)                        │
│  [2] Process EACH call through indic_nlp.process_hindi_audio_text│
│  [3] Extract entities: contractor, work_site, issue_type, etc.   │
│  [4] Create Silver Layer                                         │
│      OUTPUT: nyaya_hackathon.default.silver_complaints           │
│  [5] Aggregate by pincode + contractor                           │
│  [6] Rule-based threat classification:                           │
│      HIGH   = 3+ complaints OR 10+ workers                       │
│      MEDIUM = 2+ complaints OR 5+ workers                        │
│  [7] Create Gold Layer                                           │
│      OUTPUT: nyaya_hackathon.default.gold_heatmap                │
│                                                                  │
╰──────────────────────────────────────────────────────────────────╯
                           |
                           ↓ reads silver + gold
                           |
╭──────────────────────────────────────────────────────────────────╮
│ STEP 5: Legal RAG Pipeline - Legal Action Generation            │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  [1] Filter HIGH-priority complaints (escalate_immediately=true) │
│      FROM: silver_complaints JOIN gold_heatmap                   │
│  [2] For each HIGH complaint:                                    │
│      → Retrieve relevant BNS 2023 legal sections (FAISS RAG)    │
│      → Generate legal action (Databricks Foundation Model)      │
│      → Translate to Hindi if needed (Sarvam API)                │
│  [3] Store legal actions                                         │
│      OUTPUT: workspace.default.gold_legal_actions                │
│                                                                  │
╰──────────────────────────────────────────────────────────────────╯

══════════════════════════════════════════════════════════════════

📋 CORRECT RUNNING ORDER:

  1. Audio_to_text:             Audio files → English text → JSON files
                                ↓
  2. Data_Extraction (THIS):    JSON files → bronze_calls table
                                ↓
  3. Labor (indic_nlp):         English text → Entities extraction
                                ↓
  4. Labor (silver_complaints): Processed complaints with entities
                                ↓
  5. Labor (gold_heatmap):      Aggregated contractor hotspots
                                ↓
  6. Legal (gold_legal_actions): HIGH priority legal actions

══════════════════════════════════════════════════════════════════

✅ DATA_EXTRACTION JOB IS COMPLETE!

The bronze_calls table contains transcribed text from Audio_to_text notebook,
ready for entity extraction by the Labor Complaint Pipeline.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # ✅ Bronze Layer Complete!
# MAGIC
# MAGIC **Vibhor's Job is DONE** - You've created the `bronze_calls` table with 200 fake calls.
# MAGIC
# MAGIC ## Data Flow:
# MAGIC ```
# MAGIC Vibhor (THIS notebook):
# MAGIC   ✓ Generate 200 fake Hindi complaints
# MAGIC   ✓ Save to bronze_calls table
# MAGIC   
# MAGIC        ↓
# MAGIC        
# MAGIC Labor Complaint Pipeline:
# MAGIC   → Reads bronze_calls
# MAGIC   → Processes with REAL indic_nlp (Member 2)
# MAGIC   → Creates silver_complaints
# MAGIC   → Creates gold_heatmap
# MAGIC   
# MAGIC        ↓
# MAGIC        
# MAGIC Legal RAG Pipeline:
# MAGIC   → Reads silver_complaints + gold_heatmap
# MAGIC   → Generates legal actions for HIGH priority cases
# MAGIC ```
# MAGIC
# MAGIC **The cells below (8-14) are OLD CODE** - they were used when Vibhor did its own processing.
# MAGIC
# MAGIC Now the Labor Complaint Pipeline handles all NLP processing using the **real indic_nlp module** from Member 2.

# COMMAND ----------

# ============================================================
# OBSOLETE: Old dummy entity extraction function
# ============================================================
# 
# This cell is NO LONGER USED.
# 
# Previously: Vibhor did its own entity extraction here
# Now: Labor Complaint Pipeline does ALL entity extraction via indic_nlp
# 
# Data Flow (NEW):
#   1. Vibhor (THIS notebook): Audio → English text
#   2. Labor Pipeline: English text → Entities via indic_nlp
#   3. Labor Pipeline: Entities → Silver + Gold tables
#   4. Legal RAG: HIGH priority → Legal actions
# 
# This keeps the code clean and avoids duplication!

print("⚠️  This cell is OBSOLETE and not used anymore.")
print("Entity extraction is now done by Labor Complaint Pipeline using indic_nlp.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC # ⚠️ OBSOLETE SECTION - Cells 14-19
# MAGIC
# MAGIC **These cells are NO LONGER USED.**
# MAGIC
# MAGIC They were part of the old flow where Vibhor created Silver and Gold tables.
# MAGIC
# MAGIC **NEW FLOW:**
# MAGIC * Vibhor (THIS notebook): Audio → English text → bronze_calls
# MAGIC * **Labor Complaint Pipeline**: bronze_calls → indic_nlp → silver_complaints + gold_heatmap
# MAGIC * Legal RAG Pipeline: gold_heatmap HIGH priority → legal actions
# MAGIC
# MAGIC Do NOT run cells 14-19. They create duplicate tables with old dummy data.

# COMMAND ----------

# OBSOLETE: Old UDF registration
print("⚠️  This cell is OBSOLETE. Do not run.")
print("Entity extraction is now done by Labor Complaint Pipeline.")

# COMMAND ----------

# OBSOLETE: Old Silver table creation
print("⚠️  This cell is OBSOLETE. Do not run.")
print("Silver table is now created by Labor Complaint Pipeline using indic_nlp.")

# COMMAND ----------

# OBSOLETE: Old Silver table verification
print("⚠️  This cell is OBSOLETE. Do not run.")
print("Check silver table in Labor Complaint Pipeline instead.")

# COMMAND ----------

# MAGIC %md
# MAGIC # ⚠️ OBSOLETE: Gold Table
# MAGIC
# MAGIC **Do NOT run cell 19.** 
# MAGIC
# MAGIC Gold table is now created by Labor Complaint Pipeline.

# COMMAND ----------

# OBSOLETE: Old Gold table creation
print("⚠️  This cell is OBSOLETE. Do not run.")
print("Gold heatmap is now created by Labor Complaint Pipeline using real indic_nlp entity extraction.")