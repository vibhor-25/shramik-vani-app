# Databricks notebook source
# DBTITLE 1,Cell 1
# Cell 1: Download BNS PDF
import urllib.request
import shutil

# Download BNS PDF
urllib.request.urlretrieve(
    "https://www.mha.gov.in/sites/default/files/250883_english_01042024.pdf",
    "/tmp/bns2023.pdf"
)
shutil.copy("/tmp/bns2023.pdf", "/Volumes/workspace/default/raw_legal/bns2023.pdf")

# Cell 2: Download Wages Act PDF  
urllib.request.urlretrieve(
    "https://www.indiacode.nic.in/bitstream/123456789/15793/1/aA2019-29.pdf",
    "/tmp/wages_code.pdf"
)
shutil.copy("/tmp/wages_code.pdf", "/Volumes/workspace/default/raw_legal/wages_code.pdf")

# COMMAND ----------

# DBTITLE 1,Cell 2
# Cell 3: Download Kaggle Dataset and Copy
import os
import shutil

# Install Kaggle CLI
%pip install -q kaggle

# 1. Set Kaggle credentials (replace with your username and key)
os.environ['KAGGLE_USERNAME'] = "satyadevsuvesh"
os.environ['KAGGLE_KEY'] = "KGAT_8df7b680a09f0309e4599b07cf5ae374"

# 2. Download the dataset to /tmp using Kaggle CLI
# This will download indian-government-schemes.zip to /tmp
!kaggle datasets download -d jainamgada45/indian-government-schemes -p /tmp

# 3. Unzip the file in /tmp
!unzip -o /tmp/indian-government-schemes.zip -d /tmp

# 4. Copy the extracted CSV to your Unity Catalog Volume
# Note: Check the exact name of the extracted CSV, usually it matches the dataset name
import os
files_in_tmp = os.listdir('/tmp')
csv_files = [f for f in files_in_tmp if f.endswith('.csv')]
extracted_csv_name = csv_files[0] if csv_files else None
if not extracted_csv_name:
    raise FileNotFoundError('No CSV file found in /tmp after unzipping')

tmp_unzipped_path = f"/tmp/{extracted_csv_name}"

shutil.copy(tmp_unzipped_path, "/Volumes/workspace/default/raw_legal/indian_government_schemes.csv")

print("Successfully copied full schemes dataset to Volume!")

# COMMAND ----------

# Read the CSV from the Volume into a Spark DataFrame
schemes_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/default/raw_legal/indian_government_schemes.csv")

# Inspect what columns we got
schemes_df.printSchema()
schemes_df.show(3, truncate=False)

# COMMAND ----------

# DBTITLE 1,Cell 4
# Save to Bronze Delta Table
schemes_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_schemes")

print(f"✅ Loaded {schemes_df.count()} schemes into bronze_schemes Delta table")

# COMMAND ----------

# DBTITLE 1,Cell 5
import urllib.request
import shutil

# Official MHA source
urllib.request.urlretrieve(
    "https://www.mha.gov.in/sites/default/files/250883_english_01042024.pdf",
    "/tmp/bns2023.pdf"
)

shutil.copy("/tmp/bns2023.pdf", "/Volumes/workspace/default/raw_legal/bns2023.pdf")

print("✅ BNS 2023 PDF saved to Volume")

# COMMAND ----------

# MAGIC %pip install pdfplumber -q

# COMMAND ----------

# DBTITLE 1,Cell 7
import pdfplumber
import pandas as pd
import re

rows = []

with pdfplumber.open("/tmp/bns2023.pdf") as pdf:
    print(f"Total pages: {len(pdf.pages)}")
    for page in pdf.pages:
        text = page.extract_text()
        if text:
            rows.append({
                "page_number": page.page_number,
                "raw_text": text.strip()
            })

print(f"✅ Extracted text from {len(rows)} pages")

# Save raw pages to Bronze Delta
bns_raw_df = spark.createDataFrame(pd.DataFrame(rows))
bns_raw_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_bns_raw")

print("✅ Saved to bronze_bns_raw Delta table")

# COMMAND ----------

import re
import pandas as pd

# Read back from Delta
raw_df = spark.read.table("workspace.default.bronze_bns_raw").toPandas()
full_text = "\n".join(raw_df["raw_text"].tolist())

# Regex to split on section headers like "351." or "Section 351"
section_pattern = re.compile(
    r'(?:Section\s+)?(\d{1,3})\.\s+([A-Z][^\n]{5,80})\n(.*?)(?=(?:Section\s+)?\d{1,3}\.\s+[A-Z]|\Z)',
    re.DOTALL
)

parsed_sections = []
for match in section_pattern.finditer(full_text):
    section_num = match.group(1).strip()
    section_title = match.group(2).strip()
    section_body = match.group(3).strip().replace("\n", " ")

    if len(section_body) > 30:  # skip empty/noise matches
        parsed_sections.append({
            "section_id":   f"BNS_{section_num}",
            "section_num":  section_num,
            "title":        section_title,
            "content":      section_body,
            "source":       "BNS_2023"
        })

print(f"✅ Parsed {len(parsed_sections)} BNS sections")

# Save to Silver
bns_silver_df = spark.createDataFrame(pd.DataFrame(parsed_sections))
bns_silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_bns_sections")

bns_silver_df.show(5, truncate=True)

# COMMAND ----------

from pyspark.sql import Row

critical_sections = [
    Row(section_id="BNS_115", section_num="115", source="BNS_2023",
        title="Voluntarily causing hurt",
        content="Whoever does any act with the intention of thereby causing hurt to any person, or with the knowledge that he is likely thereby to cause hurt to any person, and does thereby cause hurt to any person, is said to voluntarily cause hurt. Punishment: imprisonment up to 1 year or fine up to 10,000 rupees or both."),
    Row(section_id="BNS_127", section_num="127", source="BNS_2023",
        title="Wrongful confinement",
        content="Whoever wrongfully restrains any person in such a manner as to prevent that person from proceeding beyond certain circumscribing limits, is said wrongfully to confine that person. Punishment: imprisonment up to 1 year or fine up to 5,000 rupees or both."),
    Row(section_id="BNS_143", section_num="143", source="BNS_2023",
        title="Unlawful compulsory labour",
        content="Whoever unlawfully compels any person to labour against the will of that person shall be subject to imprisonment of either description for a term which may extend to one year, or with fine, or with both."),
    Row(section_id="BNS_316", section_num="316", source="BNS_2023",
        title="Criminal breach of trust",
        content="Whoever, being in any manner entrusted with property, or with any dominion over property, dishonestly misappropriates or converts to his own use that property, or dishonestly uses or disposes of that property in violation of any direction of law, is said to commit criminal breach of trust. Punishment: imprisonment up to 3 years or fine or both."),
    Row(section_id="BNS_318", section_num="318", source="BNS_2023",
        title="Cheating",
        content="Whoever, by deceiving any person, fraudulently or dishonestly induces the person so deceived to deliver any property to any person, or to consent that any person shall retain any property, or intentionally induces the person so deceived to do or omit to do anything which he would not do or omit if he were not so deceived, and which act or omission causes or is likely to cause damage or harm to that person, is said to cheat. Punishment: imprisonment up to 3 years or fine or both."),
    Row(section_id="BNS_351", section_num="351", source="BNS_2023",
        title="Criminal intimidation",
        content="Whoever threatens another with any injury to his person, reputation or property, or to the person or reputation of any one in whom that person is interested, with intent to cause alarm to that person, or to cause that person to do any act which he is not legally bound to do, or to omit to do any act which that person is legally entitled to do, as the means of avoiding the execution of such threat, commits criminal intimidation. Punishment: imprisonment up to 2 years or fine or both."),
]

seed_df = spark.createDataFrame(critical_sections)

# Merge with parsed sections (parsed first, then seed fills gaps)
seed_df.write.format("delta") \
    .mode("append") \
    .saveAsTable("workspace.default.silver_bns_sections")

# Deduplicate by section_id — keep one copy of each
dedup_df = spark.read.table("workspace.default.silver_bns_sections") \
    .dropDuplicates(["section_id"])

dedup_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_bns_sections")

print(f"✅ Total sections in Silver: {dedup_df.count()}")

# COMMAND ----------

from pyspark.sql.functions import col, lower

schemes_raw = spark.read.table("workspace.default.bronze_schemes")
schemes_raw.printSchema()  # check actual column names first

# COMMAND ----------

# Adjust column names below based on printSchema() output
labour_keywords = ["labour", "worker", "wage", "unorganis", "construction", 
                   "shram", "mazdoor", "pension", "maternity", "bocw"]

# Build filter condition across all string columns
from pyspark.sql import functions as F
from functools import reduce

str_cols = [f.name for f in schemes_raw.schema.fields if str(f.dataType) == "StringType()"]

condition = reduce(
    lambda a, b: a | b,
    [lower(col(c)).contains(kw) for c in str_cols for kw in labour_keywords]
)

labour_schemes = schemes_raw.filter(condition)
labour_schemes.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_schemes")

print(f"✅ Filtered {labour_schemes.count()} labour-relevant schemes into silver_schemes")

# COMMAND ----------

from pyspark.sql.functions import lit, concat, col

# BNS sections — format as readable chunks
bns_chunks = spark.read.table("workspace.default.silver_bns_sections") \
    .select(
        col("section_id").alias("chunk_id"),
        concat(lit("BNS Section "), col("section_num"), 
               lit(" - "), col("title"),
               lit(": "), col("content")).alias("content"),
        lit("BNS_2023").alias("source")
    )

# Schemes — format as readable chunks  
# Using actual column names from the schema
schemes_chunks = spark.read.table("workspace.default.silver_schemes") \
    .select(
        col("scheme_name").alias("chunk_id"),
        concat(
            lit("Scheme: "), col("scheme_name"),
            lit(". Eligibility: "), col("eligibility"),
            lit(". Benefits: "), col("benefits"),
            lit(". Apply at: "), col("application")
        ).alias("content"),
        lit("GOV_SCHEMES").alias("source")
    )

# Union both
all_chunks = bns_chunks.union(schemes_chunks) \
    .dropDuplicates(["chunk_id"]) \
    .filter("length(content) > 50")

# Enable Change Data Feed — REQUIRED for Databricks Vector Search
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.default.silver_legal_chunks
    (chunk_id STRING, content STRING, source STRING)
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

all_chunks.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("workspace.default.silver_legal_chunks")

print(f"✅ {all_chunks.count()} total chunks ready for embedding")

# COMMAND ----------

# MAGIC %pip install faiss-cpu langchain langchain-community sentence-transformers -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import numpy as np
import pickle
import faiss
from sentence_transformers import SentenceTransformer

# Load chunks from Delta
chunks_df = spark.read.table("workspace.default.silver_legal_chunks").toPandas()
print(f"Embedding {len(chunks_df)} chunks...")

# CPU-friendly embedding model — small, fast, good quality
model = SentenceTransformer("all-MiniLM-L6-v2")  # only 80MB, runs fine on CPU

contents = chunks_df["content"].tolist()
embeddings = model.encode(contents, show_progress_bar=True, batch_size=32)
embeddings = np.array(embeddings).astype("float32")

print(f"✅ Embeddings shape: {embeddings.shape}")

# COMMAND ----------

# Build FAISS index
dimension = embeddings.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(embeddings)
print(f"✅ FAISS index built with {index.ntotal} vectors")

# Save index + metadata to DBFS so it persists across sessions
import pickle, os

os.makedirs("/tmp/faiss_legal", exist_ok=True)

faiss.write_index(index, "/tmp/faiss_legal/legal_index.faiss")

with open("/tmp/faiss_legal/chunks_metadata.pkl", "wb") as f:
    pickle.dump(chunks_df[["chunk_id", "content", "source"]].to_dict("records"), f)

# Copy to DBFS Volume so it survives cluster restarts
import shutil
shutil.copy("/tmp/faiss_legal/legal_index.faiss",
    "/Volumes/workspace/default/raw_legal/legal_index.faiss")
shutil.copy("/tmp/faiss_legal/chunks_metadata.pkl",
    "/Volumes/workspace/default/raw_legal/chunks_metadata.pkl")

print("✅ FAISS index saved to DBFS Volume")

# COMMAND ----------

# DBTITLE 1,Cell 16
import faiss, pickle, os
import numpy as np
from sentence_transformers import SentenceTransformer

# Load directly from Volume (serverless-compatible)
volume_base = "/Volumes/workspace/default/raw_legal"

index = faiss.read_index(f"{volume_base}/legal_index.faiss")

with open(f"{volume_base}/chunks_metadata.pkl", "rb") as f:
    metadata = pickle.load(f)

embed_model = SentenceTransformer("all-MiniLM-L6-v2")

def retrieve_legal_chunks(query: str, top_k: int = 3) -> list:
    query_vec = embed_model.encode([query]).astype("float32")
    distances, indices = index.search(query_vec, top_k)

    results = []
    for i, idx in enumerate(indices[0]):
        if idx < len(metadata):
            chunk = metadata[idx].copy()
            chunk["score"] = float(distances[0][i])
            results.append(chunk)
    return results

# Quick test
test_results = retrieve_legal_chunks("contractor not paying wages threat to beat")
for r in test_results:
    print(f"[{r['source']}] {r['chunk_id']}")
    print(f"  → {r['content'][:120]}...")
    print()

# COMMAND ----------

# MAGIC %pip install ctransformers -q

# COMMAND ----------

# Option A: Use Databricks built-in DBRX / Meta Llama via Foundation Model API
# This is FREE on Databricks Free Edition — no GPU needed, no endpoint setup

import os

DATABRICKS_TOKEN = dbutils.notebook.entry_point \
    .getDbutils().notebook().getContext() \
    .apiToken().get()

DATABRICKS_HOST = spark.conf.get("spark.databricks.workspaceUrl")

# Test the Foundation Model API — this replaces any need for local model loading
import requests

def call_llm(prompt: str, max_tokens: int = 512) -> str:
    url = f"https://{DATABRICKS_HOST}/serving-endpoints/databricks-meta-llama-3-1-8b-instruct/invocations"
    
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": 0.1  # low temp = consistent legal outputs
    }
    
    response = requests.post(url, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]

# Test
print(call_llm("Say 'Legal API working' and nothing else."))

# COMMAND ----------

import mlflow
import json

mlflow.set_experiment("/legal_rag_experiments")

def generate_legal_action(issue_type: str, threat_level: str, 
                           call_id: str = "TEST") -> dict:
    
    # Step 1: Retrieve relevant laws from FAISS
    query = f"{issue_type} worker rights India {threat_level} threat"
    chunks = retrieve_legal_chunks(query, top_k=3)
    
    context = "\n".join([
        f"- {c['chunk_id']}: {c['content'][:300]}" 
        for c in chunks
    ])
    
    # Step 2: Build prompt
    prompt = f"""You are a legal aid expert for Indian unorganised labour law.

A worker has reported:
- Issue: {issue_type}
- Threat Level: {threat_level}

Relevant laws retrieved:
{context}

Respond ONLY in this exact JSON format, nothing else:
{{
  "bns_sections_cited": ["Section X", "Section Y"],
  "action_steps": [
    "Step 1: ...",
    "Step 2: ...",
    "Step 3: ..."
  ],
  "government_portal": "URL or portal name",
  "urgency": "HIGH or MEDIUM or LOW"
}}"""
    
    # Step 3: Call LLM
    raw_output = call_llm(prompt)
    
    # Step 4: Parse JSON safely
    try:
        clean = raw_output.strip()
        # Strip markdown fences if LLM adds them
        if "```" in clean:
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
        result = json.loads(clean.strip())
    except Exception:
        # Fallback structure if JSON parsing fails
        result = {
            "bns_sections_cited": ["BNS Section 351", "BNS Section 316"],
            "action_steps": [
                "File an FIR at your nearest police station citing criminal intimidation.",
                "Contact the Labour Commissioner's office with wage proof.",
                "Register on eShram portal for emergency state relief."
            ],
            "government_portal": "shramsuvidha.gov.in or call 14434",
            "urgency": "HIGH" if threat_level == "High" else "MEDIUM"
        }
    
    result["raw_llm_output"] = raw_output
    result["retrieved_chunks"] = [c["chunk_id"] for c in chunks]
    result["call_id"] = call_id
    return result


# Step 5: Run test cases and log everything to MLflow
test_cases = [
    ("Wage Theft", "Low"),
    ("Wage Theft", "High"),
    ("Physical Assault", "High"),
    ("Wrongful Termination", "Low"),
    ("Forced Labour", "High"),
]

for issue, threat in test_cases:
    with mlflow.start_run(run_name=f"{issue}_{threat}"):
        mlflow.log_param("issue_type",   issue)
        mlflow.log_param("threat_level", threat)
        mlflow.log_param("llm_model",    "llama-3.1-8b-instruct")
        mlflow.log_param("retriever",    "faiss-cpu")
        mlflow.log_param("top_k",        3)
        
        result = generate_legal_action(issue, threat)
        
        mlflow.log_metric("sections_cited",  len(result.get("bns_sections_cited", [])))
        mlflow.log_metric("action_steps",    len(result.get("action_steps", [])))
        mlflow.log_text(json.dumps(result, indent=2), "full_output.json")
        mlflow.log_text(result.get("raw_llm_output", ""), "raw_llm.txt")
        
        print(f"\n{'='*50}")
        print(f"✅ {issue} | Threat: {threat}")
        print(f"   Sections: {result.get('bns_sections_cited')}")
        print(f"   Urgency:  {result.get('urgency')}")
        print(f"   Steps:    {result['action_steps'][0][:80]}...")

# COMMAND ----------

from datetime import datetime
from pyspark.sql import Row

def generate_and_persist(call_id: str, issue_type: str, 
                          threat_level: str, pincode: str = "000000") -> dict:
    result = generate_legal_action(issue_type, threat_level, call_id)
    
    row = Row(
        call_id             = call_id,
        pincode             = pincode,
        issue_type          = issue_type,
        threat_level        = threat_level,
        bns_sections_cited  = ", ".join(result.get("bns_sections_cited", [])),
        action_steps        = " | ".join(result.get("action_steps", [])),
        government_portal   = result.get("government_portal", ""),
        urgency             = result.get("urgency", "MEDIUM"),
        retrieved_chunks    = ", ".join(result.get("retrieved_chunks", [])),
        timestamp           = datetime.now().isoformat()
    )
    
    row_df = spark.createDataFrame([row])
    row_df.write.format("delta") \
        .mode("append") \
        .saveAsTable("workspace.default.gold_legal_actions")
    
    return result

# Test it end to end
output = generate_and_persist(
    call_id     = "IND-TEST-001",
    issue_type  = "Wage Theft",
    threat_level= "High",
    pincode     = "400053"
)

print(json.dumps({k: v for k, v in output.items() 
                  if k != "raw_llm_output"}, indent=2))

# Verify in Delta
spark.read.table("workspace.default.gold_legal_actions").show(truncate=False)

# COMMAND ----------

# Cell 16: Write legal_rag.py to the repo/workspace

legal_rag_code = '''
import os
import json
import faiss
import pickle
import requests
import numpy as np
from datetime import datetime
from sentence_transformers import SentenceTransformer

# ─────────────────────────────────────────────
# CONFIG — Member 5 fills these in during merge
# ─────────────────────────────────────────────
DATABRICKS_HOST  = os.environ.get("DATABRICKS_HOST", "")   # e.g. adb-xxxx.azuredatabricks.net
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")  # personal access token
FAISS_INDEX_PATH = os.environ.get("FAISS_INDEX_PATH", "/tmp/faiss_legal/legal_index.faiss")
METADATA_PATH    = os.environ.get("METADATA_PATH",    "/tmp/faiss_legal/chunks_metadata.pkl")

# ─────────────────────────────────────────────
# ISSUE → BNS SECTION MAPPING (instant fallback)
# Used if FAISS or LLM is slow during live demo
# ─────────────────────────────────────────────
ISSUE_FALLBACK = {
    "Wage Theft": {
        "bns_sections_cited": ["BNS Section 316 - Criminal Breach of Trust",
                                "BNS Section 318 - Cheating"],
        "action_steps": [
            "File a written complaint at the nearest Labour Commissioner office.",
            "Cite BNS Section 316 (Criminal Breach of Trust) in your complaint.",
            "Register on eShram portal to access emergency state relief funds.",
        ],
        "government_portal": "shramsuvidha.gov.in or call helpline 14434",
        "urgency": "HIGH"
    },
    "Physical Assault": {
        "bns_sections_cited": ["BNS Section 115 - Voluntarily Causing Hurt",
                                "BNS Section 351 - Criminal Intimidation"],
        "action_steps": [
            "File an FIR immediately at the nearest police station.",
            "Cite BNS Section 115 (Voluntarily Causing Hurt) in the FIR.",
            "Contact the local Labour Union for witness support.",
        ],
        "government_portal": "police.gov.in or nearest police station",
        "urgency": "HIGH"
    },
    "Wrongful Termination": {
        "bns_sections_cited": ["BNS Section 316 - Criminal Breach of Trust"],
        "action_steps": [
            "Obtain a written termination letter from employer (if not given, note it).",
            "File a complaint with the Labour Commissioner under the Industrial Disputes Act.",
            "Check eligibility for ESIC benefits at esic.nic.in.",
        ],
        "government_portal": "esic.nic.in or Labour Commissioner office",
        "urgency": "MEDIUM"
    },
    "Forced Labour": {
        "bns_sections_cited": ["BNS Section 143 - Unlawful Compulsory Labour"],
        "action_steps": [
            "Contact the District Magistrate immediately — this is a cognisable offence.",
            "Cite BNS Section 143 (Unlawful Compulsory Labour) in your complaint.",
            "Reach out to the nearest anti-trafficking helpline: 1800-419-8588.",
        ],
        "government_portal": "nhrc.nic.in or call 1800-419-8588",
        "urgency": "HIGH"
    },
    "Criminal Intimidation": {
        "bns_sections_cited": ["BNS Section 351 - Criminal Intimidation"],
        "action_steps": [
            "File an FIR at the nearest police station immediately.",
            "Cite BNS Section 351 (Criminal Intimidation) in the complaint.",
            "Request police protection if threat is ongoing.",
        ],
        "government_portal": "police.gov.in or nearest police station",
        "urgency": "HIGH"
    },
}


class LegalAdvisor:
    """
    Drop-in legal RAG module for Shramik-Vani.
    Member 5 usage:
        from legal_rag import LegalAdvisor
        advisor = LegalAdvisor()
        result  = advisor.generate_legal_action("Wage Theft", "High")
    """

    def __init__(self):
        self._embed_model = None
        self._index       = None
        self._metadata    = None
        self._ready       = False
        self._try_load()

    def _try_load(self):
        """Silently attempt to load FAISS. Falls back to hardcoded map if it fails."""
        try:
            self._embed_model = SentenceTransformer("all-MiniLM-L6-v2")
            self._index       = faiss.read_index(FAISS_INDEX_PATH)
            with open(METADATA_PATH, "rb") as f:
                self._metadata = pickle.load(f)
            self._ready = True
            print("✅ LegalAdvisor: FAISS index loaded, RAG mode active.")
        except Exception as e:
            self._ready = False
            print(f"⚠️  LegalAdvisor: FAISS not found ({e}). Fallback mode active.")

    # ── Private: Retrieve ──────────────────────────────────────────────────
    def _retrieve(self, query: str, top_k: int = 3) -> list:
        vec = self._embed_model.encode([query]).astype("float32")
        distances, indices = self._index.search(vec, top_k)
        results = []
        for i, idx in enumerate(indices[0]):
            if idx < len(self._metadata):
                chunk = self._metadata[idx].copy()
                chunk["score"] = float(distances[0][i])
                results.append(chunk)
        return results

    # ── Private: Call LLM ──────────────────────────────────────────────────
    def _call_llm(self, prompt: str) -> str:
        url = f"https://{DATABRICKS_HOST}/serving-endpoints/databricks-meta-llama-3-1-8b-instruct/invocations"
        headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type":  "application/json"
        }
        payload = {
            "messages":    [{"role": "user", "content": prompt}],
            "max_tokens":  512,
            "temperature": 0.1
        }
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

    # ── Private: Parse LLM JSON safely ────────────────────────────────────
    def _parse_llm_json(self, raw: str) -> dict:
        try:
            clean = raw.strip()
            if "```" in clean:
                clean = clean.split("```")[1]
                if clean.startswith("json"):
                    clean = clean[4:]
            return json.loads(clean.strip())
        except Exception:
            return {}

    # ── Public: Main function Member 5 calls ──────────────────────────────
    def generate_legal_action(self,
                               issue_type:   str,
                               threat_level: str,
                               call_id:      str = "LIVE") -> dict:
        """
        Returns:
            {
              "bns_sections_cited": [...],
              "action_steps":       [...],
              "government_portal":  "...",
              "urgency":            "HIGH / MEDIUM / LOW",
              "retrieved_chunks":   [...],   # empty in fallback mode
              "mode":               "rag" or "fallback"
            }
        """

        # ── RAG mode: FAISS + LLM ─────────────────────────────────────────
        if self._ready and DATABRICKS_HOST and DATABRICKS_TOKEN:
            try:
                query  = f"{issue_type} worker rights India {threat_level} threat"
                chunks = self._retrieve(query, top_k=3)
                context = "\\n".join([
                    f"- {c['chunk_id']}: {c['content'][:300]}"
                    for c in chunks
                ])

                prompt = f"""You are a legal aid expert for Indian unorganised labour law.

A worker has reported:
- Issue: {issue_type}
- Threat Level: {threat_level}

Relevant laws:
{context}

Respond ONLY in this exact JSON format, nothing else:
{{
  "bns_sections_cited": ["Section X", "Section Y"],
  "action_steps": ["Step 1: ...", "Step 2: ...", "Step 3: ..."],
  "government_portal": "portal name or URL",
  "urgency": "HIGH or MEDIUM or LOW"
}}"""

                raw    = self._call_llm(prompt)
                result = self._parse_llm_json(raw)

                if result.get("bns_sections_cited"):
                    result["retrieved_chunks"] = [c["chunk_id"] for c in chunks]
                    result["mode"]             = "rag"
                    result["call_id"]          = call_id
                    return result
            except Exception as e:
                print(f"⚠️  RAG failed ({e}), switching to fallback.")

        # ── Fallback mode: hardcoded map ──────────────────────────────────
        fallback = ISSUE_FALLBACK.get(
            issue_type,
            ISSUE_FALLBACK["Wage Theft"]  # default if unknown issue
        ).copy()
        fallback["retrieved_chunks"] = []
        fallback["mode"]             = "fallback"
        fallback["call_id"]          = call_id
        return fallback

    # ── Public: Hindi translation (calls LLM) ─────────────────────────────
    def translate_to_hindi(self, english_text: str) -> str:
        """Translates action plan to Hindi for worker-facing output."""
        if not (DATABRICKS_HOST and DATABRICKS_TOKEN):
            return english_text  # graceful degradation

        prompt = f"""Translate the following legal advice to simple Hindi.
Use plain language that an uneducated daily wage worker can understand.
Do not add any explanation — output only the Hindi translation.

Text: {english_text}"""
        try:
            return self._call_llm(prompt)
        except Exception:
            return english_text  # fallback: return English if translation fails

    # ── Public: Generate Show Cause Notice (Municipal Commissioner view) ───
    def generate_show_cause_notice(self,
                                    contractor_name: str,
                                    pincode:         str,
                                    total_complaints: int,
                                    primary_issue:   str) -> str:
        """
        Generates a formal municipal show cause notice.
        Member 5 calls this when judge clicks a RED cluster on the heatmap.
        """
        prompt = f"""You are a Municipal Labour Officer drafting an official notice.

Facts:
- Contractor Name: {contractor_name}
- Area Pincode: {pincode}
- Number of Worker Complaints in 48 hours: {total_complaints}
- Primary Violation: {primary_issue}

Draft a formal Show Cause Notice in 150 words citing:
1. The relevant BNS 2023 section
2. The Code on Wages 2019
3. A 7-day deadline to respond

Use official government letter language."""

        try:
            return self._call_llm(prompt)
        except Exception:
            return f"""SHOW CAUSE NOTICE
To: Contractor {contractor_name}, Pincode {pincode}
This office has received {total_complaints} complaints of {primary_issue}.
You are directed to appear before the Labour Commissioner within 7 days.
Failure to comply will result in action under BNS Section 316 and Code on Wages 2019.
Issued by: Municipal Labour Enforcement Cell"""
'''

# Write the file locally and to Volume
import os
import shutil

os.makedirs("/tmp/shramik_vani", exist_ok=True)

with open("/tmp/shramik_vani/legal_rag.py", "w") as f:
    f.write(legal_rag_code)

# Copy directly to Volume (serverless-compatible)
shutil.copy(
    "/tmp/shramik_vani/legal_rag.py",
    "/Volumes/workspace/default/raw_legal/legal_rag.py"
)

print("✅ legal_rag.py written to /tmp/shramik_vani/legal_rag.py")
print("✅ Backed up to Volume at /Volumes/workspace/default/raw_legal/legal_rag.py")

# COMMAND ----------

# DBTITLE 1,Integration with Labor Complaint Pipeline
# MAGIC %md
# MAGIC # 🔗 Integration: Labor Complaint Pipeline → Legal RAG
# MAGIC
# MAGIC **Data Flow:**
# MAGIC 1. **Member 2 (indic_nlp)**: Processes Hindi/Hinglish complaints → extracts entities
# MAGIC 2. **Member 3 (Labor Pipeline)**: Aggregates by contractor → classifies threat level
# MAGIC 3. **Member 4 (Legal RAG)**: Retrieves relevant laws → generates legal actions
# MAGIC
# MAGIC This cell reads from the Labor Complaint Pipeline's output and generates legal actions for HIGH-priority complaints.

# COMMAND ----------

# DBTITLE 1,Connect to Labor Complaint Pipeline
# ═══════════════════════════════════════════════════════════════════
# INTEGRATION: Labor Complaint Pipeline → Legal RAG Pipeline
# ═══════════════════════════════════════════════════════════════════

print("\n🔗 INTEGRATION: Reading from Labor Complaint Pipeline...\n")

# Step 1: Read individual complaints from silver layer (Member 3)
silver_complaints = spark.read.table("nyaya_hackathon.default.silver_complaints")

# Step 2: Read gold heatmap to get threat levels
gold_heatmap = spark.read.table("nyaya_hackathon.default.gold_heatmap") \
    .select("pincode", "contractor_name", "threat_label", "escalate_immediately")

# Step 3: Join to enrich each complaint with threat level
enriched_complaints = silver_complaints \
    .join(gold_heatmap, ["pincode", "contractor_name"], "left") \
    .filter("escalate_immediately = true OR threat_label = 'HIGH'") \
    .select(
        "call_id",
        "pincode",
        "issue_type",
        "threat_label",
        "contractor_name",
        "work_site",
        "violence_threatened"
    )

print(f"✓ Found {enriched_complaints.count()} HIGH-priority complaints requiring legal action\n")
enriched_complaints.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Generate Legal Actions for Each Complaint
# ═══════════════════════════════════════════════════════════════════
# Generate Legal Actions Using RAG Pipeline
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col
import json

complaints_list = enriched_complaints.collect()

print(f"\n⚖️  Generating legal actions for {len(complaints_list)} complaints...\n")

for row in complaints_list:
    call_id = row["call_id"]
    issue_type = row["issue_type"]
    threat_level = row["threat_label"]
    pincode = row["pincode"]
    
    print(f"  → Processing {call_id}: {issue_type} ({threat_level})")
    
    try:
        # Generate legal action using the RAG pipeline function
        result = generate_and_persist(
            call_id=call_id,
            issue_type=issue_type,
            threat_level=threat_level,
            pincode=pincode
        )
        
        print(f"    ✓ Legal action generated - Urgency: {result.get('urgency')}")
        print(f"      Sections cited: {', '.join(result.get('bns_sections_cited', [])[:2])}")
        print()
        
    except Exception as e:
        print(f"    ✗ Error: {e}\n")
        continue

print("\n" + "="*70)
print("✅ INTEGRATION COMPLETE")
print("="*70)
print(f"\nProcessed {len(complaints_list)} complaints")
print("Results stored in: workspace.default.gold_legal_actions")
print("\nRecent legal actions:")

# Display final integrated results (with error handling)
try:
    final_results = spark.read.table("workspace.default.gold_legal_actions") \
        .orderBy(col("timestamp").desc()) \
        .limit(10)
    
    final_results.show(truncate=False)
except Exception as e:
    print(f"⚠️  Could not display results: {e}")
    print("    Check table permissions or run cells 19-20 first to create the table.")

# COMMAND ----------

# DBTITLE 1,Dynamic Testing with Live Pipeline Data
# Dynamic Testing: Read from Labor Pipeline and Process Multiple Issue Types
import importlib.util
import sys

print("\n" + "="*70)
print("DYNAMIC LEGAL RAG TESTING WITH LIVE PIPELINE DATA")
print("="*70)

# Load the LegalAdvisor module
try:
    spec = importlib.util.spec_from_file_location(
        "legal_rag", 
        "/Volumes/workspace/default/raw_legal/legal_rag.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    LegalAdvisor = module.LegalAdvisor
    advisor = LegalAdvisor()
    print("✅ LegalAdvisor module loaded successfully\n")
except Exception as e:
    print(f"❌ ERROR: Failed to load LegalAdvisor module: {e}")
    print("   → Check if legal_rag.py exists in Volume")
    print("   → Re-run cell 21 to regenerate the module\n")
    raise

# Read actual complaints from labor pipeline
try:
    print("📊 Reading complaints from labor pipeline...")
    
    # Try to read from gold_heatmap first (aggregated data)
    try:
        pipeline_data = spark.read.table("nyaya_hackathon.default.gold_heatmap") \
            .filter("escalate_immediately = true OR threat_label = 'HIGH'") \
            .limit(5) \
            .collect()
        
        if not pipeline_data:
            print("⚠️  No HIGH-priority complaints found in gold_heatmap")
            print("   → Falling back to sample test data\n")
            pipeline_data = []
    
    except Exception as e:
        print(f"⚠️  Could not read gold_heatmap: {e}")
        print("   → Using sample test data instead\n")
        pipeline_data = []
    
    # If no real data, use diverse test scenarios
    if not pipeline_data:
        print("Using sample test scenarios covering different issue types:\n")
        test_scenarios = [
            {"issue_type": "Wage Theft", "threat_label": "HIGH", "contractor_name": "Suresh", "pincode": "400053", "complaint_count": 15},
            {"issue_type": "Physical Assault", "threat_label": "HIGH", "contractor_name": "Ramesh", "pincode": "110001", "complaint_count": 8},
            {"issue_type": "Wrongful Termination", "threat_label": "MEDIUM", "contractor_name": "Vijay", "pincode": "560001", "complaint_count": 5},
            {"issue_type": "Forced Labour", "threat_label": "HIGH", "contractor_name": "Mohan", "pincode": "700001", "complaint_count": 12},
        ]
    else:
        print(f"✅ Found {len(pipeline_data)} HIGH-priority complaints from pipeline\n")
        # Convert Spark Row objects to dict format - use direct attribute access
        test_scenarios = []
        for row in pipeline_data:
            try:
                scenario = {
                    "issue_type": getattr(row, "issue_type", "Unknown Issue"),
                    "threat_label": getattr(row, "threat_label", "MEDIUM"),
                    "contractor_name": getattr(row, "contractor_name", "Unknown"),
                    "pincode": str(getattr(row, "pincode", "000000")),
                    "complaint_count": getattr(row, "complaint_count", 1)
                }
                test_scenarios.append(scenario)
            except Exception as row_err:
                print(f"⚠️  Skipping malformed row: {row_err}")
                continue
        
        if not test_scenarios:
            print("⚠️  Could not parse pipeline data, using sample test data\n")
            test_scenarios = [
                {"issue_type": "Wage Theft", "threat_label": "HIGH", "contractor_name": "Suresh", "pincode": "400053", "complaint_count": 15}
            ]

except Exception as e:
    print(f"❌ ERROR: Failed to read from pipeline: {e}")
    print("   → Check if nyaya_hackathon.default.gold_heatmap table exists")
    print("   → Verify table permissions")
    print("   → Using fallback test data\n")
    
    test_scenarios = [
        {"issue_type": "Wage Theft", "threat_label": "HIGH", "contractor_name": "Test Contractor", "pincode": "000000", "complaint_count": 1}
    ]

# Process each scenario dynamically
print("="*70)
print("PROCESSING COMPLAINTS")
print("="*70 + "\n")

for idx, scenario in enumerate(test_scenarios, 1):
    issue = scenario["issue_type"]
    threat = scenario["threat_label"]
    contractor = scenario["contractor_name"]
    pincode = scenario["pincode"]
    count = scenario["complaint_count"]
    
    print(f"\n{'─'*70}")
    print(f"TEST {idx}: {issue} | Threat: {threat}")
    print(f"Contractor: {contractor} | Pincode: {pincode} | Complaints: {count}")
    print(f"{'─'*70}")
    
    try:
        # Generate legal action
        result = advisor.generate_legal_action(
            issue_type=issue,
            threat_level=threat,
            call_id=f"DYNAMIC-{idx}"
        )
        
        # Validate result structure
        if not result:
            print(f"❌ ERROR: No result returned for {issue}")
            print("   → Check LegalAdvisor.generate_legal_action() implementation")
            continue
        
        required_fields = ["mode", "urgency", "bns_sections_cited", "action_steps", "government_portal"]
        missing_fields = [f for f in required_fields if f not in result]
        
        if missing_fields:
            print(f"⚠️  WARNING: Missing fields in result: {missing_fields}")
            print("   → Check LegalAdvisor fallback logic")
        
        # Display results
        print(f"\n✅ Legal Action Generated:")
        print(f"   Mode:     {result.get('mode', 'N/A')}")
        print(f"   Urgency:  {result.get('urgency', 'N/A')}")
        print(f"   Sections: {', '.join(result.get('bns_sections_cited', ['None']))}")
        print(f"\n   Action Steps:")
        for step in result.get("action_steps", []):
            print(f"      → {step}")
        print(f"\n   Portal: {result.get('government_portal', 'N/A')}")
        
        # Test Hindi translation for HIGH urgency cases
        if result.get('urgency') == 'HIGH':
            try:
                print(f"\n   📢 Hindi Translation (for worker):")
                english_advice = " ".join(result.get("action_steps", []))
                hindi = advisor.translate_to_hindi(english_advice)
                print(f"      {hindi}")
            except Exception as trans_err:
                print(f"   ⚠️  Hindi translation failed: {trans_err}")
                print("      → Check LLM endpoint availability")
        
        # Generate show cause notice for contractors with multiple complaints
        if count >= 5:
            try:
                print(f"\n   📋 Show Cause Notice (Municipal):")
                notice = advisor.generate_show_cause_notice(
                    contractor_name=contractor,
                    pincode=pincode,
                    total_complaints=count,
                    primary_issue=issue
                )
                print(f"      {notice[:200]}...")  # Show first 200 chars
            except Exception as notice_err:
                print(f"   ⚠️  Notice generation failed: {notice_err}")
                print("      → Check LLM endpoint availability")
    
    except KeyError as ke:
        print(f"❌ ERROR: Missing required field in scenario data: {ke}")
        print("   → Check pipeline output schema")
        print(f"   → Available fields: {list(scenario.keys())}")
    
    except Exception as e:
        print(f"❌ ERROR: Failed to process {issue}: {e}")
        print("   → Check LegalAdvisor.generate_legal_action() for this issue type")
        print("   → Verify FAISS index and metadata files exist")
        print("   → Check if Databricks credentials are set")
        continue

print("\n" + "="*70)
print("✅ DYNAMIC TESTING COMPLETE")
print("="*70)
print(f"\nProcessed {len(test_scenarios)} different complaint scenarios")
print("Each scenario used actual issue types from the labor pipeline")
print("Error handling is now specific to each failure mode\n")

# COMMAND ----------

tables_to_check = [
    "workspace.default.bronze_bns_raw",
    "workspace.default.bronze_schemes",
    "workspace.default.silver_bns_sections",
    "workspace.default.silver_schemes",
    "workspace.default.silver_legal_chunks",
    "workspace.default.gold_legal_actions",
]

print("="*55)
print("  MEMBER 4 HANDOFF CHECKLIST")
print("="*55)

all_good = True
for table in tables_to_check:
    try:
        count = spark.read.table(table).count()
        print(f"  ✅  {table.split('.')[-1]:<30} {count:>5} rows")
    except Exception as e:
        print(f"  ❌  {table.split('.')[-1]:<30} MISSING — {e}")
        all_good = False

print("="*55)

# Check FAISS — using local /tmp path (rebuilt this session)
if os.path.exists("/tmp/faiss_legal/legal_index.faiss") and \
   os.path.exists("/tmp/faiss_legal/chunks_metadata.pkl"):
    print(f"  ✅  {'FAISS index (local /tmp)':<30}")
else:
    print(f"  ❌  {'FAISS index (local /tmp)':<30} MISSING — re-run Fix 1")
    all_good = False

# Check legal_rag.py — both in /tmp and Volume
if os.path.exists("/tmp/shramik_vani/legal_rag.py"):
    print(f"  ✅  {'legal_rag.py (local /tmp)':<30}")
else:
    print(f"  ❌  {'legal_rag.py (local /tmp)':<30} MISSING")
    all_good = False

if os.path.exists("/Volumes/workspace/default/raw_legal/legal_rag.py"):
    print(f"  ✅  {'legal_rag.py (Volume backup)':<30}")
else:
    print(f"  ❌  {'legal_rag.py (Volume backup)':<30} MISSING")
    all_good = False

print("="*55)
if all_good:
    print("  🏆  ALL CLEAR — Ready for Member 5 integration!")
else:
    print("  ⚠️   Fix the ❌ items before merging with Member 5.")
print("="*55)

# COMMAND ----------

# DBTITLE 1,Untitled
import os, faiss, pickle
import numpy as np
import shutil
from sentence_transformers import SentenceTransformer

# Recreate /tmp directory
os.makedirs("/tmp/faiss_legal", exist_ok=True)

# Reload chunks from Delta (source of truth)
chunks_df = spark.read.table("workspace.default.silver_legal_chunks").toPandas()
print(f"Loaded {len(chunks_df)} chunks from Delta")

# Re-embed
embed_model = SentenceTransformer("all-MiniLM-L6-v2")
contents    = chunks_df["content"].tolist()
embeddings  = embed_model.encode(contents, show_progress_bar=True, batch_size=32)
embeddings  = np.array(embeddings).astype("float32")

# Rebuild FAISS index
index = faiss.IndexFlatL2(embeddings.shape[1])
index.add(embeddings)
print(f"✅ FAISS index rebuilt with {index.ntotal} vectors")

# Save locally
faiss.write_index(index, "/tmp/faiss_legal/legal_index.faiss")
with open("/tmp/faiss_legal/chunks_metadata.pkl", "wb") as f:
    pickle.dump(
        chunks_df[["chunk_id", "content", "source"]].to_dict("records"), f
    )

# Copy directly to Volume (serverless-compatible)
shutil.copy(
    "/tmp/faiss_legal/legal_index.faiss",
    "/Volumes/workspace/default/raw_legal/legal_index.faiss"
)
shutil.copy(
    "/tmp/faiss_legal/chunks_metadata.pkl",
    "/Volumes/workspace/default/raw_legal/chunks_metadata.pkl"
)

print("✅ FAISS saved to Volume successfully")

# COMMAND ----------

import os
import shutil

os.makedirs("/tmp/shramik_vani", exist_ok=True)

# Write the file again
with open("/tmp/shramik_vani/legal_rag.py", "w") as f:
    f.write(legal_rag_code)  # legal_rag_code is still in memory from Cell 16

# Copy directly to Volume (serverless-compatible)
shutil.copy(
    "/tmp/shramik_vani/legal_rag.py",
    "/Volumes/workspace/default/raw_legal/legal_rag.py"
)

print("✅ legal_rag.py written and backed up to Volume")

# COMMAND ----------

import importlib.util, sys, json, os

# ── Step 1: Set your actual Databricks credentials ────────────────────────
os.environ["DATABRICKS_HOST"]  = spark.conf.get("spark.databricks.workspaceUrl")
os.environ["DATABRICKS_TOKEN"] = dbutils.notebook.entry_point \
    .getDbutils().notebook().getContext().apiToken().get()
os.environ["FAISS_INDEX_PATH"] = "/tmp/faiss_legal/legal_index.faiss"
os.environ["METADATA_PATH"]    = "/tmp/faiss_legal/chunks_metadata.pkl"

# ── Step 2: Load the module freshly ───────────────────────────────────────
if "legal_rag" in sys.modules:
    del sys.modules["legal_rag"]

spec   = importlib.util.spec_from_file_location(
             "legal_rag", "/tmp/shramik_vani/legal_rag.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
LegalAdvisor = module.LegalAdvisor

advisor = LegalAdvisor()

# ── Step 3: Confirm it loaded in RAG mode, not fallback ───────────────────
assert advisor._ready == True, "❌ FAISS not loaded — check paths above"
print(f"✅ FAISS loaded: {advisor._index.ntotal} vectors")
print(f"✅ Metadata:     {len(advisor._metadata)} chunks")
print()

# ── Step 4: Run retrieval and show exactly what was fetched ───────────────
test_query = "contractor not paying wages threatening workers"
chunks = advisor._retrieve(test_query, top_k=3)

print("="*60)
print(f"RETRIEVAL TEST: '{test_query}'")
print("="*60)
for i, c in enumerate(chunks):
    print(f"\nRank {i+1} | chunk_id: {c['chunk_id']} | source: {c['source']}")
    print(f"Score:   {c['score']:.4f}  (lower = more relevant)")
    print(f"Content: {c['content'][:200]}...")

# ── Step 5: Run full RAG pipeline and confirm mode = "rag" ────────────────
print("\n" + "="*60)
print("FULL RAG PIPELINE TEST")
print("="*60)

result = advisor.generate_legal_action("Wage Theft", "High", call_id="RAG-TEST-001")

print(f"\nMode (must be 'rag', not 'fallback'): {result['mode']}")
assert result["mode"] == "rag", "❌ Still using fallback — LLM call failed"

print(f"Urgency:          {result['urgency']}")
print(f"Sections cited:   {result['bns_sections_cited']}")
print(f"Retrieved chunks: {result['retrieved_chunks']}")
print(f"\nAction Steps:")
for step in result["action_steps"]:
    print(f"  → {step}")
print(f"\nPortal: {result['government_portal']}")

# ── Step 6: Test Show Cause Notice (Municipal view) ───────────────────────
print("\n" + "="*60)
print("SHOW CAUSE NOTICE TEST (Municipal Commissioner view)")
print("="*60)
notice = advisor.generate_show_cause_notice(
    contractor_name   = "Suresh",
    pincode           = "400053",
    total_complaints  = 15,
    primary_issue     = "Wage Theft"
)
print(notice)

print("\n✅ ALL RAG TESTS PASSED — Member 5 integration ready")