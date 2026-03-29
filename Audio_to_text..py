# Databricks notebook source
import requests

SARVAM_API_KEY = "sk_8j6pgvef_BSbqmS3VlMhGnXh9OWoF2CsT"

HEADERS = {
    "api-subscription-key": SARVAM_API_KEY
    # NOTE: no Content-Type here — requests sets it automatically for multipart
}

# Test with any audio file you have — even a voice memo in Hindi works
audio_file_path = r"/Workspace/Users/buddydarvince1@gmail.com/audd/WhatsApp Audio 2026-03-29 at 09.47.00.mp4"  # Updated path to your uploaded file

with open(audio_file_path, "rb") as f:
    resp = requests.post(
        "https://api.sarvam.ai/speech-to-text",
        headers=HEADERS,
        files={"file": ("audio.wav", f, "audio/wav")},
        data={
            "model": "saaras:v3",
            "mode": "translate"   # directly gives English output — no separate translate call needed
        }
    )

print("STATUS:", resp.status_code)
print("RESPONSE:", resp.json())

# COMMAND ----------

import requests, json, re, time, os, warnings
warnings.filterwarnings("ignore")


STT_HEADERS = {
    "api-subscription-key": SARVAM_API_KEY
}

JSON_HEADERS = {
    "api-subscription-key": SARVAM_API_KEY,
    "Content-Type": "application/json"
}

# ─────────────────────────────────────────────────────────────
# STEP 1: AUDIO FILE → ENGLISH TEXT
# ─────────────────────────────────────────────────────────────

def transcribe_audio(audio_file_path: str) -> str:
    """
    Takes any audio file path.
    Sends to Sarvam STT.
    Prints what it heard.
    Returns English text.
    """
    print("=" * 50)
    print(f"AUDIO FILE  : {audio_file_path}")

    if not os.path.exists(audio_file_path):
        print(f"ERROR: File not found at {audio_file_path}")
        return ""

    file_ext = audio_file_path.split(".")[-1].lower()
    mime_map = {
        "wav":  "audio/wav",
        "mp3":  "audio/mpeg",
        "ogg":  "audio/ogg",
        "opus": "audio/opus",
        "m4a":  "audio/mp4",
        "flac": "audio/flac",
        "aac":  "audio/aac",
        "webm": "audio/webm",
        "amr":  "audio/amr"
    }
    mime = mime_map.get(file_ext, "audio/wav")
    print(f"FILE TYPE   : {file_ext} ({mime})")

    try:
        print("SENDING TO  : Sarvam saaras:v3 STT...")
        with open(audio_file_path, "rb") as f:
            resp = requests.post(
                "https://api.sarvam.ai/speech-to-text",
                headers=STT_HEADERS,
                files={"file": (os.path.basename(audio_file_path), f, mime)},
                data={
                    "model": "saaras:v3",
                    "mode": "translate"   # Hindi/Marathi speech → English text directly
                },
                timeout=30
            )
        resp.raise_for_status()
        english_text = resp.json().get("transcript", "")
        print(f"TRANSCRIBED : {english_text}")
        print("=" * 50)
        return english_text

    except Exception as e:
        print(f"STT ERROR   : {e}")
        print("=" * 50)
        return ""


# ─────────────────────────────────────────────────────────────
# STEP 2: ENGLISH TEXT → STRUCTURED ENTITIES
# ─────────────────────────────────────────────────────────────

def extract_entities(english_text: str) -> dict:
    prompt = f"""Read this labor complaint and return ONLY a JSON object, nothing else.

Complaint: {english_text}

Return this exact JSON:
{{"contractor_name":"name or UNKNOWN","work_site":"location or UNKNOWN","issue_type":"WAGE_THEFT or PHYSICAL_ASSAULT or WRONGFUL_TERMINATION or UNSAFE_CONDITIONS or OTHER","violence_threatened":true or false,"worker_count_mentioned":0}}"""

    resp = requests.post(
        "https://api.sarvam.ai/v1/chat/completions",
        headers=JSON_HEADERS,
        json={
            "model": "sarvam-m",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 1500
        },
        timeout=30
    )
    raw = resp.json()["choices"][0]["message"]["content"]
    if "</think>" in raw:
        raw = raw.split("</think>")[-1].strip()
    raw = re.sub(r"```(?:json)?|```", "", raw).strip()
    start = raw.find("{")
    end   = raw.rfind("}") + 1
    if start == -1:
        return {}
    try:
        return json.loads(raw[start:end])
    except:
        return {}


# ─────────────────────────────────────────────────────────────
# MASTER FUNCTION 1: AUDIO → FINAL RESULT
# You call this. It prints everything. Returns structured dict.
# ─────────────────────────────────────────────────────────────

def process_audio_file(audio_file_path: str) -> dict:
    """
    YOU use this for real audio files.
    Audio → STT → entity extraction → structured dict.
    Prints every step clearly.
    """
    # Step 1: audio → english
    english = transcribe_audio(audio_file_path)
    if not english:
        print("Nothing transcribed. Check your audio file.")
        return {}

    time.sleep(0.5)

    # Step 2: english → entities
    print("EXTRACTING ENTITIES...")
    e = extract_entities(english)

    result = {
        "english_text":           english,
        "contractor_name":        str(e.get("contractor_name", "UNKNOWN")),
        "work_site":              str(e.get("work_site",        "UNKNOWN")),
        "issue_type":             str(e.get("issue_type",       "OTHER")),
        "violence_threatened":    bool(e.get("violence_threatened",    False)),
        "worker_count_mentioned": int(e.get("worker_count_mentioned",  0)),
    }

    print(f"CONTRACTOR  : {result['contractor_name']}")
    print(f"WORK SITE   : {result['work_site']}")
    print(f"ISSUE       : {result['issue_type']}")
    print(f"VIOLENCE    : {result['violence_threatened']}")
    print(f"WORKERS     : {result['worker_count_mentioned']}")
    print("DONE.")
    return result


# ─────────────────────────────────────────────────────────────
# MASTER FUNCTION 2: HINDI TEXT → FINAL RESULT
# Member 1 imports and calls this as a PySpark UDF
# on every row of the Bronze Delta table
# ─────────────────────────────────────────────────────────────

def process_hindi_audio_text(hindi_text: str) -> dict:
    """
    MEMBER 1 uses this.
    Hindi text from mock data → translate → extract → structured dict.
    """
    # Translate
    resp = requests.post(
        "https://api.sarvam.ai/translate",
        headers=JSON_HEADERS,
        json={
            "input": hindi_text,
            "source_language_code": "auto",
            "target_language_code": "en-IN",
            "model": "mayura:v1",
            "mode": "formal"
        },
        timeout=15
    )
    english = resp.json().get("translated_text", hindi_text)
    time.sleep(0.5)

    e = extract_entities(english)
    return {
        "english_text":           english,
        "contractor_name":        str(e.get("contractor_name", "UNKNOWN")),
        "work_site":              str(e.get("work_site",        "UNKNOWN")),
        "issue_type":             str(e.get("issue_type",       "OTHER")),
        "violence_threatened":    bool(e.get("violence_threatened",    False)),
        "worker_count_mentioned": int(e.get("worker_count_mentioned",  0)),
    }

print("Loaded. Two functions available:")
print("  process_audio_file(path)        — you use this for real audio")
print("  process_hindi_audio_text(text)  — Member 1 uses this for mock data")

# COMMAND ----------

test_inputs = [
    "सुरेश ने 15 मजदूरों को एक हफ्ते से पैसे नहीं दिए और धमकी दी।",
    "Contractor Ramesh fired 8 workers without notice at Andheri site.",
    "मेट्रो पिलर 44 पर काम करने वाले मजदूरों को सुरक्षा उपकरण नहीं दिए गए।",
    "Supervisor ne kal Thane bridge site par ek mazdoor ko maara.",
]

for t in test_inputs:
    r = process_hindi_audio_text(t)
    print("INPUT   :", t[:60])
    print("ENGLISH :", r["english_text"])
    print("RESULT  :", r["contractor_name"], "|", r["issue_type"], "| violence =", r["violence_threatened"])
    print()

# COMMAND ----------

# Using the process_audio_file function with an existing audio file
# For example, let's use the file that was just processed:
aud_file_path = "/Workspace/Users/buddydarvince1@gmail.com/audd/WhatsApp Audio 2026-03-29 at 09.47.00.mp4"

# Call the function
result = process_audio_file(aud_file_path)

# Print the English transcription
print("\nEnglish Transcription:", result["english_text"])
print("Extracted Entities:", result)

# COMMAND ----------

# DBTITLE 1,CREATE JSON FILES for Data_Extraction
import glob
import random
from datetime import datetime

print("="*70)
print("CREATING JSON FILES FOR DATA_EXTRACTION PIPELINE")
print("="*70)

# ============================================
# STEP 1: Setup paths
# ============================================
AUDIO_FOLDER = "/Workspace/Users/buddydarvince1@gmail.com/audd"
OUTPUT_PATH = "/Volumes/nyaya_hackathon/shramik_grid/incoming_calls"

print(f"\nAudio folder: {AUDIO_FOLDER}")
print(f"Output path: {OUTPUT_PATH}\n")

# ============================================
# STEP 2: Find all audio files
# ============================================
audio_files = []
for ext in ['*.mp3', '*.mp4', '*.wav', '*.m4a', '*.ogg']:
    audio_files.extend(glob.glob(f"{AUDIO_FOLDER}/{ext}"))

if not audio_files:
    print(f"❌ NO AUDIO FILES FOUND in {AUDIO_FOLDER}")
    print("\nAdd audio files there and re-run this cell!\n")
else:
    print(f"✓ Found {len(audio_files)} audio file(s)\n")
    
    # ============================================
    # STEP 3: Process each audio file
    # ============================================
    call_id = 1
    for audio_path in audio_files:
        try:
            print(f"Processing {call_id}: {os.path.basename(audio_path)}")
            
            # Transcribe audio to English text (NO entity extraction)
            english_text = transcribe_audio(audio_path)
            
            if not english_text:
                print(f"  ⚠️ Skipped - no transcription\n")
                continue
            
            # Create JSON record - JUST THE TEXT!
            # Entity extraction will be done by Labor Pipeline
            call_record = {
                "call_id": call_id,
                "pincode": "000000",  # Generic - will be extracted by Labor Pipeline
                "hindi_audio_text": english_text,  # Just the transcribed text
                "timestamp": datetime.now().isoformat(),
                "call_duration_seconds": random.randint(30, 180),
                "audio_source": os.path.basename(audio_path)
            }
            
            # Save JSON file
            filename = f"{OUTPUT_PATH}/call_{call_id:03d}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(call_record, f, ensure_ascii=False, indent=2)
            
            print(f"  ✓ Saved: {filename}")
            print(f"  ✓ Text: \"{english_text[:60]}...\"\n")
            
            call_id += 1
            time.sleep(1)  # API rate limit
            
        except Exception as e:
            print(f"  ✗ Error: {e}\n")
            continue
    
    print(f"\n{'='*70}")
    print(f"✅ CREATED {call_id - 1} JSON FILES")
    print(f"{'='*70}")
    print(f"\nJSON files saved to: {OUTPUT_PATH}")
    print("\n📋 Next Steps:")
    print("  1. ✓ Audio_to_text (THIS notebook) - COMPLETE")
    print("  2. → Run Data_Extraction notebook to ingest JSON files")
    print("  3. → Run Labor Complaint Pipeline for entity extraction")
    print("  4. → Run Legal RAG Pipeline for legal actions\n")