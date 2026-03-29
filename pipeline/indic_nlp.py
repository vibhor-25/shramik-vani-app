# indic_nlp.py
# Member 2: Indic NLP Pipeline
# Uses Sarvam API only — no local models, no HuggingFace

import requests
import json
import re
import time

SARVAM_MODEL = "sarvam-m"

HEADERS = {
    "api-subscription-key": "sk_8j6pgvef_BSbqmS3VlMhGnXh9OWoF2CsT",
    "Content-Type": "application/json"
}

# ── Translation ───────────────────────────────────────────────────────────────

def _translate(hindi_text: str) -> str:
    try:
        resp = requests.post(
            "https://api.sarvam.ai/translate",
            headers=HEADERS,
            json={
                "input": hindi_text,
                "source_language_code": "auto",
                "target_language_code": "en-IN",
                "model": "mayura:v1",
                "mode": "formal"
            },
            timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("translated_text", hindi_text)
    except Exception as e:
        print(f"Translation error: {e}")
        return hindi_text

# ── JSON Parser ───────────────────────────────────────────────────────────────

def _parse_json(raw: str) -> dict:
    if "</think>" in raw:
        raw = raw.split("</think>")[-1].strip()
    cleaned = re.sub(r"```(?:json)?|```", "", raw).strip()
    start = cleaned.find("{")
    end   = cleaned.rfind("}") + 1
    if start == -1 or end == 0:
        print(f"No JSON found: {raw[:100]}")
        return _default_entities()
    try:
        return json.loads(cleaned[start:end])
    except json.JSONDecodeError:
        fixed = cleaned[start:end].replace("True","true").replace("False","false").replace("None","null")
        try:
            return json.loads(fixed)
        except Exception:
            return _default_entities()

def _default_entities() -> dict:
    return {
        "contractor_name":        "UNKNOWN",
        "work_site":              "UNKNOWN",
        "issue_type":             "OTHER",
        "violence_threatened":    False,
        "worker_count_mentioned": 0
    }

# ── Entity Extraction ─────────────────────────────────────────────────────────

def _extract(english_text: str) -> dict:
    prompt = f"""Extract entities from this labor complaint and return ONLY a JSON object.

Complaint: {english_text}

Return exactly this structure:
{{"contractor_name":"name mentioned or UNKNOWN","work_site":"location mentioned or UNKNOWN","issue_type":"one of WAGE_THEFT/PHYSICAL_ASSAULT/WRONGFUL_TERMINATION/UNSAFE_CONDITIONS/OTHER","violence_threatened":true or false,"worker_count_mentioned":number or 0}}"""

    try:
        resp = requests.post(
            "https://api.sarvam.ai/v1/chat/completions",
            headers=HEADERS,
            json={
                "model": SARVAM_MODEL,
                "messages": [
                    {"role": "system", "content": "You are a labor complaint analyst. You only respond with valid JSON. Never add explanation or markdown."},
                    {"role": "user",   "content": prompt}
                ],
                "temperature": 0.1,
                "max_tokens": 1500
            },
            timeout=30
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
        return _parse_json(raw)
    except Exception as e:
        print(f"Extraction error: {e}")
        return _default_entities()

# ── Public API ────────────────────────────────────────────────────────────────

def process_hindi_audio_text(hindi_text: str) -> dict:
    """
    Process Hindi/Hinglish/English labor complaint text.
    
    Args:
        hindi_text: Raw complaint text in any language
        
    Returns:
        dict with keys:
            - english_text: Translated English text
            - contractor_name: Extracted contractor name or UNKNOWN
            - work_site: Extracted work site or UNKNOWN
            - issue_type: One of WAGE_THEFT/PHYSICAL_ASSAULT/WRONGFUL_TERMINATION/UNSAFE_CONDITIONS/OTHER
            - violence_threatened: Boolean
            - worker_count_mentioned: Integer count
    """
    english_text = _translate(hindi_text.strip())
    time.sleep(0.3)
    entities     = _extract(english_text)
    return {
        "english_text":            english_text,
        "contractor_name":         str(entities.get("contractor_name",        "UNKNOWN")),
        "work_site":               str(entities.get("work_site",              "UNKNOWN")),
        "issue_type":              str(entities.get("issue_type",             "OTHER")),
        "violence_threatened":     bool(entities.get("violence_threatened",   False)),
        "worker_count_mentioned":  int(entities.get("worker_count_mentioned", 0)),
    }

# ── Module Test ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("[indic_nlp] Testing module...\n")
    
    print("[indic_nlp] Module ready. Using Sarvam API.")
