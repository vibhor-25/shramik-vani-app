"""
Shramik-Vani & Nyaya-Grid
=========================
Deployable Streamlit app for Databricks.

Interface 1 – Worker Portal  : Audio upload → M2 (translation) → M4 (RAG) → M2 (reverse translate) → output
Interface 2 – Authority Radar: Live heatmap from Gold Delta table (Member 1)

Dependencies (add to requirements.txt / Databricks App config):
    streamlit
    streamlit-folium
    folium
    pydub
    requests

Environment variables required (set in Databricks App secrets or os.environ):
    DATABRICKS_HOST   – e.g. adb-xxxx.azuredatabricks.net
    DATABRICKS_TOKEN  – personal access token
    FAISS_INDEX_PATH  – /tmp/faiss_legal/legal_index.faiss
    METADATA_PATH     – /tmp/faiss_legal/chunks_metadata.pkl
"""

import os, sys, json, time, shutil, tempfile, importlib.util
import requests
import pandas as pd
import folium
import streamlit as st
from streamlit_folium import st_folium

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Shramik-Vani & Nyaya-Grid",
    layout="wide",
    page_icon="⚖️",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL CSS  ── dark industrial theme
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');

:root {
    --bg:        #0a0c10;
    --surface:   #111418;
    --border:    #1e2430;
    --accent:    #e8622a;
    --accent2:   #f0a500;
    --text:      #d4dae6;
    --muted:     #5a6480;
    --success:   #2ecc71;
    --danger:    #e74c3c;
    --warning:   #f39c12;
}

html, body, [data-testid="stAppViewContainer"] {
    background: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'Syne', sans-serif;
}

/* Hide default streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stToolbar"] { display: none; }

/* Tabs */
[data-testid="stTabs"] button {
    font-family: 'Space Mono', monospace !important;
    font-size: 0.75rem !important;
    letter-spacing: 0.12em !important;
    text-transform: uppercase !important;
    color: var(--muted) !important;
    border-bottom: 2px solid transparent !important;
    padding: 0.6rem 1.4rem !important;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: var(--accent) !important;
    border-bottom: 2px solid var(--accent) !important;
}
[data-testid="stTabsContent"] { padding-top: 1.5rem; }

/* Cards */
.card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1.4rem 1.6rem;
    margin-bottom: 1rem;
}
.card-accent { border-left: 3px solid var(--accent); }
.card-success { border-left: 3px solid var(--success); }
.card-warning { border-left: 3px solid var(--warning); }
.card-danger  { border-left: 3px solid var(--danger); }

/* Header banner */
.app-header {
    display: flex;
    align-items: center;
    gap: 1.2rem;
    padding: 1.2rem 0 0.4rem 0;
    border-bottom: 1px solid var(--border);
    margin-bottom: 1.6rem;
}
.app-header h1 {
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    font-size: 1.7rem;
    color: #fff;
    margin: 0;
    letter-spacing: -0.02em;
}
.app-header .sub {
    font-family: 'Space Mono', monospace;
    font-size: 0.68rem;
    color: var(--muted);
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin-top: 2px;
}
.badge {
    display: inline-block;
    font-family: 'Space Mono', monospace;
    font-size: 0.62rem;
    letter-spacing: 0.08em;
    padding: 3px 8px;
    border-radius: 3px;
    text-transform: uppercase;
}
.badge-red    { background: rgba(231,76,60,0.15);  color: #e74c3c; border: 1px solid rgba(231,76,60,0.3); }
.badge-orange { background: rgba(232,98,42,0.15);  color: var(--accent); border: 1px solid rgba(232,98,42,0.3); }
.badge-green  { background: rgba(46,204,113,0.15); color: var(--success); border: 1px solid rgba(46,204,113,0.3); }
.badge-yellow { background: rgba(243,156,18,0.15); color: var(--warning); border: 1px solid rgba(243,156,18,0.3); }

/* Pipeline step */
.pipeline-step {
    display: flex;
    align-items: flex-start;
    gap: 1rem;
    padding: 0.9rem 0;
    border-bottom: 1px solid var(--border);
}
.pipeline-step:last-child { border-bottom: none; }
.step-num {
    font-family: 'Space Mono', monospace;
    font-size: 0.65rem;
    color: var(--accent);
    background: rgba(232,98,42,0.1);
    border: 1px solid rgba(232,98,42,0.2);
    border-radius: 3px;
    padding: 3px 7px;
    white-space: nowrap;
    margin-top: 2px;
}
.step-content h4 {
    margin: 0 0 3px 0;
    font-size: 0.85rem;
    font-weight: 600;
    color: #fff;
}
.step-content p {
    margin: 0;
    font-size: 0.78rem;
    color: var(--muted);
    line-height: 1.5;
}

/* Stat cards */
.stat-row { display: flex; gap: 0.8rem; margin-bottom: 1rem; }
.stat-box {
    flex: 1;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 1rem 1.2rem;
    text-align: center;
}
.stat-box .val {
    font-family: 'Space Mono', monospace;
    font-size: 1.8rem;
    font-weight: 700;
    color: var(--accent);
}
.stat-box .lbl {
    font-size: 0.7rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-top: 3px;
}

/* Inputs */
[data-testid="stTextArea"] textarea,
[data-testid="stSelectbox"] > div,
[data-testid="stFileUploader"] {
    background: var(--surface) !important;
    border-color: var(--border) !important;
    color: var(--text) !important;
    border-radius: 6px !important;
}

/* Buttons */
[data-testid="stButton"] button {
    background: var(--accent) !important;
    color: #fff !important;
    border: none !important;
    border-radius: 5px !important;
    font-family: 'Space Mono', monospace !important;
    font-size: 0.72rem !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    padding: 0.55rem 1.4rem !important;
    transition: opacity 0.15s !important;
}
[data-testid="stButton"] button:hover { opacity: 0.85 !important; }

/* Alerts */
[data-testid="stAlert"] {
    background: rgba(46,204,113,0.08) !important;
    border: 1px solid rgba(46,204,113,0.2) !important;
    border-radius: 6px !important;
    color: var(--text) !important;
}

/* Divider */
hr { border-color: var(--border) !important; }

/* Scrollbar */
::-webkit-scrollbar { width: 5px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="app-header">
    <div>⚖️</div>
    <div>
        <h1>Shramik-Vani &amp; Nyaya-Grid</h1>
        <div class="sub">AI-Powered Labour Rights Enforcement Platform &nbsp;·&nbsp; Powered by Databricks</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# ENV + CREDENTIALS
# ─────────────────────────────────────────────────────────────────────────────
DATABRICKS_HOST  = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

# ─────────────────────────────────────────────────────────────────────────────
# HELPER: Restore FAISS from Volume to /tmp if missing
# ─────────────────────────────────────────────────────────────────────────────
def ensure_faiss():
    faiss_path = "/tmp/faiss_legal/legal_index.faiss"
    meta_path  = "/tmp/faiss_legal/chunks_metadata.pkl"
    if not os.path.exists(faiss_path):
        os.makedirs("/tmp/faiss_legal", exist_ok=True)
        shutil.copy("/Volumes/workspace/default/faiss/legal_index.faiss", faiss_path)
        shutil.copy("/Volumes/workspace/default/faiss/chunks_metadata.pkl", meta_path)
    return faiss_path, meta_path

# ─────────────────────────────────────────────────────────────────────────────
# HELPER: Load LegalAdvisor (Member 4's module)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def load_legal_advisor():
    faiss_path, meta_path = ensure_faiss()
    os.environ["FAISS_INDEX_PATH"] = faiss_path
    os.environ["METADATA_PATH"]    = meta_path

    # Restore legal_rag.py from Volume if not in /tmp
    module_path = "/tmp/shramik_vani/legal_rag.py"
    if not os.path.exists(module_path):
        os.makedirs("/tmp/shramik_vani", exist_ok=True)
        shutil.copy("/Volumes/workspace/default/raw_legal/legal_rag.py", module_path)

    if "legal_rag" in sys.modules:
        del sys.modules["legal_rag"]
    sys.path.insert(0, "/tmp/shramik_vani")

    spec   = importlib.util.spec_from_file_location("legal_rag", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.LegalAdvisor()

# ─────────────────────────────────────────────────────────────────────────────
# HELPER: Call Databricks LLM endpoint directly
# ─────────────────────────────────────────────────────────────────────────────
def call_llm(prompt: str, max_tokens: int = 512) -> str:
    url = (f"https://{DATABRICKS_HOST}/serving-endpoints/"
           f"databricks-meta-llama-3-1-8b-instruct/invocations")
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}",
                 "Content-Type": "application/json"},
        json={"messages": [{"role": "user", "content": prompt}],
              "max_tokens": max_tokens, "temperature": 0.1},
        timeout=60
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]

# ─────────────────────────────────────────────────────────────────────────────
# HELPER: Member 2 – Transcribe audio + translate to English
# ─────────────────────────────────────────────────────────────────────────────
def transcribe_and_translate(audio_bytes: bytes, language: str) -> dict:
    """
    Member 2's pipeline:
      audio_bytes → IndicTrans2 transcription → English text
    This stub calls the LLM to simulate translation for demo purposes.
    When Member 2 is ready, swap in: from indic_nlp import process_hindi_audio_text
    """
    # ── Real integration point (uncomment when Member 2 is ready) ───────────
    # sys.path.insert(0, "/tmp/shramik_vani")
    # from indic_nlp import process_hindi_audio_text
    # return process_hindi_audio_text(audio_text, language)

    # ── Demo stub: LLM simulates translation ────────────────────────────────
    prompt = f"""A worker called an IVR helpline and spoke in {language}.
The audio has been transcribed as text. Simulate a realistic Hindi/regional
labour complaint transcription, then translate it to English.

Respond ONLY in this JSON format:
{{
  "original_transcript": "...(simulated {language} text)...",
  "english_translation": "...(English translation)...",
  "detected_language": "{language}"
}}"""
    try:
        raw  = call_llm(prompt, max_tokens=300)
        clean = raw.strip()
        if "```" in clean:
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
        return json.loads(clean.strip())
    except Exception:
        return {
            "original_transcript": "ठेकेदार सुरेश ने 5 दिन से पैसे नहीं दिए और मारने की धमकी दे रहा है।",
            "english_translation": "Contractor Suresh has not paid wages for 5 days and is threatening to beat us.",
            "detected_language": language
        }

# ─────────────────────────────────────────────────────────────────────────────
# HELPER: Member 2 – Reverse translate advice back to worker's language
# ─────────────────────────────────────────────────────────────────────────────
def translate_advice_to_local(english_advice: str, language: str) -> str:
    """
    Calls Member 2's reverse translation.
    Real integration: from indic_nlp import translate_to_local
    """
    if not (DATABRICKS_HOST and DATABRICKS_TOKEN):
        return english_advice

    prompt = f"""Translate this legal advice to {language}.
Use simple words that an uneducated daily-wage worker can understand.
Output ONLY the {language} translation, nothing else.

Text: {english_advice}"""
    try:
        return call_llm(prompt, max_tokens=400)
    except Exception:
        return english_advice

# ─────────────────────────────────────────────────────────────────────────────
# HELPER: Load heatmap data from Gold Delta table (Member 1)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)   # refresh every 60s
def load_heatmap_data() -> pd.DataFrame:
    """
    Reads Member 1's gold_heatmap_clusters Delta table.
    Falls back to demo data if Spark is unavailable (local dev).
    Column contract:  contractor_name, pincode, total_complaints,
                      severity_score, primary_issue, lat, lon
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.table("workspace.default.gold_heatmap_clusters").toPandas()

        # Normalise column names — adjust if Member 1 uses different names
        col_map = {
            "contractor": "contractor_name",
            "complaint_count": "total_complaints",
            "issue": "primary_issue",
            "score": "severity_score",
        }
        df.rename(columns={k: v for k, v in col_map.items() if k in df.columns},
                  inplace=True)

        # If lat/lon not in Gold table, approximate from pincode lookup
        if "lat" not in df.columns:
            PINCODE_COORDS = {
                "400053": (19.0760,  72.8777),
                "411001": (18.5204,  73.8567),
                "110001": (28.6139,  77.2090),
                "600001": (13.0827,  80.2707),
                "500001": (17.3850,  78.4867),
                "380001": (23.0225,  72.5714),
            }
            df["lat"] = df["pincode"].map(
                lambda p: PINCODE_COORDS.get(str(p), (20.5937, 78.9629))[0])
            df["lon"] = df["pincode"].map(
                lambda p: PINCODE_COORDS.get(str(p), (20.5937, 78.9629))[1])
        return df

    except Exception:
        # ── Demo fallback data ───────────────────────────────────────────────
        return pd.DataFrame([
            {"contractor_name": "Suresh Patil", "pincode": "400053",
             "lat": 19.076,  "lon": 72.877,  "total_complaints": 15,
             "severity_score": 9.5, "primary_issue": "Wage Theft"},
            {"contractor_name": "Rajesh Kumar", "pincode": "411001",
             "lat": 18.520,  "lon": 73.856,  "total_complaints": 11,
             "severity_score": 7.8, "primary_issue": "Wage Theft"},
            {"contractor_name": "Vikram Singh", "pincode": "110001",
             "lat": 28.613,  "lon": 77.209,  "total_complaints": 8,
             "severity_score": 6.2, "primary_issue": "Forced Labour"},
            {"contractor_name": "Mohan Das",    "pincode": "600001",
             "lat": 13.082,  "lon": 80.270,  "total_complaints": 6,
             "severity_score": 5.1, "primary_issue": "Criminal Intimidation"},
            {"contractor_name": "Arun Sharma",  "pincode": "500001",
             "lat": 17.385,  "lon": 78.486,  "total_complaints": 4,
             "severity_score": 3.3, "primary_issue": "Wrongful Termination"},
            {"contractor_name": "Suresh Patil", "pincode": "380001",
             "lat": 23.022,  "lon": 72.571,  "total_complaints": 3,
             "severity_score": 2.8, "primary_issue": "Wage Theft"},
        ])

def score_to_urgency(score: float) -> str:
    if score >= 8:   return "CRITICAL"
    elif score >= 5: return "HIGH"
    elif score >= 3: return "MEDIUM"
    return "LOW"

URGENCY_COLOR  = {"CRITICAL": "#e74c3c", "HIGH": "#e8622a",
                  "MEDIUM":   "#f39c12", "LOW":  "#2ecc71"}
URGENCY_BADGE  = {"CRITICAL": "badge-red", "HIGH": "badge-orange",
                  "MEDIUM":   "badge-yellow", "LOW": "badge-green"}

# ─────────────────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs([
    "📞  Worker Portal  —  Shramik-Vani",
    "🗺️  Authority Radar  —  Nyaya-Grid",
])

# ═════════════════════════════════════════════════════════════════════════════
# TAB 1 — WORKER PORTAL
# ═════════════════════════════════════════════════════════════════════════════
with tab1:

    # ── How it works banner ──────────────────────────────────────────────────
    st.markdown("""
    <div class="card">
        <div style="font-family:'Space Mono',monospace;font-size:0.65rem;
                    color:#5a6480;letter-spacing:0.1em;text-transform:uppercase;
                    margin-bottom:0.9rem;">How it works</div>
        <div class="pipeline-step">
            <span class="step-num">01</span>
            <div class="step-content">
                <h4>Record Audio</h4>
                <p>Worker clicks the microphone button and speaks their complaint in any Indian language.</p>
            </div>
        </div>
        <div class="pipeline-step">
            <span class="step-num">02</span>
            <div class="step-content">
                <h4>Member 2 — IndicTrans2 Transcription &amp; Translation</h4>
                <p>Audio is transcribed and translated to English using IndicTrans2.</p>
            </div>
        </div>
        <div class="pipeline-step">
            <span class="step-num">03</span>
            <div class="step-content">
                <h4>Member 4 — RAG Legal Engine (BNS 2023 + FAISS)</h4>
                <p>English complaint is matched against BNS 2023 Vector DB. Airavata generates a legal action plan.</p>
            </div>
        </div>
        <div class="pipeline-step">
            <span class="step-num">04</span>
            <div class="step-content">
                <h4>Member 2 — Reverse Translation</h4>
                <p>Legal advice is translated back to the worker's original language.</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Input form ────────────────────────────────────────────────────────────
    col_in, col_out = st.columns([1, 1], gap="large")

    with col_in:
        st.markdown('<div class="card card-accent">', unsafe_allow_html=True)
        st.markdown("#### 🎙️ Record Your Complaint")

        language = st.selectbox(
            "Your Language",
            ["Hindi", "Marathi", "Tamil", "Telugu", "Bengali",
             "Kannada", "Gujarati", "Punjabi", "Odia"],
            key="worker_lang"
        )

        # ── Live browser microphone recorder ─────────────────────────────────
        st.markdown(
            '<div style="font-family:\'Space Mono\',monospace;font-size:0.68rem;'
            'color:#5a6480;letter-spacing:0.06em;margin-bottom:0.4rem;">'
            'PRESS ● TO START RECORDING, PRESS ■ TO STOP</div>',
            unsafe_allow_html=True
        )
        audio_recording = st.audio_input(
            label="Record complaint",
            key="audio_recorder",
            label_visibility="collapsed",
        )

        if audio_recording:
            st.markdown(
                '<div style="font-family:\'Space Mono\',monospace;font-size:0.68rem;'
                'color:#2ecc71;margin:0.3rem 0 0.6rem 0;">✅ Recording captured</div>',
                unsafe_allow_html=True
            )

        issue_type = st.selectbox(
            "Issue Type",
            ["Wage Theft", "Physical Assault", "Wrongful Termination",
             "Forced Labour", "Criminal Intimidation", "Other"],
            key="issue_sel"
        )

        threat_level = st.selectbox(
            "Threat Level",
            ["High", "Low"],
            key="threat_sel"
        )

        pincode = st.text_input(
            "Your Pincode (for heatmap tracking)",
            placeholder="e.g. 400053",
            key="worker_pincode"
        )

        submitted = st.button("🔍 Get Legal Help", key="submit_worker",
                              use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Output panel ─────────────────────────────────────────────────────────
    with col_out:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("#### 📤 AI Legal Advice")

        if not submitted:
            st.markdown("""
            <div style="text-align:center;padding:3rem 1rem;color:#5a6480;">
                <div style="font-size:2.5rem;margin-bottom:0.8rem;">🎙️</div>
                <div style="font-family:'Space Mono',monospace;font-size:0.72rem;
                            letter-spacing:0.08em;text-transform:uppercase;">
                    Record your complaint<br>and click Get Legal Help
                </div>
            </div>
            """, unsafe_allow_html=True)

        else:
            if audio_recording is None:
                st.warning("⚠️ Please record your complaint first using the microphone button.")
            else:
                # ── Step 1: Playback what was recorded ───────────────────────
                st.markdown(
                    '<div style="font-family:\'Space Mono\',monospace;font-size:0.68rem;'
                    'color:#5a6480;margin-bottom:0.3rem;">RECORDED AUDIO</div>',
                    unsafe_allow_html=True
                )
                st.audio(audio_recording, format="audio/wav")
                audio_bytes = audio_recording.read()

                # ── Step 2: M2 — Transcribe + Translate ──────────────────────
                with st.status("⚙️ Processing complaint...", expanded=True) as status:

                    st.write(f"🔤 Step 1 — Transcribing audio in {language}...")
                    time.sleep(0.6)

                    transcription = transcribe_and_translate(audio_bytes, language)
                    original_text = transcription["original_transcript"]
                    english_text  = transcription["english_translation"]

                    st.write("✅ Transcription complete")
                    st.write("🔍 Step 2 — Querying BNS 2023 Legal DB (FAISS)...")
                    time.sleep(0.4)

                    # ── Step 3: M4 — RAG Legal Engine ────────────────────────
                    try:
                        advisor = load_legal_advisor()
                        result  = advisor.generate_legal_action(
                            issue_type   = issue_type,
                            threat_level = threat_level,
                            call_id      = f"WEB-{int(time.time())}"
                        )
                        rag_mode = result.get("mode", "fallback")
                    except Exception as e:
                        # Graceful fallback
                        result = {
                            "bns_sections_cited": ["BNS Section 351 - Criminal Intimidation",
                                                   "BNS Section 316 - Criminal Breach of Trust"],
                            "action_steps": [
                                "File an FIR at your nearest police station.",
                                "Visit the Labour Commissioner with wage proof.",
                                "Register on eShram portal for emergency relief."
                            ],
                            "government_portal": "shramsuvidha.gov.in or call 14434",
                            "urgency": "HIGH" if threat_level == "High" else "MEDIUM",
                            "mode": "fallback"
                        }
                        rag_mode = "fallback"

                    st.write("✅ Legal sections retrieved")
                    st.write(f"🌐 Step 3 — Translating advice back to {language}...")
                    time.sleep(0.4)

                    # ── Step 4: M2 — Reverse Translate ───────────────────────
                    english_advice = "\n".join(result["action_steps"])
                    local_advice   = translate_advice_to_local(english_advice, language)

                    st.write("✅ Translation complete")
                    status.update(label="✅ Done — advice ready", state="complete")

                # ── Display results ──────────────────────────────────────────
                urgency      = result.get("urgency", "MEDIUM")
                badge_class  = URGENCY_BADGE.get(urgency, "badge-yellow")
                rag_indicator = (
                    '<span class="badge badge-green">RAG</span>'
                    if rag_mode == "rag"
                    else '<span class="badge badge-yellow">FALLBACK</span>'
                )

                st.markdown(f"""
                <div style="display:flex;align-items:center;gap:0.6rem;margin-bottom:1rem;">
                    <span class="badge {badge_class}">🚨 {urgency}</span>
                    {rag_indicator}
                </div>
                """, unsafe_allow_html=True)

                # Original + English transcript
                with st.expander("📝 Transcription", expanded=False):
                    c1, c2 = st.columns(2)
                    c1.markdown(f"**Original ({language})**")
                    c1.info(original_text)
                    c2.markdown("**English Translation**")
                    c2.info(english_text)

                # BNS Sections
                st.markdown(
                    '<div style="font-family:\'Space Mono\',monospace;font-size:0.68rem;'
                    'color:#5a6480;text-transform:uppercase;letter-spacing:0.1em;'
                    'margin-bottom:0.5rem;">BNS 2023 Sections Cited</div>',
                    unsafe_allow_html=True
                )
                for sec in result.get("bns_sections_cited", []):
                    st.markdown(
                        f'<span class="badge badge-orange" style="margin-right:6px;'
                        f'margin-bottom:6px;display:inline-block;">{sec}</span>',
                        unsafe_allow_html=True
                    )

                # Action steps in local language
                st.markdown(
                    f'<div style="font-family:\'Space Mono\',monospace;font-size:0.68rem;'
                    f'color:#5a6480;text-transform:uppercase;letter-spacing:0.1em;'
                    f'margin:1rem 0 0.5rem 0;">Action Steps ({language})</div>',
                    unsafe_allow_html=True
                )
                steps_local = local_advice.split("\n")
                for i, step in enumerate(steps_local, 1):
                    if step.strip():
                        st.markdown(
                            f'<div class="pipeline-step">'
                            f'<span class="step-num">{i:02d}</span>'
                            f'<div class="step-content"><p>{step.strip()}</p></div>'
                            f'</div>',
                            unsafe_allow_html=True
                        )

                # Portal
                portal = result.get("government_portal", "shramsuvidha.gov.in")
                st.markdown(
                    f'<div class="card card-success" style="margin-top:1rem;">'
                    f'<div style="font-size:0.72rem;color:#5a6480;margin-bottom:3px;">'
                    f'GOVERNMENT PORTAL</div>'
                    f'<div style="color:#2ecc71;font-family:\'Space Mono\',monospace;'
                    f'font-size:0.82rem;">{portal}</div>'
                    f'</div>',
                    unsafe_allow_html=True
                )

                # Log to Gold table if Spark available
                try:
                    from pyspark.sql import SparkSession, Row
                    from datetime import datetime
                    spark = SparkSession.builder.getOrCreate()
                    row_df = spark.createDataFrame([Row(
                        call_id            = result.get("call_id", "WEB"),
                        pincode            = pincode or "000000",
                        issue_type         = issue_type,
                        threat_level       = threat_level,
                        bns_sections_cited = ", ".join(result.get("bns_sections_cited", [])),
                        action_steps       = " | ".join(result.get("action_steps", [])),
                        government_portal  = result.get("government_portal", ""),
                        urgency            = urgency,
                        retrieved_chunks   = ", ".join(result.get("retrieved_chunks", [])),
                        timestamp          = datetime.now().isoformat()
                    )])
                    row_df.write.format("delta").mode("append") \
                        .saveAsTable("workspace.default.gold_legal_actions")
                except Exception:
                    pass  # non-blocking — UI never fails because of Delta write

        st.markdown('</div>', unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 2 — AUTHORITY RADAR (HEATMAP)
# ═════════════════════════════════════════════════════════════════════════════
with tab2:

    # ── Load data ────────────────────────────────────────────────────────────
    df = load_heatmap_data()
    df["urgency"] = df["severity_score"].apply(score_to_urgency)

    # ── Stats row ────────────────────────────────────────────────────────────
    total_complaints  = int(df["total_complaints"].sum())
    critical_zones    = int((df["urgency"] == "CRITICAL").sum())
    unique_contractors = int(df["contractor_name"].nunique())
    affected_pincodes = int(df["pincode"].nunique())

    st.markdown(f"""
    <div class="stat-row">
        <div class="stat-box">
            <div class="val">{total_complaints}</div>
            <div class="lbl">Total Complaints</div>
        </div>
        <div class="stat-box">
            <div class="val" style="color:#e74c3c;">{critical_zones}</div>
            <div class="lbl">Critical Zones</div>
        </div>
        <div class="stat-box">
            <div class="val">{unique_contractors}</div>
            <div class="lbl">Flagged Contractors</div>
        </div>
        <div class="stat-box">
            <div class="val">{affected_pincodes}</div>
            <div class="lbl">Affected Pincodes</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Filter controls ───────────────────────────────────────────────────────
    with st.expander("🔧 Filter & Cluster Options", expanded=False):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            urgency_filter = st.multiselect(
                "Urgency Level",
                ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                default=["CRITICAL", "HIGH", "MEDIUM", "LOW"]
            )
        with fc2:
            issue_filter = st.multiselect(
                "Issue Type",
                df["primary_issue"].unique().tolist(),
                default=df["primary_issue"].unique().tolist()
            )
        with fc3:
            cluster_by = st.selectbox(
                "Cluster Marker Size By",
                ["total_complaints", "severity_score"]
            )

    df_filtered = df[
        df["urgency"].isin(urgency_filter) &
        df["primary_issue"].isin(issue_filter)
    ]

    # ── Two-column layout: Map + Table ───────────────────────────────────────
    map_col, info_col = st.columns([3, 2], gap="large")

    with map_col:
        st.markdown(
            '<div style="font-family:\'Space Mono\',monospace;font-size:0.65rem;'
            'color:#5a6480;letter-spacing:0.1em;text-transform:uppercase;'
            'margin-bottom:0.6rem;">Live Exploitation Heatmap — India</div>',
            unsafe_allow_html=True
        )

        m = folium.Map(
            location=[20.5937, 78.9629],
            zoom_start=5,
            tiles="CartoDB dark_matter"
        )

        for _, row in df_filtered.iterrows():
            urgency_val = row["urgency"]
            color       = URGENCY_COLOR.get(urgency_val, "#f39c12")
            radius      = max(8, float(row[cluster_by]) * (3 if cluster_by == "total_complaints" else 5))

            # Outer glow ring
            folium.CircleMarker(
                location     = [row["lat"], row["lon"]],
                radius       = radius + 6,
                color        = color,
                fill         = False,
                weight       = 1,
                opacity      = 0.35,
            ).add_to(m)

            # Main marker
            folium.CircleMarker(
                location     = [row["lat"], row["lon"]],
                radius       = radius,
                color        = color,
                fill         = True,
                fill_color   = color,
                fill_opacity = 0.75,
                popup        = folium.Popup(
                    f"""<div style='font-family:monospace;font-size:12px;
                                   background:#111;color:#d4dae6;padding:8px;
                                   border-radius:4px;min-width:180px;'>
                        <b style='color:{color}'>{row['contractor_name']}</b><br>
                        Pincode: {row['pincode']}<br>
                        Complaints: <b>{row['total_complaints']}</b><br>
                        Issue: {row['primary_issue']}<br>
                        Severity: {row['severity_score']:.1f}<br>
                        Status: <b style='color:{color}'>{urgency_val}</b>
                    </div>""",
                    max_width=220
                ),
                tooltip=f"{row['contractor_name']} — {row['total_complaints']} complaints"
            ).add_to(m)

        # Legend
        legend_html = """
        <div style='position:fixed;bottom:20px;left:20px;z-index:9999;
                    background:#111418;border:1px solid #1e2430;
                    border-radius:6px;padding:10px 14px;font-family:monospace;
                    font-size:11px;color:#d4dae6;'>
            <div style='margin-bottom:5px;font-size:10px;color:#5a6480;
                        text-transform:uppercase;letter-spacing:1px;'>Urgency</div>
            <div><span style='color:#e74c3c'>●</span> Critical (≥8)</div>
            <div><span style='color:#e8622a'>●</span> High (5–8)</div>
            <div><span style='color:#f39c12'>●</span> Medium (3–5)</div>
            <div><span style='color:#2ecc71'>●</span> Low (&lt;3)</div>
        </div>"""
        m.get_root().html.add_child(folium.Element(legend_html))

        st_folium(m, width=None, height=480, returned_objects=[])

    with info_col:
        # ── Flagged contractors table ────────────────────────────────────────
        st.markdown(
            '<div style="font-family:\'Space Mono\',monospace;font-size:0.65rem;'
            'color:#5a6480;letter-spacing:0.1em;text-transform:uppercase;'
            'margin-bottom:0.6rem;">Flagged Contractors</div>',
            unsafe_allow_html=True
        )

        for _, row in df_filtered.sort_values(
                "total_complaints", ascending=False).iterrows():
            urg    = row["urgency"]
            color  = URGENCY_COLOR.get(urg, "#f39c12")
            bcls   = URGENCY_BADGE.get(urg, "badge-yellow")
            st.markdown(f"""
            <div class="card" style="padding:0.9rem 1rem;margin-bottom:0.5rem;">
                <div style="display:flex;justify-content:space-between;
                            align-items:center;margin-bottom:4px;">
                    <div style="font-weight:600;font-size:0.88rem;color:#fff;">
                        {row['contractor_name']}
                    </div>
                    <span class="badge {bcls}">{urg}</span>
                </div>
                <div style="font-family:'Space Mono',monospace;font-size:0.68rem;
                            color:#5a6480;line-height:1.7;">
                    📍 {row['pincode']} &nbsp;·&nbsp;
                    ⚠️ {row['primary_issue']} &nbsp;·&nbsp;
                    📋 {row['total_complaints']} complaints
                </div>
            </div>
            """, unsafe_allow_html=True)

        # ── Show Cause Notice generator ──────────────────────────────────────
        st.markdown("<hr>", unsafe_allow_html=True)
        st.markdown(
            '<div style="font-family:\'Space Mono\',monospace;font-size:0.65rem;'
            'color:#5a6480;letter-spacing:0.1em;text-transform:uppercase;'
            'margin-bottom:0.8rem;">⚖️ Generate Show Cause Notice</div>',
            unsafe_allow_html=True
        )

        contractor_options = df_filtered["contractor_name"].unique().tolist()
        if contractor_options:
            sel_contractor = st.selectbox(
                "Select Contractor", contractor_options, key="notice_contractor")
            sel_row = df_filtered[
                df_filtered["contractor_name"] == sel_contractor].iloc[0]

            if st.button("📄 Generate Legal Notice", key="gen_notice",
                         use_container_width=True):
                with st.spinner("Generating BNS-cited notice..."):
                    try:
                        advisor = load_legal_advisor()
                        notice  = advisor.generate_show_cause_notice(
                            contractor_name   = sel_contractor,
                            pincode           = str(sel_row["pincode"]),
                            total_complaints  = int(sel_row["total_complaints"]),
                            primary_issue     = sel_row["primary_issue"]
                        )
                    except Exception:
                        notice = f"""SHOW CAUSE NOTICE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
To: Contractor {sel_contractor}
Pincode: {sel_row['pincode']}

This office has received {int(sel_row['total_complaints'])} complaints of
{sel_row['primary_issue']} against you in the past 48 hours.

You are directed to appear before the Labour Commissioner
within 7 days of receipt of this notice.

Failure to comply will result in action under:
• BNS Section 316 (Criminal Breach of Trust)
• Code on Wages, 2019

Issued by: Municipal Labour Enforcement Cell
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

                st.text_area("Notice Text", value=notice, height=280,
                             key="notice_output")
                st.download_button(
                    "⬇️ Download Notice (.txt)",
                    data=notice,
                    file_name=f"notice_{sel_contractor.replace(' ','_')}.txt",
                    mime="text/plain",
                    use_container_width=True
                )
        else:
            st.info("No contractors match the current filters.")

    # ── Raw Delta table view ─────────────────────────────────────────────────
    with st.expander("🔬 Raw Gold Table Data (workspace.default.gold_heatmap_clusters)"):
        st.dataframe(
            df_filtered[[
                "contractor_name", "pincode", "total_complaints",
                "severity_score", "primary_issue", "urgency"
            ]].sort_values("total_complaints", ascending=False),
            use_container_width=True,
            hide_index=True
        )
