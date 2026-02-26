# app.py (corrigé)
import io
import requests
import pandas as pd
from collections import Counter
from typing import Optional, Tuple, List, Dict
import streamlit as st

# ---------- Core functions (votre logique) ----------
def fetch_page(url: str, token: str, session: Optional[requests.Session] = None) -> Tuple[List[Dict], Optional[str]]:
    s = session or requests.Session()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    resp = s.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    items = []
    if isinstance(data.get("data"), dict):
        items = data["data"].get("items", [])
    elif isinstance(data.get("data"), list):
        items = data["data"]
    elif isinstance(data, dict) and "items" in data:
        items = data.get("items", [])
    else:
        items = data.get("items", []) or []
    paging = data.get("paging", {}) if isinstance(data, dict) else {}
    next_url = paging.get("next")
    return items, next_url

def aggregate_products(base_url: str, token: str, session: Optional[requests.Session] = None, progress_callback=None, log_callback=None) -> Tuple[List[Dict], Counter]:
    session = session or requests.Session()
    next_url = base_url
    all_rows = []
    counts = Counter()
    page_index = 0

    while next_url:
        page_index += 1
        try:
            if log_callback:
                log_callback(f"Requesting page {page_index}: {next_url}")
            items, next_url = fetch_page(next_url, token, session=session)
        except requests.HTTPError as e:
            msg = f"Erreur HTTP lors de la requête {next_url}: {e}"
            if log_callback: log_callback(msg)
            break
        except requests.RequestException as e:
            msg = f"Erreur réseau/timeout lors de la requête {next_url}: {e}"
            if log_callback: log_callback(msg)
            break

        for item in items:
            sku = item.get("sku")
            state = item.get("state")
            active = item.get("hasActiveVersion")
            all_rows.append({"Sku": sku, "State": state, "Active": active})
            counts[state] += 1

        # Provide a coarse progress indication: page_index -> percentage (capped)
        if progress_callback:
            progress_callback(min(1.0, page_index * 0.05))
        if log_callback:
            log_callback(f"Fetched {len(items)} items. Next URL: {next_url}")

    if progress_callback:
        progress_callback(1.0)
    return all_rows, counts

def to_excel_bytes(details: List[Dict], counts: Counter) -> bytes:
    df_details = pd.DataFrame(details)
    df_counts = pd.DataFrame.from_dict(counts, orient="index", columns=["Count"]).reset_index()
    df_counts.columns = ["State", "Count"]
    with io.BytesIO() as buffer:
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df_details.to_excel(writer, index=False, sheet_name="Details")
            df_counts.to_excel(writer, index=False, sheet_name="Counts")
        return buffer.getvalue()

# ---------- Streamlit UI ----------
st.set_page_config(page_title="Extracteur Produits API → Excel", layout="wide")
st.title("Extraction produits API → Excel (Streamlit)")

st.markdown("Entrez l'URL de base et le token API, puis cliquez **Run**. Résultats : aperçu, histogramme et téléchargement Excel.")

# --- Sidebar params
with st.sidebar:
    st.header("Paramètres")
    default_url = "https://api-merchant.joom.com/api/v3/products/multi?limit=500"
    base_url = st.text_input("Base URL", value=default_url)
    token = st.text_area("Token API (Bearer)", value="", help="Ne collez pas de token public sur un espace partagé.")
    limit_preview = st.number_input("Max lignes aperçu (table)", min_value=5, max_value=5000, value=50, step=5)
    preserve_cache = st.checkbox("Utiliser cache léger (session)", value=True)
    st.markdown("---")
    st.markdown("Conseil : utilisez streamlit secrets pour tokens en production.")

# --- Initialize session state keys we will use
if "logs" not in st.session_state:
    st.session_state["logs"] = ""
if "cache_results" not in st.session_state:
    st.session_state["cache_results"] = {}  # (url, token) -> (details, counts)

# placeholder for logs area
log_area = st.empty()

def append_log(message: str):
    """Mise à jour du log dans st.session_state et affichage."""
    st.session_state["logs"] += message + "\n"
    # write current logs into the text area (keeps expanding)
    log_area.text_area("Logs", value=st.session_state["logs"], height=200)

# Buttons
col1, col2 = st.columns([1, 3])
with col1:
    run = st.button("Run extraction", type="primary")
    clear_logs = st.button("Clear logs")
with col2:
    st.write("")  # spacing

if clear_logs:
    st.session_state["logs"] = ""
    log_area.text_area("Logs", value="", height=200)

# Execution logic with simple session-state cache
if run:
    if not token:
        st.error("Token vide — veuillez coller votre token API dans la barre latérale.")
    elif not base_url:
        st.error("Base URL vide — fournissez l'URL de l'API.")
    else:
        progress_bar = st.progress(0)
        append_log(f"Début extraction pour {base_url}")

        # helper callbacks adapt progress (0..1) to st.progress (0..100)
        def progress_cb(v):
            try:
                pct = int(min(1.0, float(v)) * 100)
                progress_bar.progress(pct)
            except Exception:
                pass

        def log_cb(m):
            append_log(m)

        cache_key = (base_url, token)
        if preserve_cache and cache_key in st.session_state["cache_results"]:
            append_log("Résultat trouvé dans le cache session — utilisation.")
            details, counts = st.session_state["cache_results"][cache_key]
        else:
            try:
                details, counts = aggregate_products(base_url, token, session=requests.Session(), progress_callback=progress_cb, log_callback=log_cb)
                if preserve_cache:
                    st.session_state["cache_results"][cache_key] = (details, counts)
            except Exception as e:
                st.exception(e)
                append_log(f"Exception levée: {e}")
                details, counts = [], Counter()

        progress_bar.progress(100)
        append_log(f"Extraction terminée — {len(details)} lignes collectées.")

        # Show counts
        df_counts = pd.DataFrame.from_dict(counts, orient="index", columns=["Count"]).reset_index()
        df_counts.columns = ["State", "Count"]
        st.subheader("Totaux par état")
        st.dataframe(df_counts)
        if not df_counts.empty:
            st.bar_chart(df_counts.set_index("State"))

        # Preview details
        st.subheader("Aperçu - détails")
        df_details = pd.DataFrame(details)
        if df_details.empty:
            st.info("Aucun détail à afficher.")
        else:
            st.dataframe(df_details.head(int(limit_preview)))

        # Download
        excel_bytes = to_excel_bytes(details, counts)
        st.download_button(
            label="Télécharger Excel",
            data=excel_bytes,
            file_name="Extraction_Products.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# Always show current logs at the bottom
log_area.text_area("Logs", value=st.session_state["logs"], height=200)
