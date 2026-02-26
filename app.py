# app.py
import io
import requests
import pandas as pd
from collections import Counter
from typing import Optional, Tuple, List, Dict
import streamlit as st

# ---------- Core functions (votre logique existante, légèrement réorganisée) ----------

def fetch_page(url: str, token: str, session: Optional[requests.Session] = None) -> Tuple[List[Dict], Optional[str]]:
    """Récupère une page de produits et retourne (items_list, next_url)"""
    s = session or requests.Session()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    resp = s.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # Support multiple structures
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
    """Parcourt toutes les pages et retourne la liste détaillée des produits + compteur par état."""
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

        if progress_callback:
            # progress_callback expects a float 0..1, we approximate by pages (not precise)
            progress_callback(min(0.99, page_index * 0.05))
        if log_callback:
            log_callback(f"Fetched {len(items)} items. Next URL: {next_url}")

    if progress_callback:
        progress_callback(1.0)
    return all_rows, counts

def to_excel_bytes(details: List[Dict], counts: Counter) -> bytes:
    """Retourne le fichier Excel en mémoire (bytes)."""
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
st.markdown(
    "Entrez l'URL de base et le token API, puis cliquez **Run**. "
    "Résultats : aperçu, histogramme par état, et bouton de téléchargement Excel."
)

with st.sidebar:
    st.header("Paramètres")
    default_url = "https://api-merchant.joom.com/api/v3/products/multi?limit=500"
    base_url = st.text_input("Base URL", value=default_url)
    token = st.text_area("Token API (Bearer)", value="", help="Ne collez pas de token public sur un espace partagé.")
    limit_preview = st.number_input("Max lignes aperçu (table)", min_value=5, max_value=5000, value=50, step=5)
    preserve_cache = st.checkbox("Utiliser le cache Streamlit (si même URL+token)", value=True)
    st.markdown("---")
    st.markdown("Conseils : évitez d'insérer un token dans du code partagé. Préférez saisir ici.")

# Logging area
log_box = st.empty()

def log(msg: str):
    # append in the textarea-like widget
    prev = log_box.session_state.get("logs", "")
    new = prev + msg + "\n"
    log_box.session_state["logs"] = new
    log_box.text_area("Logs", value=new, height=200)

# Buttons
col1, col2 = st.columns([1, 3])
with col1:
    run = st.button("Run extraction", type="primary")
    clear_logs = st.button("Clear logs")
with col2:
    st.write("")  # placeholder for layout

if clear_logs:
    log_box.session_state["logs"] = ""
    log_box.text_area("Logs", value="", height=200)

# caching wrapper
if preserve_cache:
    @st.cache_data(show_spinner=False)
    def cached_aggregate(url, token):
        return aggregate_products(url, token, session=requests.Session(), progress_callback=None, log_callback=None)
else:
    def cached_aggregate(url, token):
        return aggregate_products(url, token, session=requests.Session(), progress_callback=None, log_callback=None)

# Execution
if run:
    if not token:
        st.error("Token vide — veuillez coller votre token API dans la zone 'Token API' de la barre latérale.")
    elif not base_url:
        st.error("Base URL vide — fournissez l'URL de l'API.")
    else:
        # real run with live progress & logs
        progress = st.progress(0.0)
        log_box.session_state["logs"] = log_box.session_state.get("logs", "")
        try:
            def progress_cb(v):
                try:
                    progress.progress(min(1.0, float(v)))
                except Exception:
                    pass

            def log_cb(m):
                log(m)

            # if caching is enabled, call the cached wrapper which calls aggregate (but we need callbacks)
            if preserve_cache:
                # can't pass callbacks through st.cache_data easily; call non-cached but still respect preserve_cache flag:
                details, counts = aggregate_products(base_url, token, session=requests.Session(), progress_callback=progress_cb, log_callback=log_cb)
            else:
                details, counts = aggregate_products(base_url, token, session=requests.Session(), progress_callback=progress_cb, log_callback=log_cb)

            st.success(f"Extraction terminée — {len(details)} lignes collectées.")
            # Show counts
            df_counts = pd.DataFrame.from_dict(counts, orient="index", columns=["Count"]).reset_index()
            df_counts.columns = ["State", "Count"]
            st.subheader("Totaux par état")
            st.dataframe(df_counts)
            st.bar_chart(df_counts.set_index("State"))

            # Preview details
            st.subheader("Aperçu - détails")
            df_details = pd.DataFrame(details)
            if df_details.empty:
                st.info("Aucun détail à afficher.")
            else:
                st.dataframe(df_details.head(int(limit_preview)))

            # Prepare Excel bytes and download
            excel_bytes = to_excel_bytes(details, counts)
            st.download_button(
                label="Télécharger Excel",
                data=excel_bytes,
                file_name="Extraction_Products.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # Show summary in logs
            log(f"Finished. Total rows: {len(details)}. States: {len(counts)}")
            for state, c in counts.most_common():
                log(f"  {state}: {c}")

        except Exception as e:
            st.exception(e)
            log(f"Exception levée: {e}")

# Show previous logs if any
if "logs" in log_box.session_state:
    log_box.text_area("Logs", value=log_box.session_state["logs"], height=200)
else:
    log_box.text_area("Logs", value="", height=200)
