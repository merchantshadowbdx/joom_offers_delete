# app.py
"""
Streamlit application pour :
- extraire les produits via l'API Joom (pagination),
- afficher SKUs et états,
- sélectionner par statut (multiselect) les produits à supprimer,
- lancer la suppression avec barre de progression et logs,
- télécharger un rapport Excel (removed + failed).
"""
import time
import io
from typing import List, Dict, Optional, Tuple
from collections import Counter

import requests
import pandas as pd
import streamlit as st


# -----------------------
# Fonctions utilitaires
# -----------------------
def fetch_json(url: str, token: str, session: Optional[requests.Session] = None, timeout: int = 30) -> dict:
    s = session or requests.Session()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    resp = s.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def iterate_products(base_url: str,
                     token: str,
                     session: Optional[requests.Session] = None,
                     log_cb=None,
                     max_pages: int = 0,
                     delay_s: float = 0.0) -> List[Dict]:
    s = session or requests.Session()
    next_url = base_url
    items_acc: List[Dict] = []
    page = 0

    while next_url:
        page += 1
        if max_pages and page > max_pages:
            if log_cb:
                log_cb(f"Reached max_pages ({max_pages}), stopping.")
            break

        if log_cb:
            log_cb(f"Requesting page {page}: {next_url}")

        try:
            data = fetch_json(next_url, token, session=s)
        except requests.HTTPError as e:
            if log_cb:
                log_cb(f"HTTP error page {page}: {e}")
            break
        except requests.RequestException as e:
            if log_cb:
                log_cb(f"Network error page {page}: {e}")
            break

        # extraction items (support formats variés)
        data_field = data.get("data", {})
        page_items = []
        if isinstance(data_field, dict):
            page_items = data_field.get("items", []) or []
        elif isinstance(data_field, list):
            page_items = data_field
        else:
            page_items = data.get("items", []) or []

        items_acc.extend(page_items)

        # pagination : next dans data.paging.next
        paging = data.get("paging", {}) if isinstance(data, dict) else {}
        next_url = paging.get("next")
        if log_cb:
            log_cb(f"Fetched {len(page_items)} items; next: {next_url}")

        if delay_s and next_url:
            time.sleep(delay_s)

    return items_acc


def summarize_states(items: List[Dict]) -> Tuple[pd.DataFrame, Counter]:
    rows = []
    counts = Counter()
    for it in items:
        sku = it.get("sku") or ""
        state = it.get("state") or "unknown"
        rows.append({"Skus": sku, "State": state})
        counts[state] += 1
    df = pd.DataFrame(rows)
    return df, counts


def post_remove_sku(sku: str, token: str, session: Optional[requests.Session] = None) -> Tuple[bool, int, str]:
    s = session or requests.Session()
    url_post = "https://api-merchant.joom.com/api/v3/products/remove"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    params = {"sku": sku}
    body = {"reason": "stopSelling"}
    try:
        resp = s.post(url_post, headers=headers, params=params, json=body, timeout=30)
        return (resp.status_code == 200, resp.status_code, resp.text)
    except requests.RequestException as e:
        return (False, -1, str(e))


def df_to_excel_bytes(df: pd.DataFrame, summary_counts: pd.DataFrame = None) -> bytes:
    with io.BytesIO() as buf:
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Report")
            if summary_counts is not None:
                summary_counts.to_excel(writer, index=False, sheet_name="Summary")
        return buf.getvalue()


# -----------------------
# Streamlit UI
# -----------------------
st.set_page_config(page_title="Joom - Suppression offres", layout="wide")
st.title("Suppression des offres Joom selon leur statut")

# Sidebar : paramètres
st.sidebar.header("Paramètres API & options")
default_url = "https://api-merchant.joom.com/api/v3/products/multi?limit=500"
base_url = st.sidebar.text_input("Base URL (API)", value=default_url)
token = st.sidebar.text_area("Token API (Bearer)", value="", height=140)
preserve_cache = st.sidebar.checkbox("Utiliser cache session (même URL+token)", value=True)
max_pages = st.sidebar.number_input("Max pages à récupérer (0 = illimité)", min_value=0, value=0, step=1)
delay_s = st.sidebar.number_input("Delay entre requêtes (s)", min_value=0.0, value=0.0, step=0.1)
limit_preview = st.sidebar.number_input("Max lignes aperçu", min_value=5, max_value=5000, value=200, step=5)
st.sidebar.markdown("---")

# session_state initialisation
if "logs" not in st.session_state:
    st.session_state["logs"] = ""
if "cached" not in st.session_state:
    st.session_state["cached"] = {}  # clé (base_url, token) -> df
if "last_df" not in st.session_state:
    st.session_state["last_df"] = pd.DataFrame()
if "last_counts" not in st.session_state:
    st.session_state["last_counts"] = Counter()

# NOTE: We no longer create or update the text_area UI from the log() function.
# log() only appends to st.session_state["logs"]. The single text_area is rendered once at the end.

def log(msg: str):
    """Append a log message to session state (no UI calls here)."""
    st.session_state["logs"] += msg + "\n"


# Controls
col_run, col_actions = st.columns([1, 2])
with col_run:
    run_extract = st.button("Extraire le catalogue")
with col_actions:
    clear_logs = st.button("Effacer logs")
    force_refresh = st.button("Forcer refresh (ignorer cache)")

if clear_logs:
    st.session_state["logs"] = ""
    log("Logs effacés.")

# Extraction
df = pd.DataFrame()
counts = Counter()
if run_extract:
    if not token:
        st.error("Token vide — collez votre token dans la barre latérale.")
    elif not base_url:
        st.error("Base URL vide.")
    else:
        cache_key = (base_url, token, max_pages, delay_s)
        use_cache = preserve_cache and (cache_key in st.session_state["cached"]) and (not force_refresh)
        if use_cache:
            st.info("Utilisation du cache session pour l'URL/token fournis.")
            df = st.session_state["cached"][cache_key].copy()
            counts = Counter(df["State"].tolist())
            st.session_state["last_df"] = df.copy()
            st.session_state["last_counts"] = counts
            log(f"Restored {len(df)} rows from cache.")
        else:
            st.info("Lancement de l'extraction depuis l'API...")
            st.session_state["logs"] = ""  # reset logs per run
            log("Démarrage extraction...")
            progress = st.progress(0)
            try:
                items = iterate_products(base_url, token, session=requests.Session(), log_cb=log, max_pages=int(max_pages), delay_s=float(delay_s))
                df, counts = summarize_states(items)
                st.session_state["last_df"] = df.copy()
                st.session_state["last_counts"] = counts
                if preserve_cache:
                    st.session_state["cached"][cache_key] = df.copy()
                log(f"Extraction terminée, {len(df)} SKUs récupérés.")
            except Exception as e:
                log(f"Exception lors de l'extraction: {e}")
                st.exception(e)
            finally:
                progress.progress(100)

# Restaurer depuis session si présent (important pour les interactions qui provoquent des reruns)
if not st.session_state["last_df"].empty:
    df = st.session_state["last_df"].copy()
    counts = st.session_state["last_counts"]

# Affichage si df disponible
if not df.empty:
    st.subheader("Aperçu des SKUs")
    st.write(f"Total SKUs: {len(df)}")
    st.dataframe(df.head(int(limit_preview)))

    st.subheader("Totaux par statut")
    summary_df = pd.DataFrame.from_records(list(counts.items()), columns=["State", "Count"]).sort_values("Count", ascending=False)
    st.table(summary_df)

    # Sélection des statuts : multiselect (conserve l'état entre reruns)
    st.subheader("Sélectionnez les statuts à supprimer")
    status_list = summary_df["State"].tolist()
    default_selection = [s for s in ("rejected") if s in status_list]

    selected_statuses = st.multiselect(
        "Statuts (sélection multiple possible)",
        options=status_list,
        default=default_selection,
        key="selected_statuses"
    )

    # Boutons d'aide pour sélectionner/désélectionner rapidement (gardent l'état via session_state)
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        if st.button("Select all statuses"):
            st.session_state["selected_statuses"] = status_list
            selected_statuses = status_list
    with c2:
        if st.button("Deselect all statuses"):
            st.session_state["selected_statuses"] = []
            selected_statuses = []
    with c3:
        if st.button("Invert selection"):
            inverted = [s for s in status_list if s not in st.session_state.get("selected_statuses", [])]
            st.session_state["selected_statuses"] = inverted
            selected_statuses = inverted

    if not selected_statuses:
        st.info("Aucun statut sélectionné pour suppression.")
    else:
        st.success(f"Statuts sélectionnés pour suppression : {', '.join(selected_statuses)}")
        df_to_delete = df[df["State"].isin(selected_statuses)].copy()
        st.write(f"SKUs ciblés : {len(df_to_delete)}")
        st.dataframe(df_to_delete.head(200))

        st.subheader("Lancer suppression")
        confirm = st.button("Confirmer suppression des SKUs sélectionnés", key="confirm_delete")
        if confirm:
            if df_to_delete.empty:
                st.warning("Aucun SKU à supprimer (liste vide).")
            else:
                total = len(df_to_delete)
                progress_bar = st.progress(0)
                removed_rows = []
                failed_rows = []
                session = requests.Session()
                # itération sur les SKUs à supprimer
                for idx, (_, row) in enumerate(df_to_delete.iterrows(), start=1):
                    sku = row["Skus"]
                    success, status_code, resp_text = post_remove_sku(sku, token, session=session)
                    if success:
                        removed_rows.append({"Skus": sku, "State": row["State"], "StatusCode": status_code})
                        log(f"Removed {sku} (code {status_code})")
                    else:
                        failed_rows.append({"Skus": sku, "State": row["State"], "StatusCode": status_code, "Error": resp_text})
                        log(f"Failed {sku} (code {status_code}) - {resp_text}")

                    # update progress
                    pct = int((idx / total) * 100)
                    progress_bar.progress(pct)

                    # small delay optional to avoid hitting rate limits (uncomment if needed)
                    # time.sleep(0.05)

                # Résumé
                st.success("Opération terminée.")
                st.write(f"Total traités: {total}")
                st.write(f"Succès: {len(removed_rows)}")
                st.write(f"Échecs: {len(failed_rows)}")

                df_removed = pd.DataFrame(removed_rows) if removed_rows else pd.DataFrame(columns=["Skus", "State", "StatusCode"])
                df_failed = pd.DataFrame(failed_rows) if failed_rows else pd.DataFrame(columns=["Skus", "State", "StatusCode", "Error"])

                if not df_removed.empty:
                    st.subheader("Removed (extrait)")
                    st.dataframe(df_removed.head(200))
                if not df_failed.empty:
                    st.subheader("Failed (extrait)")
                    st.dataframe(df_failed.head(200))

                # Préparer rapport Excel
                report_df = pd.concat([
                    df_removed.assign(Result="removed"),
                    df_failed.assign(Result="failed")
                ], ignore_index=True, sort=False) if (not df_removed.empty or not df_failed.empty) else pd.DataFrame()

                summary_counts = pd.DataFrame.from_records([
                    {"State": k, "Count": v} for k, v in counts.items()
                ]).sort_values("Count", ascending=False)

                if not report_df.empty:
                    tobytes = df_to_excel_bytes(report_df, summary_counts)
                    st.download_button("Télécharger rapport Excel (removed+failed)", data=tobytes,
                                       file_name="joom_remove_report.xlsx",
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Affichage unique des logs en bas (un seul widget text_area avec key unique)
st.subheader("Logs")
st.text_area("Logs", value=st.session_state["logs"], height=260, key="logs_area")
