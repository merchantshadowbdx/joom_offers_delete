# app.py
import time
import io
import requests
import pandas as pd
from typing import List, Dict, Optional, Tuple
import streamlit as st
from collections import Counter

# -----------------------
# Fonctions core (API)
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

def iterate_products(base_url: str, token: str, session: Optional[requests.Session] = None, log_cb=None) -> List[Dict]:
    """
    Parcourt toutes les pages à partir de base_url et retourne la liste d'items (chaque item est le dict tel que renvoyé par l'API).
    """
    s = session or requests.Session()
    next_url = base_url
    items_acc: List[Dict] = []
    page = 0

    while next_url:
        page += 1
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

        # extraction items (support des formats variés)
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
        # Petit délai optionnel pour ne pas spammer l'API (décommentez si nécessaire)
        # time.sleep(0.1)

    return items_acc

def summarize_states(items: List[Dict]) -> Tuple[pd.DataFrame, Counter]:
    """
    Construit un DataFrame (Sku, State) et un Counter des états.
    """
    rows = []
    counts = Counter()
    for it in items:
        sku = it.get("sku")
        state = it.get("state")
        # normaliser valeurs None -> 'unknown'
        if sku is None:
            sku = ""
        if state is None:
            state = "unknown"
        rows.append({"Skus": sku, "State": state})
        counts[state] += 1
    df = pd.DataFrame(rows)
    return df, counts

def post_remove_sku(sku: str, token: str, session: Optional[requests.Session] = None) -> Tuple[bool, int, str]:
    """
    Envoie la requête POST pour retirer un produit. Retourne (success, status_code, response_text).
    """
    s = session or requests.Session()
    url_post = "https://api-merchant.joom.com/api/v3/products/remove"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    params = {"sku": sku}
    body = {"reason": "stopSelling"}
    try:
        resp = s.post(url_post, headers=headers, params=params, json=body, timeout=30)
        text = resp.text
        return (resp.status_code == 200, resp.status_code, text)
    except requests.RequestException as e:
        return (False, -1, str(e))

# -----------------------
# Export Excel helper
# -----------------------
def df_to_excel_bytes(df: pd.DataFrame, summary_counts: pd.DataFrame = None) -> bytes:
    with io.BytesIO() as buf:
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="SKUs")
            if summary_counts is not None:
                summary_counts.to_excel(writer, index=False, sheet_name="Summary")
        return buf.getvalue()

# -----------------------
# UI Streamlit
# -----------------------
st.set_page_config(title="Gestion offres Joom — suppression par statut", layout="wide")
st.title("Gestion offres Joom — extraction & suppression par statut")

st.sidebar.header("Paramètres API")
default_url = "https://api-merchant.joom.com/api/v3/products/multi?limit=500"
base_url = st.sidebar.text_input("Base URL (API)", value=default_url)
token = st.sidebar.text_area("Token API (Bearer)", value="", height=120,
                             help="Ne mettez pas de token sensible dans un dépôt public. Utilisez streamlit secrets en production.")

st.sidebar.markdown("---")
st.sidebar.markdown("Options d'extraction")
preserve_cache = st.sidebar.checkbox("Utiliser cache session (même URL+token)", value=True)
limit_preview = st.sidebar.number_input("Max lignes aperçu", min_value=5, max_value=5000, value=100, step=5)

# Initialisation session_state
if "logs" not in st.session_state:
    st.session_state["logs"] = ""
if "cached" not in st.session_state:
    st.session_state["cached"] = {}  # clé (base_url, token) -> dataframe

# zone de logs
log_placeholder = st.empty()

def log(msg: str):
    st.session_state["logs"] += msg + "\n"
    log_placeholder.text_area("Logs", value=st.session_state["logs"], height=260)

# Buttons
col_run, col_actions = st.columns([1, 2])
with col_run:
    run_extract = st.button("Extraire le catalogue")
with col_actions:
    clear_logs = st.button("Effacer logs")
    force_refresh = st.button("Forcer refresh (ignorer cache)")

if clear_logs:
    st.session_state["logs"] = ""
    log("Logs cleared.")

# Extraction
df = pd.DataFrame()
counts = Counter()
if run_extract:
    if not token:
        st.error("Token vide — collez votre token dans la barre latérale.")
    elif not base_url:
        st.error("Base URL vide.")
    else:
        cache_key = (base_url, token)
        use_cache = preserve_cache and (cache_key in st.session_state["cached"]) and (not force_refresh)
        if use_cache:
            st.info("Utilisation du cache session pour l'URL et token fournis.")
            df = st.session_state["cached"][cache_key].copy()
            # compute counts
            counts = Counter(df["State"].tolist())
        else:
            st.info("Lancement de l'extraction depuis l'API...")
            progress = st.progress(0)
            st.session_state["logs"] = ""  # reset logs per run
            log("Démarrage extraction...")
            try:
                # collect items
                items = iterate_products(base_url, token, session=requests.Session(), log_cb=log)
                df, counts = summarize_states(items)
                if preserve_cache:
                    st.session_state["cached"][cache_key] = df.copy()
                log(f"Extraction terminée, {len(df)} SKUs récupérés.")
            except Exception as e:
                log(f"Exception lors de l'extraction: {e}")
                st.exception(e)
            finally:
                progress.progress(100)

# If df exists (extracted) show it
if not df.empty:
    st.subheader("Aperçu des SKUs")
    st.write(f"Total SKUs: {len(df)}")
    st.dataframe(df.head(int(limit_preview)))

    # Totaux par statut
    st.subheader("Totaux par statut")
    summary_df = pd.DataFrame.from_records(list(counts.items()), columns=["State", "Count"]).sort_values("Count", ascending=False)
    st.table(summary_df)

    # Selection des statuts à supprimer (checkboxes)
    st.subheader("Sélectionnez les statuts à supprimer")
    # dynamique : créer une checkbox par statut et stocker sélection dans session_state
    status_list = summary_df["State"].tolist()
    selected_statuses = []
    cols = st.columns(3)
    for i, st_name in enumerate(status_list):
        holder = cols[i % 3]
        key = f"chk_{st_name}"
        # default unchecked except for specific statuses dangerous
        default = st_name in ("rejected", "disabledByMerchant", "disabledByJoom")
        checked = holder.checkbox(f"{st_name} ({counts[st_name]})", value=default, key=key)
        if checked:
            selected_statuses.append(st_name)

    if not selected_statuses:
        st.info("Aucun statut sélectionné pour suppression.")
    else:
        st.success(f"Statuts sélectionnés pour suppression : {', '.join(selected_statuses)}")

        # Filtrer DataFrame pour ne garder que les SKUs à supprimer
        df_to_delete = df[df["State"].isin(selected_statuses)].copy()
        st.write(f"SKUs ciblés : {len(df_to_delete)}")
        st.dataframe(df_to_delete.head(200))

        # Confirm button and delete action
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

                    # Optionally small sleep to avoid rate-limits (uncomment if needed)
                    # time.sleep(0.05)

                # Résumé
                st.success("Opération terminée.")
                st.write(f"Total traités: {total}")
                st.write(f"Succès: {len(removed_rows)}")
                st.write(f"Échecs: {len(failed_rows)}")

                if removed_rows:
                    df_removed = pd.DataFrame(removed_rows)
                    st.subheader("Removed (extrait)")
                    st.dataframe(df_removed.head(200))
                else:
                    df_removed = pd.DataFrame(columns=["Skus", "State", "StatusCode"])

                if failed_rows:
                    df_failed = pd.DataFrame(failed_rows)
                    st.subheader("Failed (extrait)")
                    st.dataframe(df_failed.head(200))
                else:
                    df_failed = pd.DataFrame(columns=["Skus", "State", "StatusCode", "Error"])

                # Téléchargement rapport Excel (removed + failed + summary)
                report_df = pd.concat([
                    df_removed.assign(Result="removed"),
                    df_failed.assign(Result="failed")
                ], ignore_index=True, sort=False)

                summary_counts = pd.DataFrame.from_records([
                    {"State": k, "Count": v} for k, v in counts.items()
                ]).sort_values("Count", ascending=False)

                tobytes = df_to_excel_bytes(report_df, summary_counts)
                st.download_button("Télécharger rapport Excel (removed+failed)", data=tobytes,
                                   file_name="joom_remove_report.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Afficher logs en bas
st.subheader("Logs")
log_placeholder.text_area("Logs", value=st.session_state["logs"], height=260)
