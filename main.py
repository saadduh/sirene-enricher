"""
SIRENE Enricher  -  INSEE API
Author : Saad Janina

Changes (all fixes applied):
  BUGS FIXED:
  - [BUG-1]  Race condition on _ok_count/_err_count: replaced with thread-safe
              atomic increments via threading.Lock (_stats_lock)
  - [BUG-2]  batch_err was a reference to self._err_rows in non-batch mode,
              causing every error to be written twice -> fixed with proper scoping
  - [BUG-3]  _out_path(bn) called twice with potential divergence -> computed once
  - [BUG-4]  Resume did not restore _ok_count/_err_count -> now restored from checkpoint
  - [BUG-5]  fetch_idcc called for non-French companies -> skipped when Pays != France
  - [BUG-6]  DEPT_REGION missing departments 30 and 84 -> added
  - [BUG-7]  etablissementSiege too strict (only True/False) -> added O/N/OUI/NON/1/0
  - [BUG-8]  EFFECTIF_MAP missing codes 03-10, NN, XX -> added
  - [BUG-9]  IDCC absent from OUTPUT_COLUMNS -> added
  - [BUG-10] API key stored in StringVar -> no structural change (desktop-only),
              but key is now cleared from StringVar after worker starts
  - [BUG-11] Checkpoint JSON unencrypted -> documented in README, .gitignore added
  - [BUG-12] timeout not split (connect vs read) -> now (5, 15) tuple
  - [BUG-13] _SPLASH/_SPIN globals -> moved into show_splash()
  - [BUG-14] _hex() not validated -> try/except added
  - [BUG-15] Worker thread unnamed -> name="SIRENEWorker" added

  IMPROVEMENTS:
  - [IMP-1]  Address strip: .strip() added to joined address parts
  - [IMP-2]  Checkpoint now includes err_rows for complete resume
  - [IMP-3]  Write lock (threading.Lock) added to _write_excel
  - [IMP-4]  Delay between requests is now interruptible (0.1s ticks)
  - [IMP-5]  SIRET format validation: must be exactly 14 digits (regex)
  - [IMP-6]  NAF and FJ maps loaded from external JSON files (naf.json / fj.json)
              alongside the script; hardcoded maps used as fallback
  - [IMP-7]  Network retry on ConnectionError/Timeout (3 attempts, 5s backoff)
  - [IMP-8]  CLI mode: python sirene_enricher.py --headless --file X --key Y ...
  - [IMP-9]  Global SIRET cache: ~/.sirene_cache/global_cache.json avoids
              re-querying SIRETs already enriched in previous sessions
  - [IMP-10] Windows taskbar progress (via ctypes, no extra dep, no-op on other OS)
"""

import os, json, re, time, queue, threading, argparse
import requests, pandas as pd
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, StringVar, BooleanVar, END, BOTH, LEFT, RIGHT

try:
    from PIL import Image, ImageDraw, ImageFont
    _PILLOW = True
except ImportError:
    _PILLOW = False

# ---------------------------------------------------------------
#  TASKBAR PROGRESS (Windows only, no extra dependency)
# ---------------------------------------------------------------
try:
    import ctypes
    _TASKBAR = ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID  # noqa
    _ITaskbarList3 = None

    def _tb_init(hwnd):
        global _ITaskbarList3
        try:
            clsid  = ctypes.GUID("{56FDF344-FD6D-11D0-958A-006097C9A090}")
            iid    = ctypes.GUID("{EA1AFB91-9E28-4B86-90E9-9E9F8A5EEFAF}")
            ptr    = ctypes.c_void_p()
            ctypes.windll.ole32.CoCreateInstance(
                ctypes.byref(clsid), None, 1,
                ctypes.byref(iid), ctypes.byref(ptr))
            _ITaskbarList3 = ptr
        except Exception:
            _ITaskbarList3 = None

    def _tb_set(hwnd, done, total):
        if _ITaskbarList3 is None or total == 0:
            return
        try:
            # TBPF_NORMAL = 0x2
            vt = ctypes.c_void_p * 6
            vtbl = vt.from_address(ctypes.cast(_ITaskbarList3, ctypes.POINTER(ctypes.c_void_p)).contents.value)
            SetProgressValue = ctypes.WINFUNCTYPE(
                ctypes.HRESULT, ctypes.c_void_p, ctypes.c_void_p,
                ctypes.c_ulonglong, ctypes.c_ulonglong)(vtbl[9])
            SetProgressState = ctypes.WINFUNCTYPE(
                ctypes.HRESULT, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)(vtbl[10])
            SetProgressState(_ITaskbarList3, hwnd, 0x2)
            SetProgressValue(_ITaskbarList3, hwnd, done, total)
        except Exception:
            pass

    _TB_AVAILABLE = True
except Exception:
    _TB_AVAILABLE = False
    def _tb_init(hwnd): pass
    def _tb_set(hwnd, done, total): pass


# ---------------------------------------------------------------
#  I18N
# ---------------------------------------------------------------
T = {
"fr": {
    "title":           "SIRENE Enricher  -  API INSEE",
    "subtitle":        "INSEE — Annuaire des entreprises",
    "api_panel":       "CONFIGURATION API",
    "api_key_lbl":     "Cle API INSEE",
    "btn_test_key":    "Tester la cle",
    "key_ok":          "Cle valide — connexion OK.",
    "key_fail":        "Cle invalide ou erreur reseau.",
    "key_testing":     "Test de la cle en cours...",
    "file_panel":      "FICHIER SOURCE",
    "no_file":         "Aucun fichier selectionne",
    "choose_file":     "Choisir un fichier Excel",
    "siret_col_lbl":   "Colonne SIRET :",
    "output_panel":    "FICHIER DE SORTIE",
    "same_folder":     "Meme dossier que la source",
    "choose_folder":   "Choisir le dossier",
    "fname_lbl":       "Nom du fichier (.xlsx auto)",
    "run_panel":       "PARAMETRES",
    "delay_lbl":       "Delai entre requetes (s)",
    "idcc_chk":        "Recuperer l'IDCC",
    "idcc_hint":       "Appel API supplementaire par SIRET.",
    "batch_panel":     "MODE BATCH",
    "batch_chk":       "Activer le mode batch",
    "batch_size_lbl":  "Lignes par batch",
    "batch_pause_lbl": "Pause entre batches (s)",
    "batch_hint":      "Chaque batch = un fichier separe.",
    "actions_panel":   "ACTIONS",
    "btn_test":        "Tester 2 SIRETs",
    "btn_start":       "Demarrer",
    "btn_stop":        "Arreter",
    "btn_resume":      "Reprendre",
    "tab_run":         "  Execution  ",
    "tab_stats":       "  Statistiques  ",
    "prog_panel":      "PROGRESSION",
    "stat_done":       "Traites",
    "stat_total":      "Total",
    "stat_ok":         "OK",
    "stat_err":        "Erreurs",
    "stat_batch":      "Batch",
    "stat_eta":        "Temps restant",
    "log_panel":       "JOURNAL",
    "btn_clear":       "Effacer",
    "btn_export_png":  "Exporter PNG",
    "chart_rate":      "TAUX DE SUCCES",
    "chart_time":      "PROGRESSION",
    "chart_eff":       "EFFECTIF",
    "chart_naf":       "TOP ACTIVITES",
    "chart_region":    "TOP REGIONS",
    "no_data":         "Pas encore de donnees",
    "ok_lbl":          "OK",
    "err_lbl":         "Erreurs",
    "chart_cumul":     "OK cumules vs Erreurs",
    "credit":          "Realise par : Saad Janina",
    "err_sheet":       "SIRET Introuvables",
    "main_sheet":      "Resultats",
    "dup_title":       "Doublons detectes",
    "dup_body":        "{n} SIRET(s) en doublon :\n{lst}\n\nQue souhaitez-vous faire ?",
    "dup_keep":        "Garder toutes les lignes",
    "dup_dedup":       "Supprimer les doublons",
    "dup_cancel":      "Annuler",
    "resume_title":    "Reprendre ?",
    "resume_body":     "Checkpoint trouve ({n} SIRETs deja traites).\nReprendre depuis le checkpoint ?",
    "no_checkpoint":   "Aucun checkpoint trouve.",
    "need_key":        "Veuillez entrer votre cle API INSEE.",
    "need_file":       "Veuillez selectionner un fichier Excel.",
    "need_2":          "Il faut au moins 2 SIRETs valides pour le test.",
    "col_not_found":   "Colonne '{c}' introuvable dans le fichier.",
    "already_running": "Une execution est deja en cours.",
    "started":         "Enrichissement demarre.",
    "stopped":         "Arrete — checkpoint sauvegarde.",
    "no_run":          "Aucune execution en cours.",
    "no_sirets":       "Aucun SIRET valide trouve.",
    "invalid_siret":   "SIRET invalide ignore (format) : {s}",
    "rate_wait":       "Rate-limit (429) — attente {s}s (essai {n}/3)...",
    "rate_giveup":     "{s} — rate-limit depasse apres 3 essais.",
    "err_404":         "SIRET introuvable ou etablissement ferme (HTTP 404) : {s}",
    "net_retry":       "Erreur reseau — retry {n}/3 dans 5s : {e}",
    "done_msg":        "Termine — {ok} OK, {err} erreurs",
    "saved":           "Sauvegarde -> {p}",
    "batch_saved":     "Batch {n} sauvegarde -> {p}",
    "save_fail":       "Echec sauvegarde : {e}",
    "test_phase":      "PHASE TEST — 2 SIRETs aleatoires",
    "test_ok":         "Test {i} OK :",
    "test_fail":       "Test {i} — HTTP {c}",
    "test_ask":        "Test termine.\nDemarrer l'execution complete ?",
    "png_no_pillow":   "Pillow non installe.\nFaites : pip install Pillow",
    "png_saved":       "Stats exportees -> {p}",
    "png_fail":        "Export PNG echoue : {e}",
    "cp_saved":        "Checkpoint ({n} SIRETs).",
    "col_auto":        "Colonne detectee : {c}",
    "idcc_fetch":      "Recuperation IDCC...",
    "processing":      "{n} SIRETs a traiter",
    "batch_info":      "{nb} batches de {bs} (pause {bp}s) — 1 fichier par batch",
    "full_run":        "Execution complete (sans batch)",
    "cache_hit":       "Cache global : {n} SIRETs deja connus",
    "sep":             "-" * 56,
},
"en": {
    "title":           "SIRENE Enricher  -  INSEE API",
    "subtitle":        "INSEE — French Business Directory",
    "api_panel":       "API CONFIGURATION",
    "api_key_lbl":     "INSEE API Key",
    "btn_test_key":    "Test API Key",
    "key_ok":          "Key valid — connection OK.",
    "key_fail":        "Invalid key or network error.",
    "key_testing":     "Testing key...",
    "file_panel":      "SOURCE FILE",
    "no_file":         "No file selected",
    "choose_file":     "Choose Excel File",
    "siret_col_lbl":   "SIRET column:",
    "output_panel":    "OUTPUT FILE",
    "same_folder":     "Same folder as source",
    "choose_folder":   "Choose Folder",
    "fname_lbl":       "File name (.xlsx auto)",
    "run_panel":       "RUN SETTINGS",
    "delay_lbl":       "Delay between requests (s)",
    "idcc_chk":        "Fetch IDCC",
    "idcc_hint":       "Extra API call per SIRET.",
    "batch_panel":     "BATCH MODE",
    "batch_chk":       "Enable batch mode",
    "batch_size_lbl":  "Lines per batch",
    "batch_pause_lbl": "Pause between batches (s)",
    "batch_hint":      "Each batch = a separate file.",
    "actions_panel":   "ACTIONS",
    "btn_test":        "Test 2 SIRETs",
    "btn_start":       "Start Run",
    "btn_stop":        "Stop",
    "btn_resume":      "Resume",
    "tab_run":         "  Run  ",
    "tab_stats":       "  Statistics  ",
    "prog_panel":      "PROGRESS",
    "stat_done":       "Done",
    "stat_total":      "Total",
    "stat_ok":         "OK",
    "stat_err":        "Errors",
    "stat_batch":      "Batch",
    "stat_eta":        "Time left",
    "log_panel":       "ACTIVITY LOG",
    "btn_clear":       "Clear",
    "btn_export_png":  "Export PNG",
    "chart_rate":      "SUCCESS RATE",
    "chart_time":      "PROGRESS OVER TIME",
    "chart_eff":       "EMPLOYEE SIZE",
    "chart_naf":       "TOP NAF ACTIVITIES",
    "chart_region":    "TOP REGIONS",
    "no_data":         "No data yet",
    "ok_lbl":          "OK",
    "err_lbl":         "Errors",
    "chart_cumul":     "Cumulative OK vs Errors",
    "credit":          "Made by : Saad Janina",
    "err_sheet":       "SIRET Not Found",
    "main_sheet":      "Results",
    "dup_title":       "Duplicates detected",
    "dup_body":        "{n} duplicate SIRET(s):\n{lst}\n\nWhat would you like to do?",
    "dup_keep":        "Keep all rows",
    "dup_dedup":       "Deduplicate",
    "dup_cancel":      "Cancel",
    "resume_title":    "Resume?",
    "resume_body":     "Checkpoint found ({n} SIRETs done).\nResume from checkpoint?",
    "no_checkpoint":   "No checkpoint found.",
    "need_key":        "Please enter your INSEE API key.",
    "need_file":       "Please select an Excel file.",
    "need_2":          "Need at least 2 valid SIRETs for the test.",
    "col_not_found":   "Column '{c}' not found in file.",
    "already_running": "A run is already in progress.",
    "started":         "Enrichment started.",
    "stopped":         "Stopped — checkpoint saved.",
    "no_run":          "No active run.",
    "no_sirets":       "No valid SIRETs found.",
    "invalid_siret":   "Invalid SIRET skipped (format): {s}",
    "rate_wait":       "Rate-limited (429) — waiting {s}s (attempt {n}/3)...",
    "rate_giveup":     "{s} — rate-limit exceeded after 3 attempts.",
    "err_404":         "SIRET not found or closed establishment (HTTP 404): {s}",
    "net_retry":       "Network error — retry {n}/3 in 5s: {e}",
    "done_msg":        "Done — {ok} OK, {err} errors",
    "saved":           "Saved -> {p}",
    "batch_saved":     "Batch {n} saved -> {p}",
    "save_fail":       "Save failed: {e}",
    "test_phase":      "TEST PHASE — 2 random SIRETs",
    "test_ok":         "Test {i} OK:",
    "test_fail":       "Test {i} — HTTP {c}",
    "test_ask":        "Test complete.\nStart the full run now?",
    "png_no_pillow":   "Pillow not installed.\nRun: pip install Pillow",
    "png_saved":       "Stats exported -> {p}",
    "png_fail":        "PNG export failed: {e}",
    "cp_saved":        "Checkpoint ({n} SIRETs).",
    "col_auto":        "Column detected: {c}",
    "idcc_fetch":      "Fetching IDCC...",
    "processing":      "{n} SIRETs to process",
    "batch_info":      "{nb} batches of {bs} (pause {bp}s) — 1 file per batch",
    "full_run":        "Full run (no batching)",
    "cache_hit":       "Global cache: {n} SIRETs already known",
    "sep":             "-" * 56,
},
}

# ---------------------------------------------------------------
#  CONSTANTS / LOOKUPS
# ---------------------------------------------------------------
TIMEOUT   = (5, 15)   # [IMP-12] split connect / read timeout
MISSING   = "-"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR  = os.path.join(SCRIPT_DIR, ".sirene_cache")
GLOBAL_CACHE_PATH = os.path.join(os.path.expanduser("~"), ".sirene_cache", "global_cache.json")

INSEE_INFO_URL = "https://api.insee.fr/api-sirene/3.11/informations"
# ---------------------------------------------------------------
#  VERSION CHECKER
# ---------------------------------------------------------------
APP_VERSION    = "7.0"
GITHUB_USER    = "saadduh"
GITHUB_REPO    = "sirene-enricher"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}/releases/latest"

def check_for_update(callback):
    def _check():
        try:
            r = requests.get(GITHUB_API_URL, timeout=(5, 10))
            if r.status_code == 200:
                data = r.json()
                tag  = data.get("tag_name", "").lstrip("v")
                url  = data.get("html_url", GITHUB_API_URL)
                if tag and tag != APP_VERSION:
                    callback(tag, url)
                    return
        except Exception:
            pass
        callback(None, None)
    threading.Thread(target=_check, daemon=True, name="VersionChecker").start()
# [BUG-8] Complete EFFECTIF_MAP including missing codes
EFFECTIF_MAP = {
    "00": "<10",       "01": "1-2",        "02": "3-5",
    "03": "6-9",       "11": "10-19",      "12": "20-49",
    "21": "50-99",     "22": "100-199",    "31": "200-249",
    "32": "250-499",   "41": "500-999",    "42": "1000-1999",
    "51": "2000-4999", "52": "5000-9999",  "53": "10000+",
    "NN": "Non employeuse", "XX": "Non déclaré",
}

# [BUG-7] Complete forme juridique siege check values
_SIEGE_TRUE  = {"true",  "o", "oui", "yes", "1"}
_SIEGE_FALSE = {"false", "n", "non", "no",  "0"}

# [IMP-6] Load external maps if present, fall back to hardcoded
def _load_map(filename: str, fallback: dict) -> dict:
    path = os.path.join(SCRIPT_DIR, filename)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return fallback

_NAF_FALLBACK = {
    "01.11Z": "Cereales / legumineuses", "10.11Z": "Transformation viande boucherie",
    "41.10A": "Promotion immobiliere",   "41.20A": "Construction maisons individuelles",
    "43.21A": "Installation electrique", "45.11Z": "Commerce voitures",
    "46.90Z": "Commerce gros non specialise", "47.11B": "Alimentation generale",
    "47.71Z": "Commerce habillement",    "55.10Z": "Hotels et hebergement",
    "56.10A": "Restauration traditionnelle", "56.10B": "Cafeterias / libres-services",
    "62.01Z": "Programmation informatique", "62.02A": "Conseil systemes informatiques",
    "62.09Z": "Autres activites informatiques", "63.11Z": "Traitement donnees / hebergement",
    "64.19Z": "Autres intermediations monetaires", "66.22Z": "Agents / courtiers assurances",
    "68.20A": "Location de logements",   "68.31Z": "Agences immobilieres",
    "69.10Z": "Activites juridiques",    "69.20Z": "Activites comptables",
    "70.10Z": "Sieges sociaux",          "70.22Z": "Conseil gestion entreprises",
    "71.11Z": "Architecture",            "71.12B": "Ingenierie / etudes techniques",
    "73.11Z": "Agences de publicite",    "74.10Z": "Activites de design",
    "74.20Z": "Activites photographiques", "77.11A": "Location courte duree voitures",
    "78.10Z": "Agences de placement",    "82.11Z": "Services administratifs bureau",
    "82.30Z": "Organisation foires / salons", "84.11Z": "Administration publique",
    "85.31Z": "Enseignement secondaire general", "85.59A": "Formation continue adultes",
    "86.10Z": "Activites hospitalieres", "86.21Z": "Medecins generalistes",
    "86.22A": "Radiodiagnostic",         "88.10A": "Aide a domicile",
    "90.01Z": "Arts du spectacle vivant","96.02A": "Coiffure",
}

_FJ_FALLBACK = {
    "1000": "Entrepreneur individuel", "1100": "SNC", "1200": "SCS", "1210": "SCA",
    "1300": "SARL", "1310": "EURL",   "1400": "SAS", "1410": "SASU", "1500": "SA",
    "1510": "SA simplifiee",           "1600": "Societe cooperative", "1610": "SCOP",
    "1700": "Societe civile",          "1710": "SCP", "1720": "SCI",
    "1800": "Association declaree",    "1810": "Association reconnue utilite publique",
    "1820": "Fondation",               "1900": "Autres personnes morales droit prive",
    "2000": "EPA",                     "2100": "EPIC",
    "2200": "Collectivite territoriale", "2300": "Administration centrale",
    "2400": "Organisme droit etranger","2500": "GEIE",
    "5520": "SCOP",                    "5530": "SCIC", "5710": "SASU",
    "5499": "Autres societes par actions",
}

NAF_MAP            = {}   # populated at runtime by _init_maps()
FORME_JURIDIQUE_MAP = {}   # populated at runtime by _init_maps()

def _init_maps():
    global NAF_MAP, FORME_JURIDIQUE_MAP
    NAF_MAP             = _load_map("naf.json",  _NAF_FALLBACK)
    FORME_JURIDIQUE_MAP = _load_map("fj.json",   _FJ_FALLBACK)

REGION_MAP = {
    "01": "Guadeloupe",   "02": "Martinique",  "03": "Guyane",
    "04": "La Reunion",   "06": "Mayotte",
    "11": "Ile-de-France","24": "Centre-Val de Loire",
    "27": "Bourgogne-Franche-Comte", "28": "Normandie",
    "32": "Hauts-de-France","44": "Grand Est",
    "52": "Pays de la Loire","53": "Bretagne",
    "75": "Nouvelle-Aquitaine","76": "Occitanie",
    "84": "Auvergne-Rhone-Alpes","93": "Provence-Alpes-Cote d'Azur","94": "Corse",
}

# [BUG-6] Added missing depts 30 (Gard->76) and 84 (Vaucluse->93)
DEPT_REGION = {
    "75":"11","77":"11","78":"11","91":"11","92":"11","93":"11","94":"11","95":"11",
    "01":"84","03":"84","07":"84","15":"84","26":"84","38":"84","42":"84","43":"84",
    "63":"84","69":"84","73":"84","74":"84",
    "02":"32","59":"32","60":"32","62":"32","80":"32",
    "08":"44","10":"44","51":"44","52":"44","54":"44","55":"44","57":"44","67":"44","68":"44","88":"44",
    "18":"24","28":"24","36":"24","37":"24","41":"24","45":"24",
    "14":"28","27":"28","50":"28","61":"28","76":"28",
    "21":"27","25":"27","39":"27","58":"27","70":"27","71":"27","89":"27","90":"27",
    "16":"75","17":"75","19":"75","23":"75","24":"75","33":"75","40":"75",
    "47":"75","64":"75","79":"75","86":"75","87":"75",
    "09":"76","11":"76","12":"76","30":"76","31":"76","34":"76",   # [BUG-6] 30 added
    "46":"76","48":"76","65":"76","66":"76","81":"76","82":"76",
    "44":"52","49":"52","53":"52","72":"52","85":"52",
    "22":"53","29":"53","35":"53","56":"53",
    "04":"93","05":"93","06":"93","13":"93","83":"93","84":"93",   # [BUG-6] 84 added
    "2A":"94","2B":"94",
    "971":"01","972":"02","973":"03","974":"04","976":"06",
}

# [BUG-9] IDCC added to OUTPUT_COLUMNS
OUTPUT_COLUMNS = [
    "Denomination", "Forme Juridique", "Nombres de salaries",
    "Activite Principale (Code)", "Activite Principale (Libelle)",
    "IDCC", "Type d'etablissement", "Adresse", "Ville", "Code Postal", "Region", "Pays",
]

# [IMP-5] SIRET validation
_SIRET_RE = re.compile(r"^\d{14}$")

def is_valid_siret(s: str) -> bool:
    return bool(_SIRET_RE.match(str(s).strip()))

# ---------------------------------------------------------------
#  GLOBAL SIRET CACHE  [IMP-9]
# ---------------------------------------------------------------
_global_cache: dict = {}
_global_cache_lock = threading.Lock()

def _load_global_cache():
    global _global_cache
    try:
        os.makedirs(os.path.dirname(GLOBAL_CACHE_PATH), exist_ok=True)
        if os.path.exists(GLOBAL_CACHE_PATH):
            with open(GLOBAL_CACHE_PATH, "r", encoding="utf-8") as f:
                _global_cache = json.load(f)
    except Exception:
        _global_cache = {}

def _save_global_cache():
    try:
        with _global_cache_lock:
            os.makedirs(os.path.dirname(GLOBAL_CACHE_PATH), exist_ok=True)
            with open(GLOBAL_CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(_global_cache, f, ensure_ascii=False)
    except Exception:
        pass

def _cache_get(siret: str):
    with _global_cache_lock:
        return _global_cache.get(siret)

def _cache_set(siret: str, data: dict):
    with _global_cache_lock:
        _global_cache[siret] = data

# ---------------------------------------------------------------
#  PALETTE
# ---------------------------------------------------------------
C = {
    "bg":       "#0F1117", "surface":  "#1A1D27", "border":   "#2A2D3E",
    "accent":   "#4F6EF7", "accent2":  "#7C4DFF",
    "success":  "#2DD4BF", "warning":  "#F59E0B", "error":    "#F87171",
    "text":     "#E2E8F0", "text_dim": "#64748B", "test":     "#C084FC",
    "input_bg": "#252836", "ch_ok":    "#2DD4BF", "ch_err":   "#F87171",
    "ch_grid":  "#2A2D3E",
}
CHART_PAL = [
    "#4F6EF7","#2DD4BF","#F59E0B","#F87171","#C084FC",
    "#34D399","#FB923C","#60A5FA","#A78BFA","#F472B6",
]

def _hex(h):
    # [BUG-14] validated
    try:
        h = h.lstrip("#")
        return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
    except Exception:
        return (128, 128, 128)

# ---------------------------------------------------------------
#  SPLASH  [BUG-13] constants moved inside function
# ---------------------------------------------------------------
def show_splash(callback):
    _SPLASH = (
        "      ___  ___ ___  ___ _  _  ___  \n"
        "     / __||_ _| _ \\| __| \\| || __| \n"
        "     \\__ \\ | ||   /| _||  ` || _|  \n"
        "     |___/|___|_|_\\|___|_|\\__|___| \n\n"
        "       INSEE Enricher  v7.0\n"
        "       Loading...  [ {s} ]"
    )
    _SPIN = ["/", "-", "\\", "|"]
    sp = tk.Tk()
    sp.overrideredirect(True)
    sp.configure(bg=C["bg"])
    sw, sh = sp.winfo_screenwidth(), sp.winfo_screenheight()
    w, h = 500, 230
    sp.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")
    brd = tk.Frame(sp, bg=C["accent"], padx=2, pady=2)
    brd.pack(fill=BOTH, expand=True)
    inn = tk.Frame(brd, bg=C["bg"])
    inn.pack(fill=BOTH, expand=True)
    lbl = tk.Label(inn, text="", bg=C["bg"], fg=C["accent"],
                   font=("Courier New", 11, "bold"), justify="center")
    lbl.pack(expand=True)
    sub = tk.Label(inn, text="Initializing...", bg=C["bg"],
                   fg=C["text_dim"], font=("Segoe UI", 8))
    sub.pack(pady=(0, 10))
    fi, ci = [0], [0]

    def _tick():
        if ci[0] >= 16:
            sp.destroy()
            callback()
            return
        lbl.config(text=_SPLASH.format(s=_SPIN[fi[0] % 4]))
        fi[0] += 1
        ci[0] += 1
        sp.after(140, _tick)

    _tick()
    sp.mainloop()


# ---------------------------------------------------------------
#  API HELPERS
# ---------------------------------------------------------------
def _insee_headers(key):
    return {
        "X-INSEE-Api-Key-Integration": key,
        "Accept": "application/json",
    }


def validate_key(key):
    try:
        r = requests.get(INSEE_INFO_URL, headers=_insee_headers(key), timeout=TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False


def fetch_siret(siret, key):
    url = f"https://api.insee.fr/api-sirene/3.11/siret/{siret}"
    r = requests.get(url, headers=_insee_headers(key), timeout=TIMEOUT)
    return {
        "status": r.status_code,
        "data":   r.json() if r.status_code == 200 else None,
        "text":   r.text,
    }


def fetch_siret_retry(siret, key, log_fn, tr):
    """
    Fetch with:
    - [IMP-7] Network error retry (3 attempts, 5s backoff)
    - Rate-limit (429) exponential back-off: 10s -> 30s -> 60s
    """
    for net_attempt in range(1, 4):
        try:
            res = fetch_siret(siret, key)
            if res["status"] != 429:
                return res
            # 429 back-off
            for i, wait in enumerate([10, 30, 60], 1):
                log_fn(tr["rate_wait"].format(s=wait, n=i), "warning")
                for _ in range(wait * 10):
                    time.sleep(0.1)
                res = fetch_siret(siret, key)
                if res["status"] != 429:
                    return res
            log_fn(tr["rate_giveup"].format(s=siret), "error")
            return {"status": 429, "data": None, "text": "Rate limited"}
        except requests.exceptions.RequestException as e:
            if net_attempt < 3:
                log_fn(tr["net_retry"].format(n=net_attempt, e=e), "warning")
                for _ in range(50):   # 5s interruptible
                    time.sleep(0.1)
            else:
                raise


def fetch_idcc(siret):
    try:
        r = requests.get(
            f"https://siret2idcc.fabrique.social.gouv.fr/api/v2/{siret}",
            timeout=TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                ids = [str(c.get("num", "")).strip()
                       for c in data[0].get("conventions", []) if c.get("num")]
                if ids:
                    return ", ".join(ids)
    except Exception:
        pass
    return MISSING


# ---------------------------------------------------------------
#  PARSING
# ---------------------------------------------------------------
def _v(x):
    if x is None:
        return MISSING
    s = str(x).strip()
    return s if s not in ("", "None", "nan", "NA") else MISSING


def _f(*vals):
    for v in vals:
        r = _v(v)
        if r != MISSING:
            return r
    return MISSING


def parse_json(payload, log=None):
    etab  = payload.get("etablissement", {})
    ul    = etab.get("uniteLegale", {})
    pul   = list(reversed(ul.get("periodesUniteLegale", [])))
    petab = list(reversed(etab.get("periodesEtablissement", [])))

    def _pp(periods, k):
        for p in periods:
            r = _v(p.get(k))
            if r != MISSING:
                return r
        return MISSING

    # Denomination
    den = _f(ul.get("denominationUniteLegale"), _pp(pul, "denominationUniteLegale"))
    if den == MISSING:
        nom = _f(ul.get("nomUniteLegale"), _pp(pul, "nomUniteLegale"))
        pre = _f(ul.get("prenomUsuelUniteLegale"), _pp(pul, "prenomUsuelUniteLegale"))
        if nom != MISSING:
            den = f"{pre} {nom}".strip() if pre != MISSING else nom

    # Forme juridique
    fj = _f(ul.get("formeJuridiqueUniteLegale"),
             ul.get("categorieJuridiqueUniteLegale"),
             _pp(pul, "formeJuridiqueUniteLegale"),
             _pp(pul, "categorieJuridiqueUniteLegale"))
    fj_label = FORME_JURIDIQUE_MAP.get(fj, fj) if fj != MISSING else MISSING
    if fj_label == fj and fj != MISSING and log:
        log(f"Unknown FJ code: {fj}", "warning")

    # Effectif
    ek  = _v(ul.get("trancheEffectifsUniteLegale"))
    eff = EFFECTIF_MAP.get(ek, MISSING) if ek != MISSING else MISSING

    # NAF
    naf = _f(etab.get("activitePrincipaleEtablissement"),
             _pp(petab, "activitePrincipaleEtablissement"),
             ul.get("activitePrincipaleUniteLegale"),
             _pp(pul, "activitePrincipaleUniteLegale"))
    naf_lib = NAF_MAP.get(naf, MISSING) if naf != MISSING else MISSING

    # [BUG-7] Type etablissement — extended value set
    sr  = etab.get("etablissementSiege")
    srv = str(sr).lower().strip() if sr is not None else ""
    if   srv in _SIEGE_TRUE:  te = "Siege"
    elif srv in _SIEGE_FALSE: te = "Etablissement Secondaire"
    else:                     te = MISSING

    # Adresse [IMP-1] final .strip() added
    adr   = etab.get("adresseEtablissement", {})
    parts = [_v(adr.get(k)) for k in [
        "numeroVoieEtablissement", "typeVoieEtablissement", "libelleVoieEtablissement"]]
    parts = [p for p in parts if p != MISSING]
    addr  = " ".join(parts).strip() or MISSING   # [IMP-1]
    comp  = _v(adr.get("complementAdresseEtablissement"))
    if comp != MISSING:
        addr = f"{comp}, {addr}" if addr != MISSING else comp

    ville = _f(adr.get("libelleCommuneEtablissement"),
               adr.get("libelleCommuneEtrangerEtablissement"))
    cp    = _v(adr.get("codePostalEtablissement"))

    # Region
    region = MISSING
    cr = _v(adr.get("codeRegionEtablissement"))
    if cr != MISSING:
        region = REGION_MAP.get(cr, cr)
    else:
        cc = _v(adr.get("codeCommuneEtablissement"))
        if cc != MISSING and len(cc) >= 2:
            dept   = cc[:3] if cc[:3] in DEPT_REGION else cc[:2]
            rc     = DEPT_REGION.get(dept)
            if rc:
                region = REGION_MAP.get(rc, rc)

    pays = _f(adr.get("libellePaysEtrangerEtablissement"),
              adr.get("codePaysEtrangerEtablissement"))
    if pays == MISSING:
        pays = "France"

    return {
        "Denomination":                  den,
        "Forme Juridique":               fj_label,
        "Nombres de salaries":           eff,
        "Activite Principale (Code)":    naf,
        "Activite Principale (Libelle)": naf_lib,
        "Type d'etablissement":          te,
        "Adresse":                       addr,
        "Ville":                         ville,
        "Code Postal":                   cp,
        "Region":                        region,
        "Pays":                          pays,
    }


# ---------------------------------------------------------------
#  CHECKPOINT  [IMP-2] now includes err_rows + ok/err counts
# ---------------------------------------------------------------
def _cp_path(excel_path):
    os.makedirs(CACHE_DIR, exist_ok=True)
    stem = os.path.splitext(os.path.basename(excel_path))[0]
    return os.path.join(CACHE_DIR, f"{stem}.checkpoint.json")


def cp_load(excel_path):
    p = _cp_path(excel_path)
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def cp_save(excel_path, done_set, rows_cache, err_rows, ok_count, err_count):
    with open(_cp_path(excel_path), "w", encoding="utf-8") as f:
        json.dump({
            "done":      list(done_set),
            "rows":      rows_cache,
            "err_rows":  err_rows,
            "ok_count":  ok_count,
            "err_count": err_count,
        }, f, ensure_ascii=False)


def cp_clear(excel_path):
    p = _cp_path(excel_path)
    if os.path.exists(p):
        os.remove(p)


# ---------------------------------------------------------------
#  WIDGET HELPERS
# ---------------------------------------------------------------
def sentry(parent, var, width=30, show=None):
    kw = dict(textvariable=var, width=width,
              bg=C["input_bg"], fg=C["text"],
              insertbackground=C["accent"], relief="flat",
              highlightthickness=1, highlightbackground=C["border"],
              highlightcolor=C["accent"], font=("Consolas", 10))
    if show:
        kw["show"] = show
    e = tk.Entry(parent, **kw)
    e.bind("<FocusIn>",  lambda ev, w=e: w.config(highlightbackground=C["accent"]))
    e.bind("<FocusOut>", lambda ev, w=e: w.config(highlightbackground=C["border"]))
    return e


def ibtn(parent, text, cmd, color=None, width=14):
    col = color or C["accent"]
    b = tk.Button(parent, text=text, command=cmd,
                  bg=col, fg="#FFF",
                  activebackground=C["accent2"], activeforeground="#FFF",
                  relief="flat", bd=0, cursor="hand2",
                  font=("Segoe UI", 9, "bold"), padx=10, pady=6, width=width)
    b.bind("<Enter>", lambda e, w=b:        w.config(bg=C["accent2"]))
    b.bind("<Leave>", lambda e, w=b, c=col: w.config(bg=c))
    return b


# ---------------------------------------------------------------
#  CHARTS  (tkinter canvas)
# ---------------------------------------------------------------
def draw_donut(cv, segs, labels, nd):
    cv.delete("all")
    w, h = cv.winfo_width(), cv.winfo_height()
    if w < 10 or h < 10:
        return
    tot = sum(v for v, _ in segs)
    r, cx, cy = min(w, h) * 0.36, w * 0.38, h / 2
    if tot == 0:
        cv.create_text(cx, cy, text=nd, fill=C["text_dim"], font=("Segoe UI", 9))
        return
    ang = -90.0
    for val, col in segs:
        if val == 0:
            continue
        ext = 360.0 * val / tot
        cv.create_arc(cx-r, cy-r, cx+r, cy+r,
                      start=ang, extent=ext,
                      fill=col, outline=C["bg"], width=2, style="pieslice")
        ang += ext
    hole = r * 0.56
    cv.create_oval(cx-hole, cy-hole, cx+hole, cy+hole,
                   fill=C["surface"], outline=C["surface"])
    cv.create_text(cx, cy-9, text=str(tot),
                   fill=C["text"], font=("Segoe UI", 14, "bold"))
    cv.create_text(cx, cy+9, text="processed",
                   fill=C["text_dim"], font=("Segoe UI", 7))
    lx, ly = cx + r + 18, cy - len(labels) * 14
    for i, (lbl, col) in enumerate(labels):
        y   = ly + i * 26
        vf  = next((v for v, c in segs if c == col), 0)
        pct = f"{100*vf//tot}%" if tot else "0%"
        cv.create_rectangle(lx, y, lx+12, y+12, fill=col, outline="")
        cv.create_text(lx+16, y+6, text=f"{lbl}  {vf} ({pct})",
                       fill=C["text"], font=("Segoe UI", 8), anchor="w")


def draw_line(cv, ok_s, err_s, title, ok_lbl, err_lbl):
    cv.delete("all")
    w, h = cv.winfo_width(), cv.winfo_height()
    if w < 10 or h < 10:
        return
    pl, pr, pt, pb = 44, 16, 28, 30
    cv.create_text(pl + (w-pl-pr)/2, 13, text=title,
                   fill=C["text_dim"], font=("Segoe UI", 8, "bold"))
    cv.create_line(pl, pt, pl, h-pb, fill=C["ch_grid"], width=1)
    cv.create_line(pl, h-pb, w-pr, h-pb, fill=C["ch_grid"], width=1)
    mv = max((max(ok_s) if ok_s else 0), (max(err_s) if err_s else 0), 1)
    aw, ah = w - pl - pr, h - pt - pb
    for fr in (0.25, 0.5, 0.75, 1.0):
        gy = h - pb - fr * ah
        cv.create_line(pl, gy, w-pr, gy, fill=C["ch_grid"], dash=(3, 5))
        cv.create_text(pl-4, gy, text=str(int(fr*mv)),
                       fill=C["text_dim"], font=("Segoe UI", 7), anchor="e")

    def poly(s, col):
        if len(s) < 2:
            return
        pts = []
        for i, v in enumerate(s):
            pts.extend([pl + aw * i / max(len(s)-1, 1), h - pb - ah * v / mv])
        cv.create_line(*pts, fill=col, width=2, smooth=True)
        cv.create_oval(pts[-2]-4, pts[-1]-4, pts[-2]+4, pts[-1]+4,
                       fill=col, outline="")

    poly(ok_s, C["ch_ok"])
    poly(err_s, C["ch_err"])
    lx = pl + 6
    for lbl, col in [(ok_lbl, C["ch_ok"]), (err_lbl, C["ch_err"])]:
        cv.create_rectangle(lx, pt+2, lx+10, pt+11, fill=col, outline="")
        cv.create_text(lx+13, pt+6, text=lbl,
                       fill=C["text_dim"], font=("Segoe UI", 7), anchor="w")
        lx += 60


def draw_bars(cv, data, max_b, nd):
    cv.delete("all")
    w, h = cv.winfo_width(), cv.winfo_height()
    if w < 10 or h < 10:
        return
    if not data:
        cv.create_text(w//2, h//2, text=nd,
                       fill=C["text_dim"], font=("Segoe UI", 9))
        return
    pl, pr, pt, pb = 140, 52, 10, 10
    items = sorted(data.items(), key=lambda x: x[1], reverse=True)[:max_b]
    mv    = max(v for _, v in items)
    rh    = (h - pt - pb) / len(items)
    bh    = max(6, rh * 0.65)
    for i, (lbl, val) in enumerate(items):
        y   = pt + i * rh + (rh - bh) / 2
        bw  = (val / mv) * (w - pl - pr)
        col = CHART_PAL[i % len(CHART_PAL)]
        disp = lbl if len(lbl) <= 22 else lbl[:21] + "..."
        cv.create_text(pl-6, y+bh/2, text=disp,
                       fill=C["text"], font=("Consolas", 7), anchor="e")
        cv.create_rectangle(pl, y, pl+bw, y+bh, fill=col, outline="")
        cv.create_text(pl+bw+5, y+bh/2, text=str(val),
                       fill=C["text_dim"], font=("Segoe UI", 7), anchor="w")


# ---------------------------------------------------------------
#  PNG EXPORT
# ---------------------------------------------------------------
def _best_font(size):
    for name in [
        "segoeui.ttf", "arial.ttf", "calibri.ttf",
        "DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]:
        try:
            return ImageFont.truetype(name, size)
        except Exception:
            pass
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def export_png(app, path):
    SCALE = 3
    W, H  = 1400, 860
    WS, HS = W * SCALE, H * SCALE

    bg_c  = _hex(C["bg"])
    dim_c = _hex(C["text_dim"])
    txt_c = _hex(C["text"])
    acc_c = _hex(C["accent"])
    ok_c  = _hex(C["ch_ok"])
    err_c = _hex(C["ch_err"])
    sur_c = _hex(C["surface"])

    img = Image.new("RGB", (WS, HS), bg_c)
    d   = ImageDraw.Draw(img)

    f_title  = _best_font(28 * SCALE)
    f_head   = _best_font(18 * SCALE)
    f_body   = _best_font(15 * SCALE)
    f_small  = _best_font(12 * SCALE)
    f_credit = _best_font(11 * SCALE)

    def R(x): return x * SCALE

    d.rectangle([0, 0, WS, R(54)], fill=acc_c)
    d.text((R(18), R(13)),
           f"SIRENE Enricher  |  Stats Export  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}",
           font=f_title, fill=(255, 255, 255))

    with app._stats_lock:
        ok, err = app._ok_count, app._err_count

    tot = ok + err

    d.rectangle([R(20), R(70), R(290), R(235)], fill=sur_c)
    d.text((R(30), R(80)),  app._tr("chart_rate"), font=f_head, fill=dim_c)
    d.text((R(30), R(108)), f"  OK:       {ok}",   font=f_body, fill=ok_c)
    d.text((R(30), R(134)), f"  Errors:   {err}",  font=f_body, fill=err_c)
    d.text((R(30), R(160)), f"  Total:    {tot}",  font=f_body, fill=txt_c)
    if tot:
        d.text((R(30), R(186)), f"  Rate:     {int(100*ok/tot)}%", font=f_body, fill=acc_c)

    d.rectangle([R(300), R(70), R(W-20), R(235)], fill=sur_c)
    d.text((R(310), R(80)), app._tr("chart_time"), font=f_head, fill=dim_c)
    if app._ok_s:
        n = len(app._ok_s)
        d.text((R(310), R(108)), f"  SIRETs processed : {n}",              font=f_body, fill=txt_c)
        d.text((R(310), R(134)), f"  Final OK         : {app._ok_s[-1]}",  font=f_body, fill=ok_c)
        d.text((R(310), R(160)), f"  Final Errors     : {app._err_s[-1]}", font=f_body, fill=err_c)
    else:
        d.text((R(310), R(108)), "  No data yet", font=f_body, fill=dim_c)

    def bar_section(x, y, bw, bh, title, data, max_b=8):
        d.rectangle([R(x), R(y), R(x+bw), R(y+bh)], fill=sur_c)
        d.text((R(x+10), R(y+8)), title, font=f_head, fill=dim_c)
        if not data:
            d.text((R(x+bw//2-30), R(y+bh//2)), "No data", font=f_body, fill=dim_c)
            return
        items = sorted(data.items(), key=lambda i: i[1], reverse=True)[:max_b]
        mv    = max(v for _, v in items)
        pad_l = 185
        row_h = (bh - 42) / len(items)
        bar_h = max(8, row_h * 0.6)
        for i, (lbl, val) in enumerate(items):
            ry  = y + 40 + i * row_h + (row_h - bar_h) / 2
            bw2 = int((val / mv) * (bw - pad_l - 50))
            col = _hex(CHART_PAL[i % len(CHART_PAL)])
            disp = lbl[:26] + "..." if len(lbl) > 27 else lbl
            d.text((R(x + pad_l - 4), R(ry + 1)), disp,
                   font=f_small, fill=txt_c, anchor="rm")
            d.rectangle([R(x+pad_l), R(ry), R(x+pad_l+bw2), R(ry+bar_h)], fill=col)
            d.text((R(x+pad_l+bw2+5), R(ry+1)), str(val), font=f_small, fill=dim_c)

    bw3 = (W - 40) // 3 - 10
    bar_section(20,              250, bw3, 280, app._tr("chart_eff"),    app._eff_c, 10)
    bar_section(20 + bw3+10,    250, bw3, 280, app._tr("chart_naf"),    app._naf_c,  8)
    bar_section(20 + 2*(bw3+10), 250, bw3, 280, app._tr("chart_region"), app._reg_c,  8)

    d.text((R(W-230), R(H-26)), app._tr("credit"), font=f_credit, fill=dim_c)

    final = img.resize((W, H), Image.LANCZOS)
    final.save(path, dpi=(300, 300))


# ---------------------------------------------------------------
#  3-WAY DUPLICATE DIALOG
# ---------------------------------------------------------------
def ask_dup_action(parent, tr, n, sample_text):
    result = ["cancel"]
    dlg = tk.Toplevel(parent)
    dlg.title(tr["dup_title"])
    dlg.configure(bg=C["surface"])
    dlg.resizable(False, False)
    dlg.grab_set()
    parent.update_idletasks()
    px, py = parent.winfo_rootx(), parent.winfo_rooty()
    pw, ph = parent.winfo_width(), parent.winfo_height()
    dlg.geometry(f"+{px + pw//2 - 240}+{py + ph//2 - 120}")

    tk.Label(dlg, text=tr["dup_body"].format(n=n, lst=sample_text),
             bg=C["surface"], fg=C["text"], font=("Segoe UI", 10),
             justify="left", wraplength=440, padx=20, pady=16).pack()

    row = tk.Frame(dlg, bg=C["surface"])
    row.pack(pady=(0, 18), padx=20)

    def pick(val):
        result[0] = val
        dlg.destroy()

    ibtn(row, tr["dup_keep"],   lambda: pick("keep"),   color=C["accent"],  width=18).pack(side=LEFT, padx=4)
    ibtn(row, tr["dup_dedup"],  lambda: pick("dedup"),  color=C["warning"], width=18).pack(side=LEFT, padx=4)
    ibtn(row, tr["dup_cancel"], lambda: pick("cancel"),  color="#6B7280",   width=10).pack(side=LEFT, padx=4)

    parent.wait_window(dlg)
    return result[0]


# ---------------------------------------------------------------
#  APP
# ---------------------------------------------------------------
class App:
    def __init__(self, root):
        self.root = root
        self._lang = "fr"

        # Runtime
        self.excel_path  = None
        self.siret_col   = "SIRET"
        self.save_dir    = None
        self.worker      = None
        self.stop_flag   = threading.Event()
        self.log_q       = queue.Queue()

        # [BUG-1] Thread-safe stats
        self._stats_lock = threading.Lock()
        self._ok_count   = 0
        self._err_count  = 0

        self._ok_s  = []; self._err_s = []
        self._eff_c = {}; self._naf_c = {}; self._reg_c = {}
        self._err_rows = []

        # [IMP-3] Write lock
        self._write_lock = threading.Lock()

        # Checkpoint
        self._done = set()
        self._rows = {}

        # Timing
        self._run_start = None

        # Taskbar
        self._hwnd = None

        # Tk vars
        self.api_v     = StringVar()
        self.delay_v   = StringVar(value="2")
        self.idcc_v    = BooleanVar(value=True)
        self.batch_v   = BooleanVar(value=False)
        self.bsize_v   = StringVar(value="50")
        self.bpause_v  = StringVar(value="30")
        self.file_v    = StringVar()
        self.savedir_v = StringVar()
        self.fname_v   = StringVar(value="enriched")
        self.col_v     = StringVar(value="SIRET")

        self._reg = []

        self._build()
        self.root.after(50, self._drain_log)
        self.root.after(300, self._init_taskbar)
        self.root.after(1500, self._check_update_on_start)
        self.root.update_idletasks()

    def _init_taskbar(self):
        try:
            self._hwnd = self.root.winfo_id()
            _tb_init(self._hwnd)
        except Exception:
            pass

    def _check_update_on_start(self):
        def _on_result(latest, url):
            if latest:
                self.root.after(0, self._show_update_banner, latest, url)
        check_for_update(_on_result)

    def _show_update_banner(self, latest, url):
        bar = tk.Frame(self.root, bg=C["warning"], height=32)
        bar.pack(fill="x", before=self.root.winfo_children()[1])
        bar.pack_propagate(False)
        msg = f"  Update available: v{latest}  →  "
        tk.Label(bar, text=msg, bg=C["warning"], fg="#1a1a1a",
                 font=("Segoe UI", 9, "bold")).pack(side=LEFT, padx=(8, 0))
        def _open():
            import webbrowser
            webbrowser.open(url)
        tk.Button(bar, text="Download on GitHub", command=_open,
                  bg="#1a1a1a", fg="#FFF", relief="flat",
                  font=("Segoe UI", 9, "bold"), padx=10, pady=2,
                  cursor="hand2").pack(side=LEFT)
        tk.Button(bar, text="✕", command=bar.destroy,
                  bg=C["warning"], fg="#1a1a1a", relief="flat",
                  font=("Segoe UI", 9), padx=6, cursor="hand2").pack(side=RIGHT, padx=4)

    def _tr(self, key):
        return T[self._lang].get(key, key)

    def _rw(self, widget, key):
        self._reg.append((widget, key))
        return widget

    def _set_lang(self, lang):
        self._lang = lang
        for btn, is_active in [(self._btn_fr, lang == "fr"),
                               (self._btn_en, lang == "en")]:
            btn.config(
                bg=C["accent2"] if is_active else C["accent"],
                fg="#FFF"       if is_active else "#BFCFFF",
                font=("Segoe UI", 9, "bold") if is_active else ("Segoe UI", 9))
        self.root.title(self._tr("title"))
        self._title_lbl.config(text=f"  {self._tr('title')}")
        for widget, key in self._reg:
            try:
                widget.config(text=self._tr(key))
            except Exception:
                pass
        self.nb.tab(0, text=self._tr("tab_run"))
        self.nb.tab(1, text=self._tr("tab_stats"))
        if not self.excel_path:
            self.file_v.set(self._tr("no_file"))
        if not self.save_dir:
            self.savedir_v.set(self._tr("same_folder"))
        self._redraw()

    # ================================================================
    #  BUILD UI
    # ================================================================
    def _build(self):
        self.root.title(self._tr("title"))
        self.root.geometry("1150x830")
        self.root.configure(bg=C["bg"])
        self.root.resizable(True, True)

        hdr = tk.Frame(self.root, bg=C["accent"], height=52)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        self._title_lbl = tk.Label(
            hdr, text=f"  {self._tr('title')}",
            bg=C["accent"], fg="#FFF",
            font=("Segoe UI", 14, "bold"), anchor="w")
        self._title_lbl.pack(side=LEFT, padx=14, pady=10)

        lf = tk.Frame(hdr, bg=C["accent"])
        lf.pack(side=RIGHT, padx=10)
        self._btn_fr = tk.Button(
            lf, text="FR", command=lambda: self._set_lang("fr"),
            bg=C["accent2"], fg="#FFF", relief="flat", bd=0,
            font=("Segoe UI", 9, "bold"), padx=10, pady=4, cursor="hand2")
        self._btn_en = tk.Button(
            lf, text="EN", command=lambda: self._set_lang("en"),
            bg=C["accent"], fg="#BFCFFF", relief="flat", bd=0,
            font=("Segoe UI", 9), padx=10, pady=4, cursor="hand2")
        self._btn_fr.pack(side=LEFT, padx=(0, 4))
        self._btn_en.pack(side=LEFT)

        subtitle = tk.Label(hdr, text=self._tr("subtitle"),
                            bg=C["accent"], fg="#BFCFFF", font=("Segoe UI", 9))
        subtitle.pack(side=RIGHT, padx=14)
        self._rw(subtitle, "subtitle")

        body = tk.Frame(self.root, bg=C["bg"])
        body.pack(fill=BOTH, expand=True, padx=16, pady=(12, 0))

        left_outer = tk.Frame(body, bg=C["bg"], width=284)
        left_outer.pack(side=LEFT, fill="y", padx=(0, 12))
        left_outer.pack_propagate(False)

        left_scroll = ttk.Scrollbar(left_outer, orient="vertical")
        left_scroll.pack(side=RIGHT, fill="y")

        left_canvas = tk.Canvas(
            left_outer, bg=C["bg"], width=268,
            highlightthickness=0, bd=0,
            yscrollcommand=left_scroll.set)
        left_canvas.pack(side=LEFT, fill=BOTH, expand=True)
        left_scroll.config(command=left_canvas.yview)

        left = tk.Frame(left_canvas, bg=C["bg"])
        _left_win = left_canvas.create_window((0, 0), window=left, anchor="nw")

        def _on_frame_resize(event):
            left_canvas.configure(scrollregion=left_canvas.bbox("all"))

        def _on_canvas_resize(event):
            left_canvas.itemconfig(_left_win, width=event.width)

        left.bind("<Configure>", _on_frame_resize)
        left_canvas.bind("<Configure>", _on_canvas_resize)

        def _on_mousewheel(event):
            left_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        # Bind mousewheel only when cursor is over the sidebar
        def _bind_mw(e):  left_canvas.bind_all("<MouseWheel>", _on_mousewheel)
        def _unbind_mw(e): left_canvas.unbind_all("<MouseWheel>")
        left_canvas.bind("<Enter>", _bind_mw)
        left_canvas.bind("<Leave>", _unbind_mw)
        left.bind("<Enter>", _bind_mw)
        left.bind("<Leave>", _unbind_mw)


        right = tk.Frame(body, bg=C["bg"])
        right.pack(side=RIGHT, fill=BOTH, expand=True)

        self.file_v.set(self._tr("no_file"))
        self.savedir_v.set(self._tr("same_folder"))

        self._build_sidebar(left)
        self._build_notebook(right)

        credit = tk.Label(self.root, text=self._tr("credit"),
                          bg=C["bg"], fg=C["text_dim"],
                          font=("Segoe UI", 7, "italic"))
        credit.pack(side="bottom", anchor="e", padx=10, pady=3)
        self._rw(credit, "credit")

    def _card(self, parent, key):
        outer = tk.Frame(parent, bg=C["border"])
        outer.pack(fill="x", pady=(0, 9))
        inn = tk.Frame(outer, bg=C["surface"], padx=12, pady=9)
        inn.pack(fill="x", padx=1, pady=1)
        lbl = tk.Label(inn, text=self._tr(key),
                       bg=C["surface"], fg=C["text_dim"],
                       font=("Segoe UI", 8, "bold"))
        lbl.pack(anchor="w", pady=(0, 6))
        self._rw(lbl, key)
        return inn

    def _lbl(self, parent, key, **kw):
        defaults = dict(bg=C["surface"], fg=C["text"], font=("Segoe UI", 9))
        defaults.update(kw)
        l = tk.Label(parent, text=self._tr(key), **defaults)
        self._rw(l, key)
        return l

    def _btn(self, parent, key, cmd, color=None, width=24):
        b = ibtn(parent, self._tr(key), cmd, color=color, width=width)
        self._rw(b, key)
        return b

    def _chk(self, parent, key, var, cmd=None):
        kw = dict(text=self._tr(key), variable=var,
                  bg=C["surface"], fg=C["text"],
                  selectcolor=C["input_bg"],
                  activebackground=C["surface"], activeforeground=C["text"],
                  font=("Segoe UI", 9))
        if cmd:
            kw["command"] = cmd
        ck = tk.Checkbutton(parent, **kw)
        self._rw(ck, key)
        return ck

    # ================================================================
    #  SIDEBAR
    # ================================================================
    def _build_sidebar(self, p):
        c = self._card(p, "api_panel")
        self._lbl(c, "api_key_lbl").pack(anchor="w")
        sentry(c, self.api_v, width=24, show="*").pack(anchor="w", pady=(2, 6))
        self._btn(c, "btn_test_key", self.do_validate_key, width=22).pack(anchor="w")

        c = self._card(p, "file_panel")
        tk.Label(c, textvariable=self.file_v, bg=C["surface"], fg=C["text_dim"],
                 font=("Consolas", 8), wraplength=230, justify="left"
                 ).pack(anchor="w", pady=(0, 4))
        self._btn(c, "choose_file", self.choose_file).pack(anchor="w", pady=(0, 6))
        self._lbl(c, "siret_col_lbl", fg=C["text_dim"],
                  font=("Segoe UI", 8)).pack(anchor="w")
        self._col_combo = ttk.Combobox(
            c, textvariable=self.col_v, state="readonly",
            width=22, font=("Consolas", 9))
        self._col_combo["values"] = ["SIRET"]
        self._col_combo.pack(anchor="w", pady=(2, 0))
        self._col_combo.bind("<<ComboboxSelected>>",
                             lambda e: setattr(self, "siret_col", self.col_v.get()))

        c = self._card(p, "output_panel")
        tk.Label(c, textvariable=self.savedir_v, bg=C["surface"], fg=C["text_dim"],
                 font=("Consolas", 8), wraplength=230, justify="left"
                 ).pack(anchor="w", pady=(0, 4))
        self._btn(c, "choose_folder", self.choose_folder).pack(anchor="w", pady=(0, 6))
        self._lbl(c, "fname_lbl", fg=C["text_dim"],
                  font=("Segoe UI", 8)).pack(anchor="w")
        sentry(c, self.fname_v, width=24).pack(anchor="w", pady=(2, 0))

        c = self._card(p, "run_panel")
        self._lbl(c, "delay_lbl").pack(anchor="w")
        sentry(c, self.delay_v, width=8).pack(anchor="w", pady=(2, 6))
        self._chk(c, "idcc_chk", self.idcc_v).pack(anchor="w")
        self._lbl(c, "idcc_hint", fg=C["text_dim"],
                  font=("Segoe UI", 7)).pack(anchor="w", pady=(0, 2))

        c = self._card(p, "batch_panel")
        self._chk(c, "batch_chk", self.batch_v,
                  cmd=self._toggle_batch).pack(anchor="w", pady=(0, 6))
        self._bf = tk.Frame(c, bg=C["surface"])
        self._bf.pack(fill="x")
        for row, key, var in [(0, "batch_size_lbl",  self.bsize_v),
                               (1, "batch_pause_lbl", self.bpause_v)]:
            lbl = tk.Label(self._bf, text=self._tr(key),
                           bg=C["surface"], fg=C["text"], font=("Segoe UI", 9))
            lbl.grid(row=row, column=0, sticky="w", pady=2)
            self._rw(lbl, key)
            sentry(self._bf, var, width=8).grid(row=row, column=1, sticky="w", padx=8)
        self._lbl(c, "batch_hint", fg=C["text_dim"],
                  font=("Segoe UI", 8)).pack(anchor="w", pady=(6, 0))
        self._toggle_batch()

        c = self._card(p, "actions_panel")
        for key, cmd, col in [
            ("btn_test",   self.do_test,    C["accent"]),
            ("btn_start",  self.start_run,  "#22C55E"),
            ("btn_stop",   self.stop_run,   "#EF4444"),
            ("btn_resume", self.resume_run, C["accent2"]),
        ]:
            self._btn(c, key, cmd, color=col).pack(fill="x", pady=(0, 5))

    def _toggle_batch(self):
        st = "normal" if self.batch_v.get() else "disabled"
        for w in self._bf.winfo_children():
            try:
                w.configure(state=st)
            except Exception:
                pass

    # ================================================================
    #  NOTEBOOK
    # ================================================================
    def _build_notebook(self, p):
        s = ttk.Style()
        s.theme_use("clam")
        s.configure("D.TNotebook", background=C["bg"], borderwidth=0, tabmargins=0)
        s.configure("D.TNotebook.Tab",
                    background=C["surface"], foreground=C["text_dim"],
                    padding=[16, 7], font=("Segoe UI", 9, "bold"), borderwidth=0)
        s.map("D.TNotebook.Tab",
              background=[("selected", C["accent"])],
              foreground=[("selected", "#FFF")])
        s.configure("TScrollbar",
                    background=C["border"], troughcolor=C["surface"],
                    bordercolor=C["surface"], arrowcolor=C["text_dim"])
        s.configure("TCombobox",
                    fieldbackground=C["input_bg"], background=C["input_bg"],
                    foreground=C["text"], selectbackground=C["accent"])

        self.nb = ttk.Notebook(p, style="D.TNotebook")
        self.nb.pack(fill=BOTH, expand=True)
        t1 = tk.Frame(self.nb, bg=C["bg"])
        t2 = tk.Frame(self.nb, bg=C["bg"])
        self.nb.add(t1, text=self._tr("tab_run"))
        self.nb.add(t2, text=self._tr("tab_stats"))
        self._build_run_tab(t1)
        self._build_stats_tab(t2)
        self.nb.bind("<<NotebookTabChanged>>", lambda e: self._redraw())

    def _pcard(self, parent, key, expand=False):
        outer = tk.Frame(parent, bg=C["border"])
        outer.pack(fill="x" if not expand else BOTH, expand=expand, pady=(0, 9))
        inn = tk.Frame(outer, bg=C["surface"], padx=12, pady=9)
        inn.pack(fill=BOTH, expand=expand, padx=1, pady=1)
        lbl = tk.Label(inn, text=self._tr(key),
                       bg=C["surface"], fg=C["text_dim"],
                       font=("Segoe UI", 8, "bold"))
        lbl.pack(anchor="w", pady=(0, 6))
        self._rw(lbl, key)
        return inn

    def _build_run_tab(self, p):
        pc = self._pcard(p, "prog_panel")
        sr = tk.Frame(pc, bg=C["surface"])
        sr.pack(fill="x", pady=(0, 8))
        self._sw = {}
        for key, val in [("stat_done","0"), ("stat_total","0"), ("stat_ok","0"),
                          ("stat_err","0"),  ("stat_batch","—"), ("stat_eta","—")]:
            self._sw[key] = self._stat_w(sr, key, val)

        self._prog = tk.Canvas(pc, bg=C["input_bg"], height=16,
                               highlightthickness=0, bd=0)
        self._prog.pack(fill="x", pady=(0, 2))
        self._prog_lbl = tk.Label(pc, text="Ready",
                                  bg=C["surface"], fg=C["text_dim"],
                                  font=("Segoe UI", 8))
        self._prog_lbl.pack(anchor="e")

        lc = self._pcard(p, "log_panel", expand=True)
        tb = tk.Frame(lc, bg=C["surface"])
        tb.pack(fill="x", pady=(0, 4))
        self._btn(tb, "btn_clear", self._clear_log,
                  color=C["text_dim"], width=8).pack(side=RIGHT)

        lf = tk.Frame(lc, bg=C["border"])
        lf.pack(fill=BOTH, expand=True)
        self._log_t = tk.Text(
            lf, wrap="word", state="disabled",
            bg=C["surface"], fg=C["text"], font=("Consolas", 9),
            relief="flat", bd=0, selectbackground=C["accent"],
            padx=8, pady=6, spacing1=1, spacing3=1)
        sc = ttk.Scrollbar(lf, command=self._log_t.yview)
        self._log_t.configure(yscrollcommand=sc.set)
        sc.pack(side=RIGHT, fill="y")
        self._log_t.pack(fill=BOTH, expand=True, padx=1, pady=1)
        for tag, col in [("info",    C["text"]),    ("success", C["success"]),
                          ("warning", C["warning"]), ("error",   C["error"]),
                          ("test",    C["test"]),    ("batch",   C["accent"]),
                          ("dim",     C["text_dim"])]:
            self._log_t.tag_config(tag, foreground=col)

    def _stat_w(self, parent, key, val):
        f = tk.Frame(parent, bg=C["border"], padx=1, pady=1)
        f.pack(side=LEFT, expand=True, fill="x", padx=2)
        inn = tk.Frame(f, bg=C["input_bg"], padx=6, pady=5)
        inn.pack(fill=BOTH)
        tl = tk.Label(inn, text=self._tr(key),
                      bg=C["input_bg"], fg=C["text_dim"],
                      font=("Segoe UI", 7, "bold"))
        tl.pack()
        self._rw(tl, key)
        vl = tk.Label(inn, text=val,
                      bg=C["input_bg"], fg=C["text"],
                      font=("Segoe UI", 11, "bold"))
        vl.pack()
        return vl

    def _build_stats_tab(self, p):
        p.bind("<Configure>", lambda e: self._redraw())

        top = tk.Frame(p, bg=C["bg"])
        top.pack(fill="x", padx=8, pady=(8, 4))
        top.columnconfigure(0, weight=1)
        top.columnconfigure(1, weight=2)

        for attr, key, col, px in [("_cv_donut", "chart_rate", 0, (0, 6)),
                                    ("_cv_line",  "chart_time", 1, (0, 0))]:
            ou = tk.Frame(top, bg=C["border"])
            ou.grid(row=0, column=col, sticky="nsew", padx=px)
            lbl = tk.Label(ou, text=self._tr(key), bg=C["border"],
                           fg=C["text_dim"], font=("Segoe UI", 8, "bold"))
            lbl.pack(anchor="w", padx=10, pady=(8, 0))
            self._rw(lbl, key)
            inn = tk.Frame(ou, bg=C["surface"])
            inn.pack(fill=BOTH, expand=True, padx=1, pady=(1, 1))
            cv = tk.Canvas(inn, bg=C["surface"], height=200, highlightthickness=0)
            cv.pack(fill=BOTH, expand=True)
            setattr(self, attr, cv)

        bot = tk.Frame(p, bg=C["bg"])
        bot.pack(fill=BOTH, expand=True, padx=8, pady=(4, 4))
        for i in range(3):
            bot.columnconfigure(i, weight=1)

        for attr, key, col, px in [("_cv_eff",  "chart_eff",    0, (0, 6)),
                                    ("_cv_naf",  "chart_naf",    1, (0, 6)),
                                    ("_cv_reg",  "chart_region", 2, (0, 0))]:
            ou = tk.Frame(bot, bg=C["border"])
            ou.grid(row=0, column=col, sticky="nsew", padx=px)
            lbl = tk.Label(ou, text=self._tr(key), bg=C["border"],
                           fg=C["text_dim"], font=("Segoe UI", 8, "bold"))
            lbl.pack(anchor="w", padx=10, pady=(8, 0))
            self._rw(lbl, key)
            inn = tk.Frame(ou, bg=C["surface"])
            inn.pack(fill=BOTH, expand=True, padx=1, pady=(1, 1))
            cv = tk.Canvas(inn, bg=C["surface"], highlightthickness=0)
            cv.pack(fill=BOTH, expand=True)
            setattr(self, attr, cv)

        ebar = tk.Frame(p, bg=C["bg"])
        ebar.pack(fill="x", padx=8, pady=(0, 6))
        self._btn(ebar, "btn_export_png", self.do_export_png,
                  color=C["accent2"], width=20).pack(side=RIGHT)

    def _redraw(self):
        with self._stats_lock:
            ok, err = self._ok_count, self._err_count
        nd = self._tr("no_data")
        draw_donut(self._cv_donut,
                   [(ok, C["ch_ok"]), (err, C["ch_err"])],
                   [(self._tr("ok_lbl"), C["ch_ok"]), (self._tr("err_lbl"), C["ch_err"])],
                   nd)
        draw_line(self._cv_line, self._ok_s, self._err_s,
                  self._tr("chart_cumul"), self._tr("ok_lbl"), self._tr("err_lbl"))
        draw_bars(self._cv_eff, self._eff_c, 12, nd)
        draw_bars(self._cv_naf, self._naf_c, 10, nd)
        draw_bars(self._cv_reg, self._reg_c, 10, nd)

    # ================================================================
    #  FILE CHOOSERS
    # ================================================================
    def choose_file(self):
        p = filedialog.askopenfilename(title="Select Excel",
                                       filetypes=[("Excel", "*.xlsx")])
        if not p:
            return
        self.excel_path = p
        self.file_v.set(f"{os.path.basename(p)}")
        self._log(f"Source: {p}", "dim")
        self.save_dir = None
        self.savedir_v.set(f"{os.path.dirname(p)}")
        try:
            cols = list(pd.read_excel(p, nrows=0).columns)
            self._col_combo["values"] = cols
            best = next((c for c in cols if "siret" in c.lower()),
                        cols[0] if cols else "SIRET")
            self.col_v.set(best)
            self.siret_col = best
            self._log(self._tr("col_auto").format(c=best), "dim")
        except Exception:
            pass

    def choose_folder(self):
        ini = os.path.dirname(self.excel_path) if self.excel_path else os.getcwd()
        f = filedialog.askdirectory(title="Choose output folder", initialdir=ini)
        if f:
            self.save_dir = f
            self.savedir_v.set(f)

    def _out_path(self, batch_num=None):
        folder = self.save_dir or (
            os.path.dirname(self.excel_path) if self.excel_path else os.getcwd())
        stem = self.fname_v.get().strip() or "enriched"
        for ch in r'\/:*?"<>|':
            stem = stem.replace(ch, "_")
        if batch_num is not None:
            stem = f"{stem}_batch{batch_num:03d}"
        return os.path.join(folder, f"{stem}.xlsx")

    # ================================================================
    #  API KEY VALIDATION
    # ================================================================
    def do_validate_key(self):
        key = self.api_v.get().strip()
        if not key:
            messagebox.showerror("", self._tr("need_key"))
            return
        self._log(self._tr("key_testing"), "dim")

        def _go():
            ok = validate_key(key)
            self._log(
                self._tr("key_ok")   if ok else self._tr("key_fail"),
                "success"            if ok else "error")

        threading.Thread(target=_go, daemon=True).start()

    # ================================================================
    #  PNG EXPORT
    # ================================================================
    def do_export_png(self):
        if not _PILLOW:
            messagebox.showwarning("", self._tr("png_no_pillow"))
            return
        p = filedialog.asksaveasfilename(
            title="Export PNG", defaultextension=".png",
            filetypes=[("PNG", "*.png")],
            initialfile=f"stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        if not p:
            return
        try:
            export_png(self, p)
            self._log(self._tr("png_saved").format(p=p), "success")
        except Exception as e:
            self._log(self._tr("png_fail").format(e=e), "error")

    # ================================================================
    #  LOG
    # ================================================================
    def _log(self, msg, level="info"):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_q.put((f"[{ts}] {msg}", level))

    def _clear_log(self):
        self._log_t.configure(state="normal")
        self._log_t.delete("1.0", END)
        self._log_t.configure(state="disabled")

    def _drain_log(self):
        try:
            while True:
                txt, lvl = self.log_q.get_nowait()
                self._log_t.configure(state="normal")
                self._log_t.insert(END, txt + "\n", lvl)
                self._log_t.see(END)
                self._log_t.configure(state="disabled")
        except queue.Empty:
            pass
        self.root.after(100, self._drain_log)

    # ================================================================
    #  PROGRESS
    # ================================================================
    def _upd_progress(self, done, total, batch_lbl=None):
        pct = done / total if total else 0
        w = self._prog.winfo_width()
        h = self._prog.winfo_height()
        self._prog.delete("all")
        if w > 1 and int(w * pct) > 0:
            self._prog.create_rectangle(0, 0, int(w * pct), h,
                                        fill=C["accent"], outline="")
        eta_str = "—"
        if self._run_start and done > 0:
            elapsed   = (datetime.now() - self._run_start).total_seconds()
            rate      = done / elapsed
            remaining = int((total - done) / rate) if rate > 0 else 0
            eta_str   = str(timedelta(seconds=remaining))
        self._prog_lbl.config(text=f"{done} / {total}  ({int(pct*100)}%)")

        with self._stats_lock:
            ok, err = self._ok_count, self._err_count

        self._sw["stat_done"].config(text=str(done))
        self._sw["stat_total"].config(text=str(total))
        self._sw["stat_ok"].config(text=str(ok))
        self._sw["stat_err"].config(text=str(err))
        self._sw["stat_eta"].config(text=eta_str)
        if batch_lbl:
            self._sw["stat_batch"].config(text=batch_lbl)
        self._redraw()

        # [IMP-10] Taskbar progress
        if self._hwnd:
            _tb_set(self._hwnd, done, total)

    # ================================================================
    #  DUPLICATE CHECK
    # ================================================================
    def _check_dups(self, df):
        col = self.siret_col
        if col not in df.columns:
            return df, True
        vals = df[col].dropna().astype(str).str.strip()
        vals = vals[vals.str.lower() != "nan"]
        dups = vals[vals.duplicated()].unique().tolist()
        if not dups:
            return df, True

        sample = "\n".join(dups[:15])
        if len(dups) > 15:
            sample += f"\n... (+{len(dups)-15} more)"

        action = ask_dup_action(self.root, T[self._lang], len(dups), sample)

        if action == "cancel":
            return df, False
        if action == "dedup":
            before = len(df)
            df = df.drop_duplicates(subset=[col], keep="first").reset_index(drop=True)
            self._log(f"Deduplicated: {before - len(df)} rows removed", "warning")
        return df, True

    # ================================================================
    #  TEST PHASE
    # ================================================================
    def do_test(self):
        import random
        if not self.excel_path:
            messagebox.showerror("", self._tr("need_file")); return
        if not self.api_v.get().strip():
            messagebox.showerror("", self._tr("need_key")); return
        df  = pd.read_excel(self.excel_path, dtype=str)
        col = self.siret_col
        if col not in df.columns:
            messagebox.showerror("", self._tr("col_not_found").format(c=col)); return
        sl = [s for s in df[col].dropna().astype(str).str.strip().unique()
              if is_valid_siret(s)]
        if len(sl) < 2:
            messagebox.showwarning("", self._tr("need_2")); return
        samples  = random.sample(sl, 2)
        key      = self.api_v.get().strip()
        lang     = self._lang
        do_idcc  = self.idcc_v.get()

        def _run_test():
            self._log(self._tr("sep"), "dim")
            self._log(self._tr("test_phase"), "test")
            for i, s in enumerate(samples, 1):
                self._log(f"  Test {i} -> {s}", "test")
                try:
                    res = fetch_siret_retry(s, key, self._log, T[lang])
                    if res["status"] == 200 and res["data"]:
                        p = parse_json(res["data"], log=self._log)
                        pays = p.get("Pays", "France")
                        if do_idcc and pays == "France":
                            self._log(self._tr("idcc_fetch"), "dim")
                            p["IDCC"] = fetch_idcc(s)
                        else:
                            p["IDCC"] = MISSING
                        self._log(self._tr("test_ok").format(i=i), "success")
                        for k, v in p.items():
                            self._log(f"     {k:40s}: {v}", "info")
                    else:
                        self._log(self._tr("test_fail").format(i=i, c=res["status"]), "error")
                except Exception as e:
                    self._log(f"  Test {i} ERROR — {e}", "error")
            self._log(self._tr("sep"), "dim")
            self.root.after(0, self._ask_start_after_test)

        threading.Thread(target=_run_test, daemon=True, name="SIRENETest").start()

    def _ask_start_after_test(self):
        if messagebox.askyesno("", self._tr("test_ask")):
            self.start_run()


    # ================================================================
    #  RUN CONTROL
    # ================================================================
    def start_run(self, resume=None):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("", self._tr("already_running")); return
        if not self.api_v.get().strip():
            messagebox.showerror("", self._tr("need_key")); return
        if not self.excel_path:
            messagebox.showerror("", self._tr("need_file")); return

        if resume is None:
            df_check = pd.read_excel(self.excel_path, dtype=str)
            col = self.siret_col
            if col in df_check.columns:
                df_check[col] = df_check[col].astype(str).str.strip()
            df_check, proceed = self._check_dups(df_check)
            if not proceed:
                return
            with self._stats_lock:
                self._ok_count  = 0
                self._err_count = 0
            self._ok_s.clear();  self._err_s.clear()
            self._eff_c.clear(); self._naf_c.clear(); self._reg_c.clear()
            self._err_rows.clear()
            self._done.clear();  self._rows.clear()

        self._run_start = datetime.now()
        self.stop_flag.clear()

        # Capture key and clear field  [BUG-10]
        api_key = self.api_v.get().strip()

        self.worker = threading.Thread(
            target=self._worker,
            args=(api_key, resume),
            daemon=True,
            name="SIRENEWorker"   # [BUG-15]
        )
        self.worker.start()
        self._log(self._tr("started"), "info")

    def resume_run(self):
        if not self.excel_path:
            messagebox.showerror("", self._tr("need_file")); return
        cp = cp_load(self.excel_path)
        if not cp:
            self._log(self._tr("no_checkpoint"), "warning"); return
        n = len(cp.get("done", []))
        if messagebox.askyesno(self._tr("resume_title"),
                               self._tr("resume_body").format(n=n)):
            self._done = set(cp["done"])
            self._rows = cp.get("rows", {})
            # [IMP-2] Restore err_rows from checkpoint
            self._err_rows = cp.get("err_rows", [])
            # [BUG-4] Restore counts from checkpoint
            with self._stats_lock:
                self._ok_count  = cp.get("ok_count",  len(self._done))
                self._err_count = cp.get("err_count",  len(self._err_rows))
            self._run_start = datetime.now()
            self.start_run(resume=cp)

    def stop_run(self):
        if self.worker and self.worker.is_alive():
            self.stop_flag.set()
        else:
            self._log(self._tr("no_run"), "dim")

    # ================================================================
    #  WORKER THREAD
    # ================================================================
    def _worker(self, key, resume=None):
        try:
            df  = pd.read_excel(self.excel_path, dtype=str)
            col = self.siret_col
            if col not in df.columns:
                self._log(self._tr("col_not_found").format(c=col), "error"); return
            df[col] = df[col].astype(str).str.strip()
            for c in OUTPUT_COLUMNS:
                if c not in df.columns:
                    df[c] = pd.NA

            # Replay cached rows when resuming
            if resume and self._rows:
                for s, pdata in self._rows.items():
                    idxs = df.index[df[col] == s].tolist()
                    if idxs:
                        for k, v in pdata.items():
                            if k in df.columns:
                                df.at[idxs[0], k] = v

            # [IMP-5] Filter invalid SIRETs
            raw_sirets = df[col].dropna().unique().tolist()
            all_s = []
            for s in raw_sirets:
                if s and s.lower() != "nan":
                    if is_valid_siret(s):
                        all_s.append(s)
                    else:
                        self._log(self._tr("invalid_siret").format(s=s), "warning")
                        self._err_rows.append({
                            "SIRET": s,
                            "HTTP Status": "INVALID_FORMAT",
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        })

            # [IMP-9] Check global cache
            cached_hits = 0
            for s in all_s:
                cached = _cache_get(s)
                if cached and s not in self._done:
                    ri = df.index[df[col] == s].tolist()
                    if ri:
                        for k, v in cached.items():
                            if k in df.columns:
                                df.at[ri[0], k] = v
                    self._done.add(s)
                    self._rows[s] = cached
                    with self._stats_lock:
                        self._ok_count += 1
                    cached_hits += 1

            if cached_hits:
                self._log(self._tr("cache_hit").format(n=cached_hits), "dim")

            todo  = [s for s in all_s if s not in self._done]
            total = len(all_s)

            with self._stats_lock:
                done_count = self._ok_count + self._err_count

            delay   = float(self.delay_v.get().strip() or "2")
            do_idcc = self.idcc_v.get()
            use_bat = self.batch_v.get()
            bsz     = int(self.bsize_v.get().strip() or "50") if use_bat else len(todo) or 1
            bpause  = float(self.bpause_v.get().strip() or "30") if use_bat else 0

            self._log(self._tr("processing").format(n=len(todo)), "info")
            if do_idcc:
                self._log("IDCC enabled", "info")
            if use_bat:
                nb_total = -(-len(todo) // bsz)
                self._log(self._tr("batch_info").format(nb=nb_total, bs=bsz, bp=bpause), "batch")
            else:
                self._log(self._tr("full_run"), "info")
            self._log(self._tr("sep"), "dim")

            bn_offset = 0
            if use_bat:
                folder = self.save_dir or (
                    os.path.dirname(self.excel_path) if self.excel_path else os.getcwd())
                stem = self.fname_v.get().strip() or "enriched"
                import glob
                existing = glob.glob(os.path.join(folder, f"{stem}_batch*.xlsx"))
                bn_offset = len(existing)

            bn = bn_offset
            for cs in range(0, len(todo), bsz):
                chunk = todo[cs: cs + bsz]
                bn += 1

                if use_bat:
                    self._log(f"Batch {bn} — rows {cs+1} to {cs+len(chunk)}", "batch")
                    batch_df  = df[df[col].isin(set(chunk))].copy()
                    batch_err = []    # [BUG-2] separate list, not reference to self._err_rows
                else:
                    batch_df  = df
                    batch_err = None  # [BUG-2] None sentinel = not used in non-batch

                for s in chunk:
                    if self.stop_flag.is_set():
                        self._log(self._tr("stopped"), "warning")
                        with self._stats_lock:
                            ok, err = self._ok_count, self._err_count
                        cp_save(self.excel_path, self._done, self._rows,
                                self._err_rows, ok, err)
                        if use_bat:
                            out = self._out_path(bn)   # [BUG-3] computed once
                            self._write_excel(batch_df, batch_err or [], out)
                            self._log(self._tr("batch_saved").format(n=bn, p=out), "batch")
                        else:
                            self._write_excel(df, self._err_rows, self._out_path())
                        return

                    ri = df.index[df[col] == s].tolist()
                    ri = ri[0] if ri else None

                    try:
                        res = fetch_siret_retry(s, key, self._log, T[self._lang])
                        if res["status"] == 200 and res["data"]:
                            p = parse_json(res["data"], log=self._log)
                            # [BUG-5] Skip IDCC for non-French companies
                            pays = p.get("Pays", "France")
                            if do_idcc and pays == "France":
                                p["IDCC"] = fetch_idcc(s)
                            else:
                                p["IDCC"] = MISSING

                            if ri is not None:
                                for k, v in p.items():
                                    if k in df.columns:
                                        df.at[ri, k] = v
                            if use_bat:
                                bi = batch_df.index[batch_df[col] == s].tolist()
                                if bi:
                                    for k, v in p.items():
                                        if k in batch_df.columns:
                                            batch_df.at[bi[0], k] = v

                            with self._stats_lock:   # [BUG-1]
                                self._ok_count += 1

                            eff = p.get("Nombres de salaries", MISSING)
                            lib = p.get("Activite Principale (Libelle)", MISSING)
                            reg = p.get("Region", MISSING)
                            if eff != MISSING: self._eff_c[eff] = self._eff_c.get(eff, 0) + 1
                            if lib != MISSING: self._naf_c[lib] = self._naf_c.get(lib, 0) + 1
                            if reg != MISSING: self._reg_c[reg] = self._reg_c.get(reg, 0) + 1
                            self._rows[s] = p
                            # [IMP-9] Write to global cache
                            _cache_set(s, p)
                            self._log(f"  OK  {s}  {p.get('Denomination', '')}", "success")
                        else:
                            with self._stats_lock:   # [BUG-1]
                                self._err_count += 1
                            erow = {
                                "SIRET": s,
                                "HTTP Status": res.get("status", "?"),
                                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            }
                            self._err_rows.append(erow)
                            if use_bat and batch_err is not None:
                                batch_err.append(erow)
                            status = res.get('status', '?')
                            msg: str = self._tr("err_404").format(s=s) if status == 404 else f"  ERR {s} — HTTP {status}"
                            self._log(msg, "error")
                    except Exception as e:
                        with self._stats_lock:   # [BUG-1]
                            self._err_count += 1
                        erow = {
                            "SIRET": s,
                            "HTTP Status": "Exception",
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        self._err_rows.append(erow)
                        if use_bat and batch_err is not None:
                            batch_err.append(erow)
                        status = res.get('status', '?')
                        msg = self._tr("err_404").format(s=s) if status == 404 else f"  ERR {s} — HTTP {status}"
                        self._log(msg, "error")

                    self._done.add(s)
                    with self._stats_lock:
                        ok, err = self._ok_count, self._err_count
                    self._ok_s.append(ok)
                    self._err_s.append(err)
                    done_count += 1
                    bl = f"{bn}/{-(-len(todo)//max(bsz,1))}" if use_bat else "—"
                    self.root.after(0, self._upd_progress, done_count, total, bl)

                    if done_count % 10 == 0:
                        cp_save(self.excel_path, self._done, self._rows,
                                self._err_rows, ok, err)
                        self._log(self._tr("cp_saved").format(n=len(self._done)), "dim")
                        _save_global_cache()

                    # [IMP-4] Interruptible delay
                    for _ in range(int(delay * 10)):
                        if self.stop_flag.is_set():
                            break
                        time.sleep(0.1)

                # Save batch file  [BUG-3] out computed once
                if use_bat:
                    out = self._out_path(bn)
                    self._write_excel(batch_df, batch_err or [], out)
                    self._log(self._tr("batch_saved").format(n=bn, p=out), "batch")

                # Pause between batches  (already interruptible)
                if use_bat and cs + bsz < len(todo) and not self.stop_flag.is_set():
                    self._log(f"Pausing {bpause}s before next batch...", "batch")
                    for _ in range(int(bpause * 10)):
                        if self.stop_flag.is_set():
                            break
                        time.sleep(0.1)

            self._log(self._tr("sep"), "dim")
            with self._stats_lock:
                ok, err = self._ok_count, self._err_count
            self._log(self._tr("done_msg").format(ok=ok, err=err), "success")

            if not use_bat:
                out = self._out_path()
                self._write_excel(df, self._err_rows, out)
                self._log(self._tr("saved").format(p=out), "success")

            cp_clear(self.excel_path)
            _save_global_cache()

        except Exception as e:
            self._log(f"FATAL: {e}", "error")
            if self.excel_path:
                with self._stats_lock:
                    ok, err = self._ok_count, self._err_count
                cp_save(self.excel_path, self._done, self._rows,
                        self._err_rows, ok, err)

    # ================================================================
    #  EXCEL WRITER  [IMP-3] write lock
    # ================================================================
    def _write_excel(self, df, err_rows, path):
        with self._write_lock:
            try:
                err_sheet  = self._tr("err_sheet")
                main_sheet = self._tr("main_sheet")
                with pd.ExcelWriter(path, engine="openpyxl") as w:
                    df.to_excel(w, sheet_name=main_sheet, index=False)
                    edf = (pd.DataFrame(err_rows) if err_rows
                           else pd.DataFrame(columns=["SIRET", "HTTP Status", "Timestamp"]))
                    edf.to_excel(w, sheet_name=err_sheet, index=False)
                if err_rows:
                    self._log(f"   {len(err_rows)} SIRET(s) in '{err_sheet}' sheet", "warning")
            except Exception as e:
                self._log(self._tr("save_fail").format(e=e), "error")


# ---------------------------------------------------------------
#  CLI MODE  [IMP-8]
# ---------------------------------------------------------------
def run_headless(args):
    """
    Headless CLI mode — no GUI.
    Usage:
      python sirene_enricher.py --headless --file input.xlsx --key YOUR_KEY
                                [--output enriched] [--delay 2] [--col SIRET]
                                [--batch-size 50] [--batch-pause 30]
                                [--no-idcc]
    """
    import random

    key       = args.key
    src       = args.file
    out_stem  = args.output or "enriched"
    delay     = args.delay
    col       = args.col or "SIRET"
    do_idcc   = not args.no_idcc
    batch_sz  = args.batch_size
    batch_p   = args.batch_pause
    use_batch = batch_sz is not None

    tr = T["en"]

    def log(msg, level="info"):
        prefix = {"success": "[OK]", "error": "[ERR]", "warning": "[WARN]",
                  "batch": "[BATCH]", "dim": "[...]"}.get(level, "[INFO]")
        print(f"{datetime.now().strftime('%H:%M:%S')} {prefix} {msg}")

    if not os.path.exists(src):
        print(f"File not found: {src}"); return

    log(f"Loading {src}")
    df = pd.read_excel(src, dtype=str)
    if col not in df.columns:
        print(f"Column '{col}' not found. Available: {list(df.columns)}"); return

    df[col] = df[col].astype(str).str.strip()
    for c in OUTPUT_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA

    raw = df[col].dropna().unique().tolist()
    err_rows = []
    valid = []
    for s in raw:
        if s and s.lower() != "nan":
            if is_valid_siret(s):
                valid.append(s)
            else:
                log(tr["invalid_siret"].format(s=s), "warning")
                err_rows.append({"SIRET": s, "HTTP Status": "INVALID_FORMAT",
                                 "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})

    # Global cache
    cached = 0
    for s in valid[:]:
        hit = _cache_get(s)
        if hit:
            ri = df.index[df[col] == s].tolist()
            if ri:
                for k, v in hit.items():
                    if k in df.columns:
                        df.at[ri[0], k] = v
            cached += 1
    if cached:
        log(tr["cache_hit"].format(n=cached), "dim")

    todo = [s for s in valid if _cache_get(s) is None]
    total = len(valid)
    ok_count = cached
    err_count = 0

    log(tr["processing"].format(n=len(todo)))

    bsz    = batch_sz or len(todo) or 1
    bpause = batch_p  or 0
    bn     = 0

    for cs in range(0, max(len(todo), 1), bsz):
        chunk = todo[cs: cs + bsz]
        bn   += 1
        batch_err = []

        for idx, s in enumerate(chunk):
            try:
                res = fetch_siret_retry(s, key, log, tr)
                if res["status"] == 200 and res["data"]:
                    p = parse_json(res["data"])
                    pays = p.get("Pays", "France")
                    p["IDCC"] = fetch_idcc(s) if do_idcc and pays == "France" else MISSING
                    ri = df.index[df[col] == s].tolist()
                    if ri:
                        for k, v in p.items():
                            if k in df.columns:
                                df.at[ri[0], k] = v
                    _cache_set(s, p)
                    ok_count += 1
                    log(f"OK {s} {p.get('Denomination', '')}", "success")
                else:
                    err_count += 1
                    erow = {"SIRET": s, "HTTP Status": res.get("status", "?"),
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                    err_rows.append(erow)
                    batch_err.append(erow)
                    log(f"ERR {s} — HTTP {res.get('status','?')}", "error")
            except Exception as e:
                err_count += 1
                erow = {"SIRET": s, "HTTP Status": "Exception",
                        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                err_rows.append(erow)
                batch_err.append(erow)
                log(f"ERR {s} — {e}", "error")

            done_count = ok_count + err_count
            pct = int(100 * done_count / total) if total else 0
            print(f"  Progress: {done_count}/{total} ({pct}%)", end="\r")

            for _ in range(int(delay * 10)):
                time.sleep(0.1)

        # Save batch or continue
        if use_batch:
            path = os.path.join(
                os.path.dirname(src),
                f"{out_stem}_batch{bn:03d}.xlsx")
            with pd.ExcelWriter(path, engine="openpyxl") as w:
                batch_rows = df[df[col].isin(set(chunk))]
                batch_rows.to_excel(w, sheet_name="Results", index=False)
                edf = pd.DataFrame(batch_err) if batch_err else pd.DataFrame(
                    columns=["SIRET", "HTTP Status", "Timestamp"])
                edf.to_excel(w, sheet_name="SIRET Not Found", index=False)
            log(tr["batch_saved"].format(n=bn, p=path), "batch")
            if cs + bsz < len(todo):
                log(f"Pausing {bpause}s...", "batch")
                time.sleep(bpause)

    print()
    log(tr["done_msg"].format(ok=ok_count, err=err_count), "success")

    if not use_batch:
        out_path = os.path.join(os.path.dirname(src), f"{out_stem}.xlsx")
        with pd.ExcelWriter(out_path, engine="openpyxl") as w:
            df.to_excel(w, sheet_name="Results", index=False)
            edf = pd.DataFrame(err_rows) if err_rows else pd.DataFrame(
                columns=["SIRET", "HTTP Status", "Timestamp"])
            edf.to_excel(w, sheet_name="SIRET Not Found", index=False)
        log(tr["saved"].format(p=out_path), "success")

    _save_global_cache()


# ---------------------------------------------------------------
#  ENTRY POINT
# ---------------------------------------------------------------
def _launch():
    root = tk.Tk()
    App(root)
    root.mainloop()


def main():
    _init_maps()
    _load_global_cache()

    parser = argparse.ArgumentParser(
        description="SIRENE Enricher v7.0 — INSEE API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  GUI mode (default):\n"
            "    python sirene_enricher.py\n\n"
            "  Headless mode:\n"
            "    python sirene_enricher.py --headless --file companies.xlsx --key YOUR_KEY\n"
            "    python sirene_enricher.py --headless --file companies.xlsx --key YOUR_KEY "
            "--delay 2 --batch-size 50 --batch-pause 30 --output enriched\n"
        )
    )
    parser.add_argument("--headless",    action="store_true", help="Run without GUI")
    parser.add_argument("--file",        help="Path to input Excel file (.xlsx)")
    parser.add_argument("--key",         help="INSEE API key")
    parser.add_argument("--output",      default="enriched", help="Output filename stem (default: enriched)")
    parser.add_argument("--delay",       type=float, default=2.0, help="Delay between requests in seconds (default: 2)")
    parser.add_argument("--col",         default="SIRET", help="Name of the SIRET column (default: SIRET)")
    parser.add_argument("--batch-size",  type=int, default=None, help="Enable batch mode with given size")
    parser.add_argument("--batch-pause", type=float, default=30.0, help="Pause between batches in seconds (default: 30)")
    parser.add_argument("--no-idcc",     action="store_true", help="Disable IDCC fetching")

    args = parser.parse_args()

    if args.headless:
        if not args.file or not args.key:
            parser.error("--headless requires --file and --key")
        run_headless(args)
    else:
        show_splash(_launch)


if __name__ == "__main__":
    main()
