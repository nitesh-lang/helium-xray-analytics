# ============================================================
# IMPORTS
# ============================================================
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import pandas as pd
import os

from concurrent.futures import ThreadPoolExecutor

from processor import process_xray_df


# ============================================================
# GLOBAL VARIABLES
# ============================================================

CACHE_DF = None

RAW_FOLDER = "data/raw"

CACHE_FILE = "data/cache.parquet"


# ============================================================
# FASTAPI APP
# ============================================================

app = FastAPI()

from fastapi.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=1000)

templates = Jinja2Templates(directory="templates")


# ============================================================
# FILE LOADER
# Loads a single file safely
# ============================================================

def load_single_file(file_path, month_folder, brand_folder, file):

    try:

        # ---------------------------
        # CSV FILE
        # ---------------------------
        if file.lower().endswith(".csv"):
            df = pd.read_csv(file_path)

        # ---------------------------
        # XLSX FILE
        # ---------------------------
        elif file.lower().endswith(".xlsx"):
            df = pd.read_excel(
                file_path,
                engine="openpyxl"
            )

        # ---------------------------
        # XLS FILE
        # ---------------------------
        elif file.lower().endswith(".xls"):
            df = pd.read_excel(
                file_path,
                engine="xlrd"
            )

        else:
            return None

        # ---------------------------
        # ADD META DATA
        # ---------------------------
        subcategory_name = os.path.splitext(file)[0]

        df["Month"] = month_folder
        df["BrandFolder"] = brand_folder
        df["SubCategory"] = subcategory_name

        return df

    except Exception as e:
        print(f"Skipping file {file} → {e}")
        return None


# ============================================================
# STARTUP DATA LOADER
# Runs once when server starts
# ============================================================

@app.on_event("startup")
def load_data():

    global CACHE_DF

    if os.path.exists(CACHE_FILE):
        print("Loading cached data...")
        CACHE_DF = pd.read_parquet(CACHE_FILE)
        return

    all_data = []

    # --------------------------------------------------------
    # CHECK RAW FOLDER
    # --------------------------------------------------------

    if not os.path.exists(RAW_FOLDER):
        print("data/raw folder not found")
        return

    # --------------------------------------------------------
    # LOOP MONTH FOLDERS
    # --------------------------------------------------------

    for month_folder in os.listdir(RAW_FOLDER):

        month_path = os.path.join(RAW_FOLDER, month_folder)

        if not os.path.isdir(month_path):
            continue

        # ----------------------------------------------------
        # LOOP BRAND FOLDERS
        # ----------------------------------------------------

        for brand_folder in os.listdir(month_path):

            brand_path = os.path.join(month_path, brand_folder)

            if not os.path.isdir(brand_path):
                continue

            files = os.listdir(brand_path)

            # ------------------------------------------------
            # PARALLEL FILE LOADING
            # ------------------------------------------------

            with ThreadPoolExecutor(max_workers=6) as executor:

                results = executor.map(
                    lambda file: load_single_file(
                        os.path.join(brand_path, file),
                        month_folder,
                        brand_folder,
                        file
                    ),
                    files
                )

                for df in results:
                    if df is not None:
                        all_data.append(df)

    # --------------------------------------------------------
    # COMBINE ALL DATA
    # --------------------------------------------------------

    if all_data:

        CACHE_DF = pd.concat(all_data, ignore_index=True)

        # ---------------------------
        # CLEAN NUMERIC COLUMNS
        # ---------------------------

        CACHE_DF["Sales"] = CACHE_DF["Sales"].astype(str).str.replace(",", "")
        CACHE_DF["Sales"] = pd.to_numeric(CACHE_DF["Sales"], errors="coerce").fillna(0)

        CACHE_DF["Revenue"] = CACHE_DF["Revenue"].astype(str).str.replace("₹", "").str.replace(",", "")
        CACHE_DF["Revenue"] = pd.to_numeric(CACHE_DF["Revenue"], errors="coerce").fillna(0)

        # ---------------------------
        # CLEAN ALL OBJECT COLUMNS
        # ---------------------------

        for col in CACHE_DF.columns:
            if CACHE_DF[col].dtype == "object":
                CACHE_DF[col] = CACHE_DF[col].astype(str).str.replace(",", "").str.replace("₹", "")

        # ---------------------------
        # SAVE PARQUET CACHE
        # ---------------------------

        CACHE_DF.to_parquet(CACHE_FILE)

        print("Cache file created")
        print("Data loaded successfully")
        print(f"Total rows loaded: {len(CACHE_DF)}")

    else:
        print("No data loaded")


# ============================================================
# HOME ROUTE
# ============================================================

@app.get("/", response_class=HTMLResponse)
def home(request: Request):

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request
        }
    )


# ============================================================
# ANALYZE ROUTE
# ============================================================

@app.get("/analyze")
def analyze(
    month: str = "All",
    brand: str = "All",
    subcategory: str = "All"
):

    # --------------------------------------------------------
    # SAFETY CHECK
    # --------------------------------------------------------

    if CACHE_DF is None:

        return {
            "kpi": {
                "revenue": 0,
                "units": 0,
                "asp": 0
            },
            "data": []
        }

    # --------------------------------------------------------
    # USE CACHE DIRECTLY (NO COPY)
    # --------------------------------------------------------

    final_df = CACHE_DF

    # --------------------------------------------------------
    # PROCESS DATA
    # --------------------------------------------------------

    final_df = final_df.fillna(0)

    result = process_xray_df(
        final_df,
        month,
        brand,
        subcategory
    )

    result = result[:2000]

    # --------------------------------------------------------
    # KPI CALCULATION
    # --------------------------------------------------------

    total_revenue = (
        final_df["Revenue"].sum()
        if "Revenue" in final_df.columns
        else 0
    )

    total_units = (
        final_df["Sales"].sum()
        if "Sales" in final_df.columns
        else 0
    )

    asp = (
        total_revenue / total_units
        if total_units
        else 0
    )

    # --------------------------------------------------------
    # RESPONSE
    # --------------------------------------------------------

    return {
        "kpi": {
            "revenue": round(total_revenue, 2),
            "units": round(total_units, 2),
            "asp": round(asp, 2)
        },
        "data": result
    }