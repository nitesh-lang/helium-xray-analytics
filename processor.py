import pandas as pd
import numpy as np



# =========================================================
# CLEAN NUMERIC VALUES
# Removes ₹ , commas and converts safely to float
# =========================================================
def clean_numeric(col):

    col = (
        col.astype(str)
        .str.replace("₹", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )

    col = pd.to_numeric(col, errors="coerce")

    col = col.replace([np.inf, -np.inf], np.nan)

    return col.fillna(0)



# =========================================================
# SAFE FLOAT CONVERSION
# Prevents NaN / infinity values
# =========================================================
def safe(val):

    if pd.isna(val):
        return 0

    if val == np.inf or val == -np.inf:
        return 0

    return float(val)



# =========================================================
# NORMALIZE STRINGS
# Lowercase + remove extra spaces
# =========================================================
def normalize(val):

    return str(val).strip().lower()



# =========================================================
# VECTOR NORMALIZATION FOR SERIES
# Faster than apply(normalize)
# =========================================================
def normalize_series(series):

    return series.astype(str).str.strip().str.lower()



# =========================================================
# VALIDATE REQUIRED COLUMNS
# =========================================================
def validate_columns(df):

    required_columns = [
        "Brand",
        "ASIN",
        "Sales",
        "Month",
        "BrandFolder",
        "SubCategory"
    ]

    missing = []

    for col in required_columns:

        if col not in df.columns:
            missing.append(col)

    if len(missing) > 0:

        raise ValueError(
            "Missing required columns: " + ", ".join(missing)
        )



# =========================================================
# FIND REVENUE COLUMN
# Handles variations like "Revenue (₹)"
# =========================================================
def find_revenue_column(df):

    revenue_cols = []

    for col in df.columns:

        if "revenue" in col.lower():

            revenue_cols.append(col)

    if len(revenue_cols) == 0:

        raise ValueError("Revenue column missing")

    return revenue_cols[0]



# =========================================================
# CLEAN DATAFRAME NUMERIC DATA
# =========================================================
def clean_numeric_columns(df, revenue_col):

    df[revenue_col] = clean_numeric(df[revenue_col])

    df["Sales"] = clean_numeric(df["Sales"])

    df["Revenue"] = df[revenue_col]

    df = df.replace([np.inf, -np.inf], 0)

    df = df.fillna(0)

    return df



# =========================================================
# APPLY USER FILTERS
# =========================================================
def apply_filters(df,
                  selected_month,
                  selected_brandfolder,
                  selected_subcategory):


    if selected_month != "All":

        df = df[
            normalize_series(df["Month"])
            ==
            normalize(selected_month)
        ]


    if selected_brandfolder != "All":

        df = df[
            normalize_series(df["BrandFolder"])
            ==
            normalize(selected_brandfolder)
        ]


    if selected_subcategory != "All":

        df = df[
            normalize_series(df["SubCategory"])
            ==
            normalize(selected_subcategory)
        ]


    return df



# =========================================================
# CALCULATE SUBCATEGORY TOTALS
# =========================================================
def calculate_subcategory_totals(df):

    sub_totals = (

        df
        .groupby(
            ["Month", "BrandFolder", "SubCategory"],
            as_index=False
        )["Revenue"]
        .sum()
        .rename(columns={"Revenue": "SubRevenue"})

    )

    df = df.merge(

        sub_totals,

        on=["Month", "BrandFolder", "SubCategory"],

        how="left"

    )

    return df



# =========================================================
# CALCULATE METRICS
# =========================================================
def calculate_metrics(df):

    df["Revenue %"] = np.where(

        df["SubRevenue"] == 0,

        0,

        (df["Revenue"] / df["SubRevenue"]) * 100

    )


    df["ASP"] = np.where(

        df["Sales"] == 0,

        0,

        df["Revenue"] / df["Sales"]

    )


    df["Revenue"] = df["Revenue"].round(2)

    df["Revenue %"] = df["Revenue %"].round(2)

    df["Sales"] = df["Sales"].round(2)

    df["ASP"] = df["ASP"].round(2)


    return df



# =========================================================
# FINAL RESULT BUILDER
# =========================================================
def build_result(df):

    result_df = df[

        [

            "Month",

            "BrandFolder",

            "SubCategory",

            "Brand",

            "ASIN",

            "Revenue",

            "Revenue %",

            "Sales",

            "ASP"

        ]

    ].rename(

        columns={"Sales": "Units"}

    )


    return result_df.to_dict("records")



# =========================================================
# MAIN PROCESSOR FUNCTION
# =========================================================
def process_xray_df(
        df,
        selected_month="All",
        selected_brandfolder="All",
        selected_subcategory="All"
):


    # -----------------------------------------------------
    # CLEAN COLUMN NAMES
    # -----------------------------------------------------
    df.columns = df.columns.str.strip()



    # -----------------------------------------------------
    # VALIDATE REQUIRED STRUCTURE
    # -----------------------------------------------------
    validate_columns(df)



    # -----------------------------------------------------
    # FIND REVENUE COLUMN
    # -----------------------------------------------------
    revenue_col = find_revenue_column(df)



    # -----------------------------------------------------
    # CLEAN NUMERIC DATA
    # -----------------------------------------------------
    df = clean_numeric_columns(df, revenue_col)



    # -----------------------------------------------------
    # APPLY USER FILTERS
    # -----------------------------------------------------
    df = apply_filters(

        df,

        selected_month,

        selected_brandfolder,

        selected_subcategory

    )



    # -----------------------------------------------------
    # EMPTY DATA CHECK
    # -----------------------------------------------------
    if df.empty:

        return []



    # -----------------------------------------------------
    # CALCULATE CATEGORY TOTALS
    # -----------------------------------------------------
    df = calculate_subcategory_totals(df)



    # -----------------------------------------------------
    # CALCULATE METRICS
    # -----------------------------------------------------
    df = calculate_metrics(df)



    # -----------------------------------------------------
    # BUILD FINAL RESULT
    # -----------------------------------------------------
    result = build_result(df)



    return result