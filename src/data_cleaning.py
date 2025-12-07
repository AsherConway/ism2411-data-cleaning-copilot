"""
data_cleaning.py

This script loads messy sales data, applies several cleaning steps,
and saves the cleaned dataset to data/processed/sales_data_clean.csv.
It demonstrates basic Python, data cleaning, and responsible use of GitHub Copilot.
"""

import pandas as pd
import os

# Function: load_data
# Purpose: Load the raw CSV file into a DataFrame.
# Why: This gives us a starting point for cleaning.
def load_data(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    df = pd.read_csv(file_path)
    print(f"Loaded data with {df.shape[0]} rows and {df.shape[1]} columns")
    return df

# Function: clean_column_names
# Purpose: Standardize column names to lowercase and underscores.
# Why: Consistent column names prevent bugs later.
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    print("Standardized column names.")
    return df

# Function: strip_text_fields
# Purpose: Remove leading/trailing spaces from text fields.
# Why: Extra whitespace causes duplicate categories + grouping problems.
def strip_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()
    print("Stripped whitespace from text fields.")
    return df

# Function: handle_missing_values
# Purpose: Fill missing numeric values with 0 and text with forward-fill.
# Why: Missing values break analysis; consistent rules help maintain dataset quality.
def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    text_cols = df.select_dtypes(include=["object"]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    df[text_cols] = df[text_cols].fillna(method="ffill")
    print("Handled missing numeric and text values.")
    return df

# Function: remove_invalid_rows
# Purpose: Remove negative prices or quantities.
# Why: Negative numbers are data entry errors and break analytics.
def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    # Fix typo in column name
    df.columns = df.columns.str.replace("quanitity", "quantity")

    # Convert columns to numeric safely
    for col in ["price", "quantity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with NaNs in price or quantity
    cols_to_check = [c for c in ["price", "quantity"] if c in df.columns]
    if cols_to_check:
        df = df.dropna(subset=cols_to_check)

    # Remove negative values
    for col in ["price", "quantity"]:
        if col in df.columns:
            df = df[df[col] >= 0]

    print("Removed invalid rows (negative price/quantity).")
    return df

# Function: save_cleaned_data
# Purpose: Save cleaned DataFrame to processed folder.
# Why: Final deliverable for grader and recruiter portfolio.
def save_cleaned_data(df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved cleaned data → {output_path}")

# FULL CLEANING PIPELINE
def clean_pipeline(raw_path: str, cleaned_path: str) -> None:
    df = load_data(raw_path)
    df = clean_column_names(df)
    df = strip_text_fields(df)
    df = handle_missing_values(df)
    df = remove_invalid_rows(df)
    save_cleaned_data(df, cleaned_path)
    print("Cleaning pipeline finished successfully!")
    print(df.head())

# MAIN EXECUTION BLOCK
if __name__ == "__main__":
    raw_path = "data/raw/sales_data_raw.csv"
    cleaned_path = "data/processed/sales_data_clean.csv"
    clean_pipeline(raw_path, cleaned_path)


