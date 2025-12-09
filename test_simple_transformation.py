import pandas as pd
import numpy as np
import sys

# Read the input parquet file
input_file = sys.argv[1] if len(sys.argv) > 1 else "/Users/prathameshbhandekar/Downloads/11_30_00-13_00_00.parquet"
print(f"Reading: {input_file}\n")

df = pd.read_parquet(input_file)

print("=== INPUT FILE (before transformation) ===")
print(f"Columns: {df.columns.tolist()}\n")

# Check transaction_id column
if 'transaction_id' in df.columns:
    print(f"transaction_id dtype: {df['transaction_id'].dtype}")
    print(f"Sample values: {df['transaction_id'].dropna().head(10).tolist()}\n")

# Now simulate the transformation process
print("=== SIMULATING TRANSFORMATION ===\n")

# Step 1: Rename columns (simulating the column_mapping)
df_transformed = df.copy()
if 'transaction_id' in df_transformed.columns:
    df_transformed = df_transformed.rename(columns={'transaction_id': 'TRANSACTION_ID'})

print("Step 1 - After renaming:")
print(f"TRANSACTION_ID dtype: {df_transformed['TRANSACTION_ID'].dtype}")
print(f"Sample values: {df_transformed['TRANSACTION_ID'].dropna().head(5).tolist()}\n")

# Step 2: OLD method (astype(str) - causes .0 problem)
df_old = df_transformed.copy()
if 'TRANSACTION_ID' in df_old.columns:
    df_old['TRANSACTION_ID'] = df_old['TRANSACTION_ID'].astype(str, errors='ignore')
    
print("Step 2 - OLD method (astype(str)):")
print(f"TRANSACTION_ID dtype: {df_old['TRANSACTION_ID'].dtype}")
print(f"Sample values: {df_old['TRANSACTION_ID'].dropna().head(5).tolist()}")
has_decimal = df_old['TRANSACTION_ID'].astype(str).str.contains(r'\.0$', na=False).any()
print(f"Has .0 suffix: {'❌ YES' if has_decimal else '✅ NO'}\n")

# Step 3: NEW method (with cleaning function)
def clean_decimal_suffix(x):
    if pd.isnull(x):
        return x
    x_str = str(x)
    if '.' in x_str and x_str.endswith('.0'):
        x_str = x_str[:-2]
    return x_str

df_new = df_transformed.copy()
if 'TRANSACTION_ID' in df_new.columns:
    df_new['TRANSACTION_ID'] = df_new['TRANSACTION_ID'].apply(clean_decimal_suffix)

print("Step 3 - NEW method (with clean_decimal_suffix):")
print(f"TRANSACTION_ID dtype: {df_new['TRANSACTION_ID'].dtype}")
print(f"Sample values: {df_new['TRANSACTION_ID'].dropna().head(10).tolist()}")
has_decimal = df_new['TRANSACTION_ID'].astype(str).str.contains(r'\.0$', na=False).any()
print(f"Has .0 suffix: {'❌ YES' if has_decimal else '✅ NO'}\n")

# Step 4: Test parquet write/read cycle
import io
print("Step 4 - Testing parquet write/read cycle:")
buf = io.BytesIO()
df_new[['TRANSACTION_ID']].to_parquet(buf, index=False)
buf.seek(0)
df_readback = pd.read_parquet(buf)

print(f"After parquet round-trip dtype: {df_readback['TRANSACTION_ID'].dtype}")
print(f"Sample values: {df_readback['TRANSACTION_ID'].dropna().head(10).tolist()}")
has_decimal = df_readback['TRANSACTION_ID'].astype(str).str.contains(r'\.0$', na=False).any()
print(f"Has .0 suffix: {'❌ YES - PROBLEM!' if has_decimal else '✅ NO - WORKING!'}\n")

print("=" * 60)
print("CONCLUSION:")
if has_decimal:
    print("❌ The fix is NOT working - .0 suffix still present after parquet write!")
else:
    print("✅ The fix IS working - no .0 suffix in the output!")
print("=" * 60)
