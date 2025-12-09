import pandas as pd
import numpy as np
import io

# Create a sample dataframe that mimics the input data
# This simulates what happens when parquet files are read
sample_data = {
    'order_id': [218461993.0, 218461994.0, 218461995.0, None, 218461996.0],
    'transaction_id': [111222333.0, 444555666.0, None, 777888999.0, 123456789.0],
    'affid': [100.0, 200.0, 300.0, 400.0, None],
    'customer_id': [5001.0, 5002.0, 5003.0, 5004.0, 5005.0],
    'order_total': [99.99, 149.99, 199.99, 299.99, 49.99],
    'order_status': [2, 2, 7, 2, 6],
}

# Create DataFrame - this simulates reading from parquet
df = pd.DataFrame(sample_data)

print("=== STEP 1: Initial DataFrame (after reading parquet) ===")
print(df.head())
print("\nColumn dtypes:")
print(df.dtypes)

# Simulate the column mapping
column_mapping = {
    "order_id": "ORDER_ID",
    "transaction_id": "TRANSACTION_ID",
    "affid": "AFFID",
    "customer_id": "CUSTOMER_NUMBER",
    "order_total": "ORDER_TOTAL",
    "order_status": "ORDER_STATUS",
}

df = df.rename(columns=column_mapping)

print("\n=== STEP 2: After column renaming ===")
print(df.dtypes)

# Define schema types (subset for testing)
schema_types = {
    "ORDER_ID": str,
    "TRANSACTION_ID": str,
    "AFFID": str,
    "CUSTOMER_NUMBER": str,
    "ORDER_TOTAL": float,
    "ORDER_STATUS": int,
}

print("\n=== STEP 3: Testing OLD method (astype(str) directly) ===")
df_old = df.copy()
for col, dtype in schema_types.items():
    if col in df_old.columns:
        df_old[col] = df_old[col].astype(dtype, errors="ignore")

print("ORDER_ID values (OLD method):")
print(df_old['ORDER_ID'].head())
print("\nTRANSACTION_ID values (OLD method):")
print(df_old['TRANSACTION_ID'].head())

print("\n=== STEP 4: Testing NEW method (with cleaning function) ===")

def clean_decimal_suffix(x):
    # Keep null values as-is
    if pd.isnull(x):
        return x
    # Convert to string and remove .0 if present
    x_str = str(x)
    if '.' in x_str and x_str.endswith('.0'):
        # Remove only the .0 suffix
        x_str = x_str[:-2]
    return x_str

string_columns_to_clean = {
    "TRANSACTION_ID", "AFFID", "CUSTOMER_NUMBER", "ORDER_ID"
}

df_new = df.copy()
for col, dtype in schema_types.items():
    if col in df_new.columns:
        if dtype == str and col in string_columns_to_clean:
            # For string columns that need cleaning, apply the cleaning function during conversion
            df_new[col] = df_new[col].apply(clean_decimal_suffix)
        else:
            df_new[col] = df_new[col].astype(dtype, errors="ignore")

print("ORDER_ID values (NEW method with cleaning):")
print(df_new['ORDER_ID'].head())
print("\nTRANSACTION_ID values (NEW method with cleaning):")
print(df_new['TRANSACTION_ID'].head())
print("\nAFFID values (NEW method with cleaning):")
print(df_new['AFFID'].head())

print("\n=== STEP 5: Testing parquet write/read cycle ===")
# Write to parquet
buf = io.BytesIO()
df_new.to_parquet(buf, index=False)
buf.seek(0)

# Read back from parquet
df_read_back = pd.read_parquet(buf)
print("After writing and reading parquet:")
print(df_read_back.dtypes)
print("\nORDER_ID values (after parquet round-trip):")
print(df_read_back['ORDER_ID'].head())
print("\nTRANSACTION_ID values (after parquet round-trip):")
print(df_read_back['TRANSACTION_ID'].head())

# Check for .0 suffix
print("\n=== STEP 6: Checking for .0 suffix ===")
for col in ['ORDER_ID', 'TRANSACTION_ID', 'AFFID', 'CUSTOMER_NUMBER']:
    if col in df_read_back.columns:
        has_decimal = df_read_back[col].astype(str).str.contains(r'\.0$', na=False).any()
        if has_decimal:
            print(f"❌ {col} still has .0 suffix!")
            examples = df_read_back[df_read_back[col].astype(str).str.contains(r'\.0$', na=False)][col].head().tolist()
            print(f"   Examples: {examples}")
        else:
            print(f"✅ {col} is clean (no .0 suffix)")
