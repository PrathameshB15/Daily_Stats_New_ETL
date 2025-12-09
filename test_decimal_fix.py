import pandas as pd
import numpy as np
import sys

# Read the parquet file
if len(sys.argv) > 1:
    parquet_file = sys.argv[1]
else:
    print("Usage: python test_decimal_fix.py <parquet_file_path>")
    sys.exit(1)

print(f"Reading parquet file: {parquet_file}")
df = pd.read_parquet(parquet_file)

# Check columns that might have .0 issues
columns_to_check = [
    "TRANSACTION_ID", "SUB_AFFID", "AFFID", "CUSTOMER_NUMBER", 
    "ORDER_ID", "TRACKING_NUMBER", "CREDIT_CARD_NUMBER", 
    "ANCESTOR_ORDER_ID", "AUTH_ID", "transaction_id", "order_id"
]

print("\n=== Checking for .0 values ===")
for col in columns_to_check:
    if col in df.columns:
        print(f"\n{col}:")
        print(f"  dtype: {df[col].dtype}")
        # Show first 5 non-null values
        sample_values = df[col].dropna().head(10).tolist()
        print(f"  Sample values: {sample_values}")
        
        # Check if any values contain .0
        if df[col].dtype == 'object' or df[col].dtype == 'string':
            has_decimal = df[col].astype(str).str.contains(r'\.0$', na=False).any()
            if has_decimal:
                print(f"  ⚠️  Contains .0 suffix!")
                # Show some examples
                examples = df[df[col].astype(str).str.contains(r'\.0$', na=False)][col].head(5).tolist()
                print(f"  Examples: {examples}")
        elif df[col].dtype in ['float64', 'float32']:
            print(f"  ⚠️  Column is still float type (should be string)!")

print("\n=== Testing cleaning function ===")

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

# Test the function on a few values
test_values = [218461993.0, "218461993.0", "218461993", None, np.nan, "Not Available", 123.45]
print("\nTesting clean_decimal_suffix function:")
for val in test_values:
    result = clean_decimal_suffix(val)
    print(f"  {val!r} ({type(val).__name__}) -> {result!r} ({type(result).__name__})")
