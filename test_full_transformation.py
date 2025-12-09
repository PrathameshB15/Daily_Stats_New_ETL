import sys
import os
import importlib.util
import pandas as pd
import io

# Load the transformation script
spec = importlib.util.spec_from_file_location("transform_module", "/Users/prathameshbhandekar/Downloads/import pandas as pd.py")
transform_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transform_module)
transform_parquet_files = transform_module.transform_parquet_files

# Test with actual file
if len(sys.argv) > 1:
    input_file = sys.argv[1]
else:
    input_file = "/Users/prathameshbhandekar/Downloads/11_30_00-13_00_00.parquet"

print(f"Testing transformation with: {input_file}")

# Prepare the file as the function expects (BytesIO)
with open(input_file, 'rb') as f:
    file_content = f.read()
    
parquet_files = [io.BytesIO(file_content)]

# Run transformation with test parameters
print("\n=== Running transformation ===")
result_buf = transform_parquet_files(
    parquet_files=parquet_files,
    client_id=10010,
    client_name="Test Client",
    crm_name="Test CRM",
    date_time="2025-11-21 00:00:00"
)

# Read the result
result_buf.seek(0)
result_df = pd.read_parquet(result_buf)

print("\n=== Output DataFrame Info ===")
print(f"Shape: {result_df.shape}")
print(f"\nColumns: {result_df.columns.tolist()}")

# Check the problematic columns
columns_to_check = [
    "TRANSACTION_ID", "SUB_AFFID", "AFFID", "CUSTOMER_NUMBER", 
    "ORDER_ID", "TRACKING_NUMBER", "CREDIT_CARD_NUMBER", 
    "ANCESTOR_ORDER_ID", "AUTH_ID"
]

print("\n=== Checking for .0 suffix in OUTPUT ===")
for col in columns_to_check:
    if col in result_df.columns:
        print(f"\n{col}:")
        print(f"  dtype: {result_df[col].dtype}")
        
        # Show first 10 non-null values
        sample_values = result_df[col].dropna().head(10).tolist()
        print(f"  Sample values: {sample_values}")
        
        # Check for .0 suffix
        if result_df[col].dtype == 'object' or str(result_df[col].dtype) == 'string':
            has_decimal = result_df[col].astype(str).str.contains(r'\.0$', na=False).any()
            if has_decimal:
                print(f"  ❌ Contains .0 suffix!")
                examples = result_df[result_df[col].astype(str).str.contains(r'\.0$', na=False)][col].head(5).tolist()
                print(f"  Examples with .0: {examples}")
            else:
                print(f"  ✅ Clean (no .0 suffix)")
        elif result_df[col].dtype in ['float64', 'float32']:
            print(f"  ⚠️  Column is still float type (should be string)!")
            print(f"  First values: {result_df[col].head(10).tolist()}")

# Save output for manual inspection
output_file = "/Users/prathameshbhandekar/Daily_stats/test_output.parquet"
result_df.to_parquet(output_file, index=False)
print(f"\n✅ Output saved to: {output_file}")
print("You can now inspect this file to verify the fix worked!")
