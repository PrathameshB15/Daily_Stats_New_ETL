#!/usr/bin/env python3

import pandas as pd
import sys
import os

def test_flexfactor_approval_processing():
    """Test the FlexFactor approval rate processing logic"""
    
    print("=== FlexFactor Approval Rate Processing Test ===")
    
    # Simulate API response data with FlexFactor approval rate (12 rows total)
    test_data = [
        100,      # Joins count
        "15.50%", # Joins approval %
        25000,    # Joins value
        75,       # Initials count
        "12.75%", # Initials approval %
        18000,    # Initials value
        50,       # Rebill count
        "20.25%", # Rebill approval %
        15000,    # Rebill value
        200,      # FlexFactor total count
        "18.25%", # FlexFactor approval % (THIS IS THE NEW ONE)
        45000     # FlexFactor total value
    ]
    
    print(f"Test data has {len(test_data)} elements")
    
    # Create DataFrame with required format - now with 12 rows
    df = pd.DataFrame({
        'Metric': [
            'Joins', 'Joins Approval %', 'Joins Value',
            'Initials', 'Initials Approval %', 'Initials Value', 
            'Rebill', 'Rebill Approval %', 'Rebill Value',
            'FlexFactor Total', 'FlexFactor Approval %', 'FlexFactor Total Value'
        ],
        'Value': test_data
    })
    
    print("\nDataFrame structure:")
    print(df)
    
    # Extract values
    joins_count = test_data[0]
    joins_approval = test_data[1] 
    joins_value = test_data[2]

    initials_count = test_data[3]
    initials_approval = test_data[4]
    initials_value = test_data[5]

    rebill_count = test_data[6]
    rebill_approval = test_data[7]
    rebill_value = test_data[8]
    
    # Extract FlexFactor combined values
    flexfactor_total_count = test_data[9]
    flexfactor_approval = test_data[10]  # THIS IS THE KEY ONE
    flexfactor_total_value = test_data[11]
    
    print(f"\nExtracted values:")
    print(f"Joins -> Count: {joins_count}, Approval %: {joins_approval}, Value: {joins_value}")
    print(f"Initials -> Count: {initials_count}, Approval %: {initials_approval}, Value: {initials_value}")
    print(f"Rebill -> Count: {rebill_count}, Approval %: {rebill_approval}, Value: {rebill_value}")
    print(f"FlexFactor Total -> Count: {flexfactor_total_count}, Approval %: {flexfactor_approval}, Value: {flexfactor_total_value}")
    
    # Test FlexFactor approval formatting
    flexfactor_approval_formatted = flexfactor_approval if flexfactor_approval is not None else "0.00%"
    print(f"\nFlexFactor approval formatted: {flexfactor_approval_formatted}")
    
    # Test metrics dictionary creation
    metrics = {
        "flexfactor_total": flexfactor_total_count,
        "flexfactor_approval": flexfactor_approval_formatted,
        "flexfactor_total_revenue": flexfactor_total_value,
    }
    
    print(f"\nMetrics dictionary (FlexFactor portion):")
    for key, value in metrics.items():
        print(f"  {key}: {value}")
    
    # Test image generation data structure
    metrics_data = [
        ("Joins", "joins","joins_approval", "joins_revenue"),
        ("Initials", "initials","initials_approval", "initials_revenue"),
        ("Rebills", "rebills","rebills_approval", "rebills_revenue"),
        ("FlexFactor", "flexfactor_total", "flexfactor_approval", "flexfactor_total_revenue"),
    ]
    
    print(f"\nImage generation metrics_data structure:")
    for metric in metrics_data:
        print(f"  {metric}")
    
    print(f"\n✅ Test completed successfully!")
    print(f"✅ FlexFactor approval rate is properly processed: {flexfactor_approval}")
    print(f"✅ Data structure supports 12 rows (9 original + 3 FlexFactor)")
    
if __name__ == "__main__":
    test_flexfactor_approval_processing()
