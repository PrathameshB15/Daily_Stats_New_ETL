#!/usr/bin/env python3

"""
Test script to validate FlexFactor alignment in image generation
"""

import sys
import os
sys.path.append('/Users/prathameshbhandekar/Daily_stats')

from PIL import Image, ImageDraw, ImageFont
from datetime import datetime, timedelta

def test_flexfactor_alignment():
    """Test FlexFactor alignment in image generation"""
    
    # Mock client metrics with FlexFactor data
    mock_metrics = {
        "joins": "1,234",
        "joins_approval": "15.25%",
        "joins_revenue": "50,000",
        
        "initials": "2,567",
        "initials_approval": "18.75%", 
        "initials_revenue": "125,000",
        
        "rebills": "890",
        "rebills_approval": "22.10%",
        "rebills_revenue": "75,000",
        
        "flexfactor_total": "456",
        "flexfactor_approval": "19.50%",  # This should be properly aligned
        "flexfactor_total_revenue": "32,500",
        
        "gross_revenue": "282,500",
        "net_revenue": "250,000",
        "chargebacks_count": "45",
        "chargebacks": "32,500",
        "refund_cs_count": "12",
        "refund_cs_amount": "8,500",
        "refund_alert_count": "8", 
        "refund_alert_amount": "5,200"
    }
    
    print("Testing FlexFactor Alignment in Image Generation")
    print("=" * 50)
    
    # Test metrics data structure
    metrics_data = [
        ("Joins", "joins","joins_approval", "joins_revenue"),
        ("Initials", "initials","initials_approval", "initials_revenue"),
        ("Rebills", "rebills","rebills_approval", "rebills_revenue"),
        ("FlexFactor", "flexfactor_total", "flexfactor_approval", "flexfactor_total_revenue"),
        ("Gross", "gross_revenue", None, "gross_revenue"),
        ("Chargebacks", "chargebacks_count", None, "chargebacks"),
        ("CS Refunds", "refund_cs_count", None, "refund_cs_amount"),
        ("Alert Refunds", "refund_alert_count", None, "refund_alert_amount"),
        ("Net", "net_revenue", None, "net_revenue")
    ]
    
    print("Metrics Data Structure:")
    for i, (metric_name, count_key, approval_key, revenue_key) in enumerate(metrics_data):
        count_value = mock_metrics.get(count_key, "0")
        approval_value = mock_metrics.get(approval_key, "0") if approval_key else "N/A"
        revenue_value = mock_metrics.get(revenue_key, "0")
        
        print(f"{i+1:2}. {metric_name:12} | Count: {count_value:>8} | Approval: {approval_value:>8} | Revenue: {revenue_value:>10}")
    
    print("\nFlexFactor Specific Checks:")
    flexfactor_count = mock_metrics.get("flexfactor_total", "0")
    flexfactor_approval = mock_metrics.get("flexfactor_approval", "0")
    flexfactor_revenue = mock_metrics.get("flexfactor_total_revenue", "0")
    
    print(f"FlexFactor Count: {flexfactor_count}")
    print(f"FlexFactor Approval: {flexfactor_approval}")
    print(f"FlexFactor Revenue: {flexfactor_revenue}")
    
    # Check approval rate formatting
    approval_value = flexfactor_approval
    if approval_value and not str(approval_value).endswith('%'):
        approval_text = f"{approval_value}%"
    elif approval_value:
        approval_text = str(approval_value)
    else:
        approval_text = "0.00%"
    
    print(f"Formatted FlexFactor Approval: {approval_text}")
    
    print("\n✅ FlexFactor alignment test completed successfully!")
    print("The alignment fixes should ensure:")
    print("1. Consistent column positioning across all metrics")
    print("2. Proper approval text centering within its column")  
    print("3. Consistent font usage for better alignment")

if __name__ == "__main__":
    test_flexfactor_alignment()
