#!/usr/bin/env python3
"""
Debug script to check the data structure being passed to the HTML template
"""
import json
import os
import sys

# Sample data structure based on the code
sample_data = {
    "step1": [
        {
            'week_start_date': '2024-01-01',
            'week_range': 'Jan 1 - Jan 7, 2024',
            'initial_count': 1000,
            'retention_rate': '75%',
            'rebill_cycle1_count': 750,
            'cycle1_retention_rate': '85%'
        },
        {
            'week_start_date': '2023-12-25',
            'week_range': 'Dec 25 - Dec 31, 2023',
            'initial_count': 950,
            'retention_rate': '72%',
            'rebill_cycle1_count': 684,
            'cycle1_retention_rate': '83%'
        },
        {
            'week_start_date': '2023-12-18',
            'week_range': 'Dec 18 - Dec 24, 2023',
            'initial_count': 920,
            'retention_rate': '70%',
            'rebill_cycle1_count': 644,
            'cycle1_retention_rate': '81%'
        },
        {
            'week_start_date': '2023-12-11',
            'week_range': 'Dec 11 - Dec 17, 2023',
            'initial_count': 890,
            'retention_rate': '68%',
            'rebill_cycle1_count': 605,
            'cycle1_retention_rate': '79%'
        }
    ],
    "step1b": [
        {
            'week_start_date': '2024-01-01',
            'week_range': 'Jan 1 - Jan 7, 2024',
            'initial_count': 800,
            'retention_rate': '73%',
            'rebill_cycle1_count': 584,
            'cycle1_retention_rate': '82%'
        }
    ],
    "step2": [
        {
            'week_start_date': '2024-01-01',
            'week_range': 'Jan 1 - Jan 7, 2024',
            'initial_count': 600,
            'retention_rate': '71%',
            'rebill_cycle1_count': 426,
            'cycle1_retention_rate': '80%'
        }
    ],
    "step3": [
        {
            'week_start_date': '2024-01-01',
            'week_range': 'Jan 1 - Jan 7, 2024',
            'initial_count': 400,
            'retention_rate': '69%',
            'rebill_cycle1_count': 276,
            'cycle1_retention_rate': '78%'
        }
    ]
}

print("📋 Sample data structure:")
print(json.dumps(sample_data, indent=2))

print("\n🔍 Checking specific week_range access:")
print(f"Step1 Week 1 range: {sample_data['step1'][0]['week_range']}")
print(f"Step1 Week 2 range: {sample_data['step1'][1]['week_range']}")

print("\n📊 JavaScript equivalent access:")
print("retentionData.step1[0].week_range:", sample_data['step1'][0]['week_range'])
print("retentionData.step1[1].week_range:", sample_data['step1'][1]['week_range'])
