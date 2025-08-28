#!/usr/bin/env python3
"""
Test script to verify GST mapping logic
"""

import json
import sys
import os

# Add the current directory to Python path to import api_mapper
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api_mapper import APINeonToRetroMapper

def test_gst_mapping():
    """Test the GST mapping logic with sample data"""
    
    # Sample Neon data that should be processed
    sample_neon_data = {
        'invoice_no': 'TEST-123',
        'total_amount': 5000.00,
        'tax_details': json.dumps([
            {
                'tax_rate': 3,
                'amount': 5000,
                'hsn_sac': 'TESTING',
                'igst': 0,
                'cgst': 75,
                'sgst': 75
            }
        ]),
        'additional_costs': json.dumps([
            {
                'type': 'Courier Charge',
                'amount': 2000,
                'hsn_sac': 'TESTING',
                'tax_rate': 3
            }
        ])
    }
    
    # Create mapper instance (we don't need real connection strings for this test)
    mapper = APINeonToRetroMapper(
        neon_connection_string="test",
        auth_api_url="test",
        retro_api_url="test",
        username="test",
        password="test"
    )
    
    # Test the transformation
    print("🧪 Testing GST Mapping Logic")
    print("=" * 50)
    
    print(f"📥 Input Neon Data:")
    print(f"  Invoice No: {sample_neon_data['invoice_no']}")
    print(f"  Total Amount: {sample_neon_data['total_amount']}")
    print(f"  Tax Details: {sample_neon_data['tax_details']}")
    print(f"  Additional Costs: {sample_neon_data['additional_costs']}")
    print()
    
    # Transform the data
    transformed_data = mapper._transform_for_retro_api(sample_neon_data)
    
    print("📤 Transformed Data:")
    print(f"  Data JSON: {transformed_data.get('data', 'NOT_FOUND')}")
    print()
    
    print("💰 GST Data Entries:")
    for i, gst_entry in enumerate(transformed_data.get('_gst_data_entries', [])):
        gst_data = json.loads(gst_entry)
        print(f"  Entry {i+1}: Rate={gst_data['Rate']}%, Amount={gst_data['Amount']}, TaxTotal={gst_data['TaxTotal']}, Total={gst_data['Total']}")
    print()
    
    print("📦 Additional Cost Entries:")
    for i, cost_entry in enumerate(transformed_data.get('_a_cost_data_entries', [])):
        cost_data = json.loads(cost_entry)
        print(f"  Entry {i+1}: Name={cost_data['Name']}, Amount={cost_data['Amount']}, TaxTotal={cost_data['TaxTotal']}, Total={cost_data['Total']}")
    print()
    
    # Verify calculations
    print("✅ Verification:")
    
    # Check GST calculations
    gst_entries = [json.loads(entry) for entry in transformed_data.get('_gst_data_entries', [])]
    for gst in gst_entries:
        if gst['Rate'] == 3 and gst['Amount'] == 5000:
            expected_tax = (5000 * 3) / 100  # 150
            expected_total = 5000 + expected_tax  # 5150
            if gst['TaxTotal'] == expected_tax and gst['Total'] == expected_total:
                print(f"  ✅ GST 3% calculation correct: {gst['Amount']} + {gst['TaxTotal']} = {gst['Total']}")
            else:
                print(f"  ❌ GST 3% calculation wrong: Expected TaxTotal={expected_tax}, Total={expected_total}, Got TaxTotal={gst['TaxTotal']}, Total={gst['Total']}")
    
    # Check Additional Cost calculations
    cost_entries = [json.loads(entry) for entry in transformed_data.get('_a_cost_data_entries', [])]
    for cost in cost_entries:
        if cost['Name'] == 'Courier Charge' and cost['Amount'] == 2000:
            expected_tax = (2000 * 3) / 100  # 60
            expected_total = 2000 + expected_tax  # 2060
            if cost['TaxTotal'] == expected_tax and cost['Total'] == expected_total:
                print(f"  ✅ Courier Charge calculation correct: {cost['Amount']} + {cost['TaxTotal']} = {cost['Total']}")
            else:
                print(f"  ❌ Courier Charge calculation wrong: Expected TaxTotal={expected_tax}, Total={expected_total}, Got TaxTotal={cost['TaxTotal']}, Total={cost['Total']}")

if __name__ == "__main__":
    test_gst_mapping() 