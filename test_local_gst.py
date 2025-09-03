#!/usr/bin/env python3
"""
Local test for GST calculations - no Docker, no database
"""

import json

def test_gst_calculations():
    """Test the GST calculation logic locally"""
    
    print("🧪 Testing GST calculations locally...")
    print("=" * 80)
    
    # Sample data that matches your Neon database structure
    sample_data = {
        'tax_details': '[{"sgst":0,"cgst":0,"igst":0,"hsn_sac":"996425","tax_rate":0},{"sgst":45,"cgst":45,"igst":0,"hsn_sac":"996425","tax_rate":18}]',
        'additional_costs': '[]'
    }
    
    print("📥 Input data:")
    print(f"  tax_details: {sample_data['tax_details']}")
    print(f"  additional_costs: {sample_data['additional_costs']}")
    print()
    
    # Parse the JSON strings
    tax_details = json.loads(sample_data['tax_details'])
    additional_costs = json.loads(sample_data['additional_costs'])
    
    print("🔄 Processing tax_details...")
    
    # Test the calculation logic
    existing_tax_data = {}
    
    for tax in tax_details:
        tax_rate = float(tax.get('tax_rate', 0))
        sgst = float(tax.get('sgst', 0))
        cgst = float(tax.get('cgst', 0))
        igst = float(tax.get('igst', 0))
        
        # Base amount = (SGST + CGST + IGST) / tax_rate * 100
        if tax_rate > 0:
            base_amount = (sgst + cgst + igst) / tax_rate * 100
        else:
            base_amount = 0
        
        # Store the calculated base amount
        tax['calculated_amount'] = base_amount
        existing_tax_data[tax_rate] = tax
        
        print(f"  ✅ Tax rate {tax_rate}%: SGST={sgst}, CGST={cgst}, IGST={igst}, BaseAmount={base_amount}")
    
    print(f"\n📊 Stored tax rates: {list(existing_tax_data.keys())}")
    
    # Test the lookup logic with FLOAT values (the fix we applied)
    required_gst_rates = [0.0, 3.0, 5.0, 12.0, 18.0, 28.0]
    print(f"\n🔍 Testing lookup for required rates: {required_gst_rates}")
    
    gst_entries = []
    
    for rate in required_gst_rates:
        if rate in existing_tax_data:
            tax = existing_tax_data[rate]
            base_amount = tax.get('calculated_amount', 0)
            tax_amount = (base_amount * rate) / 100
            total_amount = base_amount + tax_amount
            
            print(f"  ✅ Found {rate}%: BaseAmount={base_amount}, TaxAmount={tax_amount}, Total={total_amount}")
            
            gst_entry = {
                'Rate': rate,
                'Amount': base_amount,
                'HSN_SAC': tax.get('hsn_sac', ''),
                'TaxTotal': tax_amount,
                'Total': total_amount,
                'GSTType': 'na',
                'IGST': tax.get('igst', 0),
                'CGST': tax.get('cgst', 0),
                'SGST': tax.get('sgst', 0)
            }
            gst_entries.append(gst_entry)
        else:
            print(f"  ⚠️ Rate {rate}% not found, sending empty entry")
            gst_entry = {
                'Rate': rate,
                'Amount': 0,
                'HSN_SAC': '',
                'TaxTotal': 0,
                'Total': 0,
                'GSTType': 'na',
                'IGST': 0,
                'CGST': 0,
                'SGST': 0
            }
            gst_entries.append(gst_entry)
    
    print(f"\n📊 Final GST entries count: {len(gst_entries)}")
    
    # Test the masterEdit field
    print(f"\n🔧 Testing masterEdit field:")
    master_edit = False  # This should be the correct boolean value
    print(f"  masterEdit: {master_edit} (type: {type(master_edit)})")
    print(f"  Is boolean False: {master_edit == False}")
    
    print("\n🏁 Test completed!")
    return gst_entries

if __name__ == "__main__":
    gst_entries = test_gst_calculations() 