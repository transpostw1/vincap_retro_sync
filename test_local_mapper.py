#!/usr/bin/env python3
"""
Local test script for APINeonToRetroMapper
Run this locally to test the data transformation logic
"""

import asyncio
import json
from api_mapper import APINeonToRetroMapper

async def test_local_mapper():
    """Test the mapper locally with sample data"""
    
    # Sample data that matches your Neon database structure
    sample_data = {
        'vendor_id': '9bf5f81d-839c-4918-9624-5226646eafb2',
        'org_id': 'd886152a-248f-40a9-bed9-3ac61c35be5a',
        'invoice_type': '99edfd0b-bcfc-4c2f-a745-fabb41f0765d',
        'corresponding_proforma_invoice': '',
        'invoice_no': 'DSAM2526-001416',
        'invoice_date': '2025-09-02',
        'invoice_due_date': '2025-09-07',
        'purchase_order_no': '',
        'received_date': None,
        'office_vessel': '7d178838-3cad-47a3-9a05-7c70ccb453ea',
        'currency': '00098981-b3ec-45bb-bc3c-f29c7cdc07b0',
        'total_amount': '14369.0',
        'additional_costs': '[]',
        'additional_costs_total': '0.00',
        'tax_details': '[{"sgst":0,"cgst":0,"igst":0,"hsn_sac":"996425","tax_rate":0},{"sgst":45,"cgst":45,"igst":0,"hsn_sac":"996425","tax_rate":18}]',
        'tax_details_total': '90.00',
        'igst_total': '0.00',
        'department': '26651c8a-2553-4920-85ea-86140b6a4e33',
        'assignee': '5e419976-2282-4c65-a305-7fef164b5b71',
        'invoice_file': '',
        'supporting_documents': '',
        'id': 43,
        'created_at': '2025-09-02T00:00:00Z',
        'updated_at': '2025-09-02T00:00:00Z'
    }
    
    print("🧪 Testing APINeonToRetroMapper locally...")
    print("=" * 80)
    
    # Create mapper instance (without connecting to databases)
    mapper = APINeonToRetroMapper(
        neon_connection_string="dummy",  # Won't actually connect
        auth_api_url="http://dummy",
        retro_api_url="http://dummy",
        username="dummy",
        password="dummy"
    )
    
    print("📥 Input data:")
    print(f"  total_amount: {sample_data['total_amount']} (type: {type(sample_data['total_amount'])})")
    print(f"  tax_details: {sample_data['tax_details']}")
    print(f"  additional_costs: {sample_data['additional_costs']}")
    print()
    
    # Test the transformation function directly
    print("🔄 Testing data transformation...")
    try:
        transformed_data = mapper._transform_for_retro_api(sample_data)
        
        print("✅ Transformation successful!")
        print()
        
        print("📊 Transformed data structure:")
        print(f"  data field: {transformed_data.get('data', 'NOT_FOUND')}")
        print(f"  masterEdit: {transformed_data.get('masterEdit', 'NOT_FOUND')}")
        print(f"  _gst_data_entries count: {len(transformed_data.get('_gst_data_entries', []))}")
        print(f"  _a_cost_data_entries count: {len(transformed_data.get('_a_cost_data_entries', []))}")
        print()
        
        # Show GST entries
        if '_gst_data_entries' in transformed_data:
            print("💰 GST DATA ENTRIES:")
            for i, gst_entry in enumerate(transformed_data['_gst_data_entries']):
                print(f"    GST[{i}]: {gst_entry}")
            print()
        
        # Show Cost entries
        if '_a_cost_data_entries' in transformed_data:
            print("📦 ADDITIONAL COST ENTRIES:")
            for i, cost_entry in enumerate(transformed_data['_a_cost_data_entries']):
                print(f"    Cost[{i}]: {cost_entry}")
            print()
        
        # Test the form data creation
        print("📤 Testing form data creation...")
        from aiohttp import FormData
        
        form_data = FormData()
        
        # Add regular fields
        for key, value in transformed_data.items():
            if key.startswith('_'):
                continue
            if value is not None and value != "":
                form_data.add_field(key, str(value))
                print(f"  📝 Form field '{key}': {value}")
        
        # Add GST entries
        if '_gst_data_entries' in transformed_data:
            print(f"➕ Adding {len(transformed_data['_gst_data_entries'])} gstData entries")
            for i, gst_entry in enumerate(transformed_data['_gst_data_entries']):
                form_data.add_field('gstData', gst_entry)
                print(f"    ✅ Added gstData[{i}]: {gst_entry}")
        
        # Add Cost entries
        if '_a_cost_data_entries' in transformed_data:
            print(f"➕ Adding {len(transformed_data['_a_cost_data_entries'])} aCostData entries")
            for i, cost_entry in enumerate(transformed_data['_a_cost_data_entries']):
                form_data.add_field('aCostData', cost_entry)
                print(f"    ✅ Added aCostData[{i}]: {cost_entry}")
        
        print()
        print(f"📊 Total form fields: {len(form_data._fields) if hasattr(form_data, '_fields') else 'Unknown'}")
        
    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 Starting local test...")
    asyncio.run(test_local_mapper())
    print("🏁 Test completed!") 