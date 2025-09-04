import asyncio
import asyncpg
import json

async def debug_neon_data():
    """Debug the actual Neon data structure for record 42"""
    
    neon_connection_string = "postgresql://neondb_owner:npg_ziNBtp5sX4Fv@ep-quiet-forest-a53t111o-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    
    query = """
    SELECT 
        invoice_no,
        total_amount,
        tax_details,
        additional_costs,
        tax_details_total,
        additional_costs_total
    FROM invoices 
    WHERE id = 42
    """
    
    conn = await asyncpg.connect(neon_connection_string)
    try:
        row = await conn.fetchrow(query)
        if row:
            print(f"🔍 NEON DATA ANALYSIS FOR RECORD 42:")
            print(f"Invoice: {row['invoice_no']}")
            print(f"Total Amount: {row['total_amount']}")
            print(f"Tax Details Total: {row['tax_details_total']}")
            print(f"Additional Costs Total: {row['additional_costs_total']}")
            print()
            
            print(f"🔍 RAW tax_details: {repr(row['tax_details'])}")
            print(f"🔍 RAW additional_costs: {repr(row['additional_costs'])}")
            print()
            
            # Parse tax_details
            try:
                tax_str = row['tax_details']
                if isinstance(tax_str, str):
                    # Handle double JSON encoding
                    if tax_str.startswith('"') and tax_str.endswith('"'):
                        tax_str = json.loads(tax_str)  # Remove outer quotes
                    
                    tax_data = json.loads(tax_str)
                    print(f"✅ PARSED tax_details: {json.dumps(tax_data, indent=2)}")
                    
                    # Calculate amounts
                    for i, tax in enumerate(tax_data):
                        rate = tax.get('tax_rate', 0)
                        sgst = tax.get('sgst', 0)
                        cgst = tax.get('cgst', 0) 
                        igst = tax.get('igst', 0)
                        total_tax = sgst + cgst + igst
                        
                        if rate > 0 and total_tax > 0:
                            base_amount = (total_tax / rate) * 100
                            print(f"  Tax Entry {i}: Rate={rate}%, SGST={sgst}, CGST={cgst}, IGST={igst}")
                            print(f"    Total Tax: {total_tax}, Calculated Base: {base_amount}")
                        else:
                            print(f"  Tax Entry {i}: Rate={rate}%, No tax amounts")
                            
            except Exception as e:
                print(f"❌ Error parsing tax_details: {e}")
            
            print()
            
            # Parse additional_costs  
            try:
                cost_str = row['additional_costs']
                if isinstance(cost_str, str):
                    # Handle double JSON encoding
                    if cost_str.startswith('"') and cost_str.endswith('"'):
                        cost_str = json.loads(cost_str)  # Remove outer quotes
                    
                    cost_data = json.loads(cost_str)
                    print(f"✅ PARSED additional_costs: {json.dumps(cost_data, indent=2)}")
                    
                    if cost_data:
                        for i, cost in enumerate(cost_data):
                            amount = cost.get('amount', 0)
                            tax_rate = cost.get('tax_rate', 0)
                            cost_type = cost.get('type', 'Unknown')
                            print(f"  Cost Entry {i}: Type={cost_type}, Amount={amount}, Tax Rate={tax_rate}%")
                    else:
                        print("  No additional costs found")
                            
            except Exception as e:
                print(f"❌ Error parsing additional_costs: {e}")
                
        else:
            print("❌ No record found for ID 42")
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(debug_neon_data())