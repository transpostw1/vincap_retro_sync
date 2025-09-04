#!/usr/bin/env python3
"""
Test script that replicates the EXACT working curl command format
"""

import aiohttp
import asyncio
import json
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_exact_working_format():
    """Test using the EXACT working curl format with real Neon data"""
    
    # Use your existing session cookie
    api_url = "http://192.168.1.25:801/InvoiceManager/AddUpdateInvoice"
    
    # Create session with your cookie
    cookies = {'ASP.NET_SessionId': 'gaiwuhwq5vfufbnkyxds0t0c'}
    async with aiohttp.ClientSession(cookies=cookies) as session:
        logger.info("✅ Using your session cookie")
        
        # Step 1: Create form data with EXACT structure from working curl
        logger.info("📝 Creating form data with EXACT working structure...")
        
        form_data = aiohttp.FormData()
        
        # EXACT aCostData entries from working curl - FIXED STRUCTURE
        acost_entries = [
            {"Name":"Cess","HSN_SAC":"","Amount":0,"GSTRate":"","TaxTotal":0,"Total":0,"TaxAmount":0,"externalId":"","AdditionalCost":"1!G!0ff1217d-fcd0-4ec1-9f5b-0543af73d7ed"},
            {"Name":"Courier Charge","HSN_SAC":"TESTING","Amount":2000,"GSTRate":"22!G!906e8b3a-8817-4fc1-8e9c-fc8c5f6b3fbe","TaxTotal":60,"Total":2060,"TaxAmount":0,"externalId":"","AdditionalCost":"1!G!f34767f5-cd75-4aa8-95f7-0e78167fc493"},
            {"Name":"Transportation Charge","HSN_SAC":"","Amount":0,"GSTRate":"","TaxTotal":0,"Total":0,"TaxAmount":0,"externalId":"","AdditionalCost":"1!G!482e3378-f20e-4d68-9b23-6885f8b9aaf5"},
            {"Name":"Delivery Charge","HSN_SAC":"","Amount":0,"GSTRate":"","TaxTotal":0,"Total":0,"TaxAmount":0,"externalId":"","AdditionalCost":"1!G!0a46e8f4-9f8d-401e-b9cb-fd420db1de7b"}
        ]
        
        # Add each aCostData entry
        for entry in acost_entries:
            form_data.add_field('aCostData', json.dumps(entry))
        
        # EXACT main data from working curl - but with real Neon values
        main_data = {
            "Status":"",
            "ApprovalStatus":"",
            "InternalReference":"",
            "DryDockInvoice":False,
            "Date":"2025-07-15T18:30:00.000Z",
            "DueDate":"2025-07-16T18:30:00.000Z", 
            "ReceivedDate":"2025-07-16T18:30:00.000Z",
            "CounterParty":"15!G!717acd53-286b-413f-936e-84b2505a6fe3",  # Real Neon vendor
            "ReferenceNumber":"NEON-EXACT-WORKING-" + datetime.now().strftime('%m%d%H%M%S'),
            "Type":"35!G!4333c68d-28bd-4607-b18c-c97cc6580db5",
            "SubTotal":None,
            "Remark":"Migrated from Neon with EXACT working format",
            "Currency":"17!G!00098981-b3ec-45bb-bc3c-f29c7cdc07b0",
            "Location":"26!G!f35de236-4bdc-48b8-b3d2-9bd5c8273843",
            "Paid":False,
            "Due":False,
            "Overdue":False,
            "PendingAssignment":True,
            "externalId":"",
            "Organization":"28!G!de9c5634-6948-446f-9ff8-8e824994b423",
            "Department":"18!G!b5ae33a7-bf63-426e-95d0-b5c9c9e3803f",
            "TotalAmount":2360,  # Real calculated total from Neon GST data
            "AdditionalCostTotal":2060,  # From courier charge
            "GSTTotal":210,  # 150 (3% of 5000) + 60 (from courier)
            "CostCenter":"14!G!d490d9af-a497-4ea6-b807-cbbeae42c35b",
            "CorrespondingProformaInvoice":"",
            "CorrespondingProformaInvoiceExternalId":"",
            "RCMApplicable":False,
            "CargoType":"9!G!e2c8c531-8387-4e07-afac-24f8727ffa1b",
            "CharterType":"tc",
            "PurchaseOrderId":"CRG/SER/ENG/23/0500-A",
            "PurchaseOrderRetroNETReference":"6c285600-93ac-4bc4-bef5-487c2ca3504e",
            "isServicePurchaseOrder":True,
            "TakeOverExpense":False
        }
        
        form_data.add_field('data', json.dumps(main_data))
        
        # EXACT gstData entries from working curl - FIXED STRUCTURE with real Neon values
        gst_entries = [
            {"Rate":0,"Amount":0,"HSN_SAC":"","TaxTotal":0,"Total":0,"GSTType":"na","IGST":0,"CGST":0,"SGST":0,"externalid":"","GSTRate":"22!G!26fd346e-e9f1-4e50-8c26-05859f753250"},
            {"Rate":3,"Amount":5000,"HSN_SAC":"TESTING","TaxTotal":150,"Total":5150,"GSTType":"na","IGST":0,"CGST":0,"SGST":0,"externalid":"","GSTRate":"22!G!906e8b3a-8817-4fc1-8e9c-fc8c5f6b3fbe"},
            {"Rate":5,"Amount":0,"HSN_SAC":"","TaxTotal":0,"Total":0,"GSTType":"na","IGST":0,"CGST":0,"SGST":0,"externalid":"","GSTRate":"22!G!b0f98850-b5ba-4f71-8536-2121b7c06ad7"},
            {"Rate":12,"Amount":0,"HSN_SAC":"","TaxTotal":0,"Total":0,"GSTType":"na","IGST":0,"CGST":0,"SGST":0,"externalid":"","GSTRate":"22!G!030734df-53ae-4cb9-803d-c8929f11152c"},
            {"Rate":18,"Amount":2000,"HSN_SAC":"996425","TaxTotal":360,"Total":2360,"GSTType":"na","IGST":0,"CGST":180,"SGST":180,"externalid":"","GSTRate":"22!G!01482a49-1aee-4a46-8043-78a74c95b034"},  # Real Neon 18% data
            {"Rate":28,"Amount":0,"HSN_SAC":"","TaxTotal":0,"Total":0,"GSTType":"na","IGST":0,"CGST":0,"SGST":0,"externalid":"","GSTRate":"22!G!96a553ec-2f36-4f3f-a66a-77780c5bb1ec"}
        ]
        
        # Add each gstData entry
        for entry in gst_entries:
            form_data.add_field('gstData', json.dumps(entry))
        
        # EXACT masterEdit from working curl
        form_data.add_field('masterEdit', 'false')
        
        # Step 2: Send the request with EXACT headers
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'http://192.168.1.25:801/InvoiceManager/VendorInvoiceListing',
            'Origin': 'http://192.168.1.25:801',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        logger.info(f"📤 Sending request with reference: {main_data['ReferenceNumber']}")
        logger.info(f"💰 Total Amount: {main_data['TotalAmount']}")
        logger.info(f"🏷️ GST Total: {main_data['GSTTotal']}")
        logger.info(f"💼 Additional Cost Total: {main_data['AdditionalCostTotal']}")
        
        async with session.post(api_url, data=form_data, headers=headers) as response:
            response_text = await response.text()
            logger.info(f"📡 Response Status: {response.status}")
            logger.info(f"📡 Response: {response_text}")
            
            if "Success" in response_text:
                logger.info(f"🎉 SUCCESS! Invoice created with reference: {main_data['ReferenceNumber']}")
                logger.info(f"✅ Check this reference number in Retro - it should show NON-ZERO amounts!")
                print(f"\n🎯 TEST REFERENCE: {main_data['ReferenceNumber']}")
                print(f"Use your curl command to verify amounts!")
                return main_data['ReferenceNumber']
            else:
                logger.error(f"❌ Failed: {response_text}")
                return None

if __name__ == "__main__":
    asyncio.run(test_exact_working_format())