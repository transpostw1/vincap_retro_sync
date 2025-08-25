#!/usr/bin/env python3
"""
Test script to send made-up data to Retro API
This will help us verify the API integration is working correctly.
"""

import asyncio
import aiohttp
import json
import logging
from datetime import datetime
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RetroAPITester:
    def __init__(self):
        self.auth_api_url = "http://192.168.1.25:801"
        self.retro_api_url = "http://192.168.1.25:801"
        self.username = "akshay"
        self.password = "retroinv@123"
        self.session_cookies = None

    async def authenticate(self):
        """Authenticate with the Retro API"""
        try:
            async with aiohttp.ClientSession() as session:
                auth_url = f"{self.auth_api_url}/Authentication/AuthenticateUser?userName={self.username}&password={self.password}"
                
                headers = {
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'keep-alive',
                    'Origin': self.auth_api_url,
                    'Referer': f'{self.auth_api_url}/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
                }
                
                async with session.post(auth_url, headers=headers) as response:
                    response_text = await response.text()
                    logger.info(f"Auth response: {response_text}")
                    
                    # Parse response manually due to content-type issues
                    try:
                        auth_data = json.loads(response_text)
                        if auth_data and auth_data[0].get('response') == True:
                            self.session_cookies = response.cookies
                            logger.info("Successfully authenticated with Retro API")
                            return True
                    except:
                        pass
                    
                    logger.error("Authentication failed")
                    return False
                    
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False

    def generate_mock_data(self):
        """Generate mock invoice data with complete field mapping"""
        # Generate a unique reference number
        ref_number = f"TEST-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"
        
        # Use the exact same field structure as the working example
        mock_data = {
            'data': {
                'Status': '',  # Use empty string like original
                'ApprovalStatus': '',  # Use empty string like original
                'InternalReference': '',  # Use empty string like original
                'DryDockInvoice': False,
                'Date': '2025-08-25T00:00:00.000Z',
                'DueDate': '2025-09-25T00:00:00.000Z',
                'ReceivedDate': '2025-08-25T00:00:00.000Z',
                'CounterParty': '15!G!717acd53-286b-413f-936e-84b2505a6fe3',
                'ReferenceNumber': ref_number,
                'Type': '35!G!4333c68d-28bd-4607-b18c-c97cc6580db5',
                'SubTotal': None,  # Use null like original
                'Remark': 'Test invoice created by API integration test - Complete field mapping',
                'Currency': '17!G!00098981-b3ec-45bb-bc3c-f29c7cdc07b0',
                'Location': '26!G!f35de236-4bdc-48b8-b3d2-9bd5c8273843',
                'Paid': False,
                'Due': False,
                'Overdue': False,
                'PendingAssignment': True,
                'externalId': '',  # Use empty string like original
                'Organization': '28!G!de9c5634-6948-446f-9ff8-8e824994b423',
                'Department': '18!G!b5ae33a7-bf63-426e-95d0-b5c9c9e3803f',
                'TotalAmount': 7210,  # Use same amount as original
                'AdditionalCostTotal': None,  # Use null like original
                'GSTTotal': None,  # Use null like original
                'CostCenter': '14!G!d490d9af-a497-4ea6-b807-cbbeae42c35b',
                'CorrespondingProformaInvoice': '',  # Use empty string like original
                'CorrespondingProformaInvoiceExternalId': '',  # Use empty string like original
                'RCMApplicable': False,
                'CargoType': '9!G!e2c8c531-8387-4e07-afac-24f8727ffa1b',
                'CharterType': 'tc',
                'PurchaseOrderId': 'CRG/SER/ENG/23/0500-A',  # Use same as original
                'PurchaseOrderRetroNETReference': '6c285600-93ac-4bc4-bef5-487c2ca3504e',  # Use same as original
                'isServicePurchaseOrder': True,
                'TakeOverExpense': False
            },
            'masterEdit': 'false'
        }
        
        # Convert data to JSON string
        mock_data['data'] = json.dumps(mock_data['data'])
        
        # Add GST data entries with proper amounts
        gst_data_entries = []
        gst_entries = [
            {'Rate': 0, 'Amount': 0, 'TaxTotal': 0, 'Total': 0},
            {'Rate': 3, 'Amount': 5000, 'TaxTotal': 150, 'Total': 5150},
            {'Rate': 5, 'Amount': 0, 'TaxTotal': 0, 'Total': 0},
            {'Rate': 12, 'Amount': 0, 'TaxTotal': 0, 'Total': 0},
            {'Rate': 18, 'Amount': 0, 'TaxTotal': 0, 'Total': 0},
            {'Rate': 28, 'Amount': 0, 'TaxTotal': 0, 'Total': 0}
        ]
        
        for entry in gst_entries:
            gst_data_entries.append(json.dumps({
                'Rate': entry['Rate'],
                'Amount': entry['Amount'],
                'HSN_SAC': 'TESTING' if entry['Rate'] == 3 else '',
                'TaxTotal': entry['TaxTotal'],
                'Total': entry['Total'],
                'GSTType': 'na',
                'IGST': 0,
                'CGST': 0,
                'SGST': 0,
                'externalid': '',
                'GSTRate': f"22!G!{uuid.uuid4()}"
            }))
        
        # Add additional cost data entries with proper amounts
        a_cost_data_entries = []
        test_costs = [
            {'Name': 'Cess', 'Amount': 0, 'TaxTotal': 0, 'Total': 0},
            {'Name': 'Courier Charge', 'Amount': 2000, 'TaxTotal': 60, 'Total': 2060},
            {'Name': 'Transportation Charge', 'Amount': 0, 'TaxTotal': 0, 'Total': 0},
            {'Name': 'Delivery Charge', 'Amount': 0, 'TaxTotal': 0, 'Total': 0}
        ]
        
        for cost in test_costs:
            a_cost_data_entries.append(json.dumps({
                'Name': cost['Name'],
                'HSN_SAC': 'TESTING' if cost['Name'] == 'Courier Charge' else '',
                'Amount': cost['Amount'],
                'GSTRate': '22!G!906e8b3a-8817-4fc1-8e9c-fc8c5f6b3fbe' if cost['Name'] == 'Courier Charge' else '',
                'TaxTotal': cost['TaxTotal'],
                'Total': cost['Total'],
                'TaxAmount': 0,
                'externalId': '',
                'AdditionalCost': f"1!G!{uuid.uuid4()}"
            }))
        
        return {
            'data': mock_data['data'],
            'masterEdit': mock_data['masterEdit'],
            '_gst_data_entries': gst_data_entries,
            '_a_cost_data_entries': a_cost_data_entries,
            'reference_number': ref_number
        }

    async def send_test_data(self, test_data):
        """Send test data to Retro API"""
        try:
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                'Referer': f'{self.retro_api_url}/InvoiceManager/VendorInvoiceListing',
                'Origin': self.retro_api_url,
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            # Prepare form data
            form_data = aiohttp.FormData()
            
            # Add main data
            form_data.add_field('data', test_data['data'])
            form_data.add_field('masterEdit', test_data['masterEdit'])
            
            # Add GST data entries
            logger.info(f"Adding {len(test_data['_gst_data_entries'])} gstData entries")
            for gst_entry in test_data['_gst_data_entries']:
                form_data.add_field('gstData', gst_entry)
            
            # Add additional cost data entries
            logger.info(f"Adding {len(test_data['_a_cost_data_entries'])} aCostData entries")
            for cost_entry in test_data['_a_cost_data_entries']:
                form_data.add_field('aCostData', cost_entry)
            
            # Log what we're sending
            logger.info(f"Sending test data with ReferenceNumber: {test_data['reference_number']}")
            logger.info(f"Data JSON: {test_data['data']}")
            
            async with aiohttp.ClientSession(cookies=self.session_cookies) as session:
                async with session.post(
                    f"{self.retro_api_url}/InvoiceManager/AddUpdateInvoice",
                    headers=headers,
                    data=form_data
                ) as response:
                    response_text = await response.text()
                    logger.info(f"API Response: Status={response.status}, Response={response_text}")
                    
                    if response.status == 200 or response.status == 201:
                        logger.info(f"✅ Successfully sent test data to API: {test_data['reference_number']}")
                        return True, response_text
                    else:
                        logger.error(f"❌ API request failed with status {response.status}: {response_text}")
                        return False, response_text
                        
        except Exception as e:
            logger.error(f"❌ Error sending test data to API: {e}")
            return False, str(e)

async def main():
    """Main test function"""
    logger.info("🧪 Starting Retro API Test with Complete Mock Data")
    
    tester = RetroAPITester()
    
    # Step 1: Authenticate
    logger.info("🔐 Step 1: Authenticating...")
    auth_success = await tester.authenticate()
    if not auth_success:
        logger.error("❌ Authentication failed. Cannot proceed with test.")
        return
    
    # Step 2: Generate test data
    logger.info("📝 Step 2: Generating complete test data...")
    test_data = tester.generate_mock_data()
    
    # Step 3: Send test data
    logger.info("🚀 Step 3: Sending test data to API...")
    success, response = await tester.send_test_data(test_data)
    
    # Step 4: Results
    if success:
        logger.info("🎉 Test completed successfully!")
        logger.info(f"📋 Test Reference Number: {test_data['reference_number']}")
        logger.info("💡 You can now check the Retro system for this test record.")
        logger.info("🔍 This test includes complete field mapping that should appear in the application.")
    else:
        logger.error("❌ Test failed!")
        logger.error(f"🔍 Response: {response}")

if __name__ == "__main__":
    asyncio.run(main()) 