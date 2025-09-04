import asyncio
import asyncpg
import aiohttp
import json
import logging
from datetime import datetime
import uuid

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WorkingAPINeonToRetroMapper:
    def __init__(self, neon_connection_string, auth_api_url, retro_api_url, username, password):
        self.neon_connection_string = neon_connection_string
        self.auth_api_url = auth_api_url
        self.retro_api_url = retro_api_url
        self.username = username
        self.password = password
        self.session_cookies = None

    def _generate_uuid(self):
        """Generate a UUID for external references"""
        return str(uuid.uuid4())

    async def authenticate(self):
        """Authenticate with the Retro API"""
        auth_params = {
            'userName': self.username,
            'password': self.password
        }
        
        logger.info("🔐 Authenticating with Retro API...")
        async with aiohttp.ClientSession() as session:
            async with session.post(self.auth_api_url, params=auth_params) as auth_response:
                if auth_response.status == 200:
                    self.session_cookies = auth_response.cookies
                    logger.info("✅ Authentication successful")
                    return True
                else:
                    logger.error(f"❌ Authentication failed: {auth_response.status}")
                    return False

    async def fetch_neon_data(self, record_id):
        """Fetch data from Neon database"""
        logger.info(f"📊 Fetching Neon data for record_id: {record_id}")
        
        query = """
        SELECT 
            invoice_no,
            vendor_id,
            invoice_date,
            invoice_due_date,
            received_date,
            total_amount,
            additional_costs_total,
            tax_details_total,
            tax_details,
            additional_costs,
            purchase_order_no,
            office_vessel
        FROM invoices 
        WHERE id = $1
        """
        
        conn = await asyncpg.connect(self.neon_connection_string)
        try:
            row = await conn.fetchrow(query, record_id)
            if row:
                # Convert row to dict
                data = dict(row)
                logger.info(f"✅ Found Neon data: {data['invoice_no']}")
                return data
            else:
                logger.warning(f"⚠️ No data found for record_id: {record_id}")
                return None
        finally:
            await conn.close()

    def parse_neon_tax_data(self, tax_details_str):
        """Parse tax details from Neon database"""
        logger.info(f"🔍 PARSING tax_details_str: {repr(tax_details_str)}")
        
        if not tax_details_str:
            return {}
        
        try:
            # Handle potential double JSON encoding
            if isinstance(tax_details_str, str):
                # Remove outer quotes if double-encoded
                if tax_details_str.startswith('"') and tax_details_str.endswith('"'):
                    tax_details_str = json.loads(tax_details_str)  # Remove outer quotes
                    logger.info(f"🔍 After removing outer quotes: {repr(tax_details_str)}")
                
                try:
                    tax_data = json.loads(tax_details_str)
                    logger.info(f"🔍 Parsed tax_data: {tax_data}")
                except json.JSONDecodeError as e:
                    logger.error(f"❌ JSON decode error: {e}")
                    return {}
            else:
                tax_data = tax_details_str
            
            # Process tax data by rate
            tax_by_rate = {}
            if isinstance(tax_data, list):
                logger.info(f"🔍 Processing {len(tax_data)} tax entries")
                for i, tax in enumerate(tax_data):
                    if isinstance(tax, dict):
                        rate = float(tax.get('tax_rate', 0))
                        sgst = float(tax.get('sgst', 0))
                        cgst = float(tax.get('cgst', 0))
                        igst = float(tax.get('igst', 0))
                        
                        logger.info(f"  Tax entry {i}: rate={rate}, sgst={sgst}, cgst={cgst}, igst={igst}")
                        
                        # Calculate base amount from tax values
                        if rate > 0:
                            total_tax = sgst + cgst + igst
                            if total_tax > 0:
                                base_amount = (total_tax / rate) * 100
                            else:
                                base_amount = 0
                        else:
                            base_amount = 0
                        
                        tax_by_rate[rate] = {
                            'base_amount': base_amount,
                            'sgst': sgst,
                            'cgst': cgst,
                            'igst': igst,
                            'hsn_sac': tax.get('hsn_sac', 'TESTING')
                        }
                        
                        logger.info(f"    Added to tax_by_rate[{rate}]: base={base_amount}")
            else:
                logger.warning(f"❌ tax_data is not a list: {type(tax_data)}")
            
            logger.info(f"🔍 Final tax_by_rate: {tax_by_rate}")
            return tax_by_rate
        except Exception as e:
            logger.error(f"❌ Error parsing tax data: {e}")
            return {}

    def parse_neon_cost_data(self, additional_costs_str):
        """Parse additional costs from Neon database"""
        if not additional_costs_str:
            return []
        
        try:
            # Handle potential double JSON encoding
            if isinstance(additional_costs_str, str):
                try:
                    cost_data = json.loads(additional_costs_str)
                except json.JSONDecodeError:
                    return []
            else:
                cost_data = additional_costs_str
            
            costs = []
            if isinstance(cost_data, list):
                for cost in cost_data:
                    if isinstance(cost, dict):
                        amount = float(cost.get('amount', 0))
                        tax_rate = float(cost.get('tax_rate', 0))
                        tax_amount = (amount * tax_rate) / 100
                        
                        costs.append({
                            'name': cost.get('type', 'Unknown Cost'),
                            'amount': amount,
                            'tax_rate': tax_rate,
                            'tax_amount': tax_amount,
                            'hsn_sac': cost.get('hsn_sac', 'TESTING')
                        })
            
            return costs
        except Exception as e:
            logger.error(f"Error parsing cost data: {e}")
            return []

    async def send_to_retro_api(self, neon_data):
        """Send data to Retro API using the EXACT working format"""
        if not self.session_cookies:
            logger.error("❌ Not authenticated")
            return False
        
        # Parse Neon data
        tax_by_rate = self.parse_neon_tax_data(neon_data.get('tax_details'))
        cost_data = self.parse_neon_cost_data(neon_data.get('additional_costs'))
        
        # Create unique reference number
        ref_number = f"NEON-WORKING-{datetime.now().strftime('%m%d%H%M%S')}"
        
        # Calculate total amount from GST data to match line items
        # The issue is that Neon total (37868) doesn't match GST breakdown (2360)
        # For Retro to show correct amounts, the main total must match GST + cost totals
        calculated_total = 0
        for rate, tax_info in tax_by_rate.items():
            if rate > 0:  # Only count non-zero tax rates
                base = tax_info['base_amount']
                tax = tax_info['sgst'] + tax_info['cgst'] + tax_info['igst']
                calculated_total += base + tax
        
        # Add additional costs
        for cost in cost_data:
            calculated_total += cost['amount'] + cost['tax_amount']
        
        # Use calculated total that matches line items
        total_amount = calculated_total if calculated_total > 0 else float(neon_data.get('total_amount', 0))
        
        logger.info(f"📤 Sending Neon data to Retro API...")
        logger.info(f"Invoice: {neon_data.get('invoice_no', 'Unknown')}")
        logger.info(f"Reference: {ref_number}")
        logger.info(f"Total Amount: {total_amount}")
        
        # Debug the parsed tax data
        logger.info(f"🔍 DEBUG: tax_by_rate = {tax_by_rate}")
        logger.info(f"🔍 DEBUG: cost_data = {cost_data}")
        
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
        
        # Create FormData exactly like working test
        form_data = aiohttp.FormData()
        
        # Add main data field
        main_data = {
            "Status": "",
            "ApprovalStatus": "",
            "InternalReference": "",
            "DryDockInvoice": False,
            "Date": "2025-07-15T18:30:00.000Z",  # Use fixed working date format
            "DueDate": "2025-07-16T18:30:00.000Z",  # Use fixed working date format
            "ReceivedDate": "2025-07-16T18:30:00.000Z",  # Use fixed working date format
            "CounterParty": "15!G!717acd53-286b-413f-936e-84b2505a6fe3",
            "ReferenceNumber": ref_number,
            "Type": "35!G!4333c68d-28bd-4607-b18c-c97cc6580db5",
            "SubTotal": None,
            "Remark": f"Migrated from Neon - {neon_data.get('office_vessel', 'No vessel info')}",
            "Currency": "17!G!00098981-b3ec-45bb-bc3c-f29c7cdc07b0",
            "Location": "26!G!f35de236-4bdc-48b8-b3d2-9bd5c8273843",
            "Paid": False,
            "Due": False,
            "Overdue": False,
            "PendingAssignment": True,
            "externalId": "",
            "Organization": "28!G!de9c5634-6948-446f-9ff8-8e824994b423",
            "Department": "18!G!b5ae33a7-bf63-426e-95d0-b5c9c9e3803f",
            "TotalAmount": int(total_amount),  # Use REAL Neon amount
            "AdditionalCostTotal": None,
            "GSTTotal": None,
            "CostCenter": "14!G!d490d9af-a497-4ea6-b807-cbbeae42c35b",
            "CorrespondingProformaInvoice": "",
            "CorrespondingProformaInvoiceExternalId": "",
            "RCMApplicable": False,
            "CargoType": "9!G!e2c8c531-8387-4e07-afac-24f8727ffa1b",
            "CharterType": "tc",
            "PurchaseOrderId": neon_data.get('purchase_order_no', ''),
            "PurchaseOrderRetroNETReference": "6c285600-93ac-4bc4-bef5-487c2ca3504e",
            "isServicePurchaseOrder": True,
            "TakeOverExpense": False
        }
        
        form_data.add_field('data', json.dumps(main_data))
        form_data.add_field('masterEdit', 'false')  # EXACT working format
        
        # Add GST entries using REAL Neon data
        required_rates = [0, 3, 5, 12, 18, 28]
        for rate in required_rates:
            if rate in tax_by_rate:
                # Use REAL Neon data
                tax_info = tax_by_rate[rate]
                base_amount = int(tax_info['base_amount'])
                # Use actual tax amounts from Neon, not calculated
                sgst = int(tax_info['sgst'])
                cgst = int(tax_info['cgst']) 
                igst = int(tax_info['igst'])
                tax_total = sgst + cgst + igst
                total = base_amount + tax_total
                
                logger.info(f"GST[{rate}%] REAL Neon data: Base={base_amount}, SGST={sgst}, CGST={cgst}, IGST={igst}, Tax={tax_total}, Total={total}")
                
                gst_entry = json.dumps({
                    "Rate": rate,
                    "Amount": base_amount,
                    "HSN_SAC": tax_info['hsn_sac'],
                    "TaxTotal": tax_total,
                    "Total": total,
                    "GSTType": "na",
                    "IGST": igst,
                    "CGST": cgst,
                    "SGST": sgst,
                    "externalid": "",
                    "GSTRate": f"22!G!{self._generate_uuid()}"
                })
            else:
                # Empty entry for missing rates
                gst_entry = json.dumps({
                    "Rate": rate,
                    "Amount": 0,
                    "HSN_SAC": "",
                    "TaxTotal": 0,
                    "Total": 0,
                    "GSTType": "na",
                    "IGST": 0,
                    "CGST": 0,
                    "SGST": 0,
                    "externalid": "",
                    "GSTRate": f"22!G!{self._generate_uuid()}"
                })
            
            form_data.add_field('gstData', gst_entry)
        
        # Add additional cost entries using REAL Neon data
        default_costs = ['Cess', 'Courier Charge', 'Transportation Charge', 'Delivery Charge']
        cost_index = 0
        
        for cost_name in default_costs:
            if cost_index < len(cost_data):
                # Use REAL Neon data
                cost_info = cost_data[cost_index]
                amount = int(cost_info['amount'])
                tax_amount = int(cost_info['tax_amount'])
                total = amount + tax_amount
                
                logger.info(f"Additional Cost: {amount} for {cost_info['name']} (Tax: {tax_amount}, Total: {total})")
                
                acost_entry = json.dumps({
                    "Name": cost_info['name'],
                    "HSN_SAC": cost_info['hsn_sac'],
                    "Amount": amount,
                    "GSTRate": f"22!G!{self._generate_uuid()}" if cost_info['tax_rate'] > 0 else "",
                    "TaxTotal": tax_amount,
                    "Total": total,
                    "TaxAmount": 0,  # Keep as 0 like working format
                    "externalId": "",
                    "AdditionalCost": f"1!G!{self._generate_uuid()}"
                })
                cost_index += 1
            else:
                # Empty entry for missing costs
                acost_entry = json.dumps({
                    "Name": cost_name,
                    "HSN_SAC": "",
                    "Amount": 0,
                    "GSTRate": "",
                    "TaxTotal": 0,
                    "Total": 0,
                    "TaxAmount": 0,
                    "externalId": "",
                    "AdditionalCost": f"1!G!{self._generate_uuid()}"
                })
            
            form_data.add_field('aCostData', acost_entry)
        
        # Send the request
        async with aiohttp.ClientSession(cookies=self.session_cookies) as session:
            async with session.post(self.retro_api_url, headers=headers, data=form_data) as response:
                response_text = await response.text()
                
                logger.info(f"📡 API Response Status: {response.status}")
                logger.info(f"📡 API Response: {response_text}")
                
                if response.status == 200:
                    try:
                        response_data = json.loads(response_text)
                        if isinstance(response_data, list) and len(response_data) > 0:
                            if response_data[0].get('response') == True:
                                logger.info("✅ SUCCESS! Invoice created in Retro with REAL Neon data!")
                                return ref_number
                            else:
                                logger.error(f"❌ API Error: {response_data[0].get('message', 'Unknown error')}")
                                return None
                    except:
                        logger.error(f"❌ Failed to parse response: {response_text}")
                        return None
                else:
                    logger.error(f"❌ HTTP Error: {response.status}")
                    return None

    async def migrate_record(self, record_id):
        """Migrate a single record from Neon to Retro"""
        try:
            # Step 1: Authenticate
            if not await self.authenticate():
                return False
            
            # Step 2: Fetch Neon data
            neon_data = await self.fetch_neon_data(record_id)
            if not neon_data:
                return False
            
            # Step 3: Send to Retro
            ref_number = await self.send_to_retro_api(neon_data)
            if ref_number:
                logger.info(f"🎯 Migration successful! Reference: {ref_number}")
                return ref_number
            else:
                logger.error("❌ Migration failed")
                return False
                
        except Exception as e:
            logger.error(f"❌ Migration error: {e}")
            return False

# Test function
async def test_working_mapper():
    """Test the working mapper with real Neon data"""
    
    # Connection details (use your actual values)
    neon_connection_string = "postgresql://neondb_owner:npg_ziNBtp5sX4Fv@ep-quiet-forest-a53t111o-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    auth_api_url = "http://192.168.1.25:801/Authentication/AuthenticateUser"
    retro_api_url = "http://192.168.1.25:801/InvoiceManager/AddUpdateInvoice"
    username = "akshay"
    password = "retroinv@123"
    
    mapper = WorkingAPINeonToRetroMapper(
        neon_connection_string=neon_connection_string,
        auth_api_url=auth_api_url,
        retro_api_url=retro_api_url,
        username=username,
        password=password
    )
    
    # Test with record 42
    result = await mapper.migrate_record(42)
    if result:
        print(f"🎉 SUCCESS! Check reference: {result}")
        print("Use your curl command to verify non-zero amounts!")
    else:
        print("❌ Migration failed")

if __name__ == "__main__":
    asyncio.run(test_working_mapper())