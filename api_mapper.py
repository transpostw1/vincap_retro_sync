#!/usr/bin/env python3
"""
UPDATED API MAPPER WITH WORKING SOLUTION
Combines the proven FIXED DATA STRUCTURE approach with the expected API interface
"""

import aiohttp
import asyncio
import json
import asyncpg
from datetime import datetime
import logging
from decimal import Decimal

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class APINeonToRetroMapper:
    """API-based data mapper that sends data to Retro API endpoint using FIXED DATA STRUCTURE"""
    
    def __init__(self, neon_connection_string: str, auth_api_url: str, retro_api_url: str, username: str, password: str):
        self.neon_connection_string = neon_connection_string
        self.auth_api_url = auth_api_url
        self.retro_api_url = f"{retro_api_url}/InvoiceManager/AddUpdateInvoice"  # Full endpoint
        self.username = username
        self.password = password
        self.neon_conn = None
        self.auth_token = None
        self.session_cookies = None
        
        # Fixed Retro data structures (CRITICAL - DO NOT CHANGE!)
        self.fixed_gst_rates = [
            {"Rate": 0, "GSTRate": "22!G!26fd346e-e9f1-4e50-8c26-05859f753250"},
            {"Rate": 3, "GSTRate": "22!G!906e8b3a-8817-4fc1-8e9c-fc8c5f6b3fbe"},
            {"Rate": 5, "GSTRate": "22!G!b0f98850-b5ba-4f71-8536-2121b7c06ad7"},
            {"Rate": 12, "GSTRate": "22!G!030734df-53ae-4cb9-803d-c8929f11152c"},
            {"Rate": 18, "GSTRate": "22!G!01482a49-1aee-4a46-8043-78a74c95b034"},
            {"Rate": 28, "GSTRate": "22!G!96a553ec-2f36-4f3f-a66a-77780c5bb1ec"}
        ]
        
        self.fixed_cost_types = [
            {"Name": "Cess", "AdditionalCost": "1!G!0ff1217d-fcd0-4ec1-9f5b-0543af73d7ed"},
            {"Name": "Courier Charge", "AdditionalCost": "1!G!f34767f5-cd75-4aa8-95f7-0e78167fc493"},
            {"Name": "Transportation Charge", "AdditionalCost": "1!G!482e3378-f20e-4d68-9b23-6885f8b9aaf5"},
            {"Name": "Delivery Charge", "AdditionalCost": "1!G!0a46e8f4-9f8d-401e-b9cb-fd420db1de7b"}
        ]
        
        # API server compatibility attributes
        self.field_mappings = {
            "vendor_id": "vendor_id",
            "org_id": "org_id", 
            "invoice_type": "invoice_type",
            "invoice_no": "invoice_no",
            "invoice_date": "invoice_date",
            "invoice_due_date": "invoice_due_date",
            "total_amount": "total_amount"
        }
        self.special_fields = {}
        self.date_fields = ["invoice_date", "invoice_due_date", "received_date"]
        self.numeric_fields = ["total_amount"]
        self.json_fields = ["tax_details", "additional_costs"]
    
    def parse_neon_tax_data(self, tax_details_str):
        """Parse tax details from Neon (handles double JSON encoding)"""
        if not tax_details_str:
            return {}
            
        try:
            # Handle double JSON encoding
            if isinstance(tax_details_str, str) and tax_details_str.startswith('"') and tax_details_str.endswith('"'):
                tax_details_str = json.loads(tax_details_str)
            
            tax_data = json.loads(tax_details_str) if isinstance(tax_details_str, str) else tax_details_str
            
            if not isinstance(tax_data, list):
                return {}
            
            # Group by tax rate and calculate base amounts
            tax_by_rate = {}
            for entry in tax_data:
                rate = float(entry.get('tax_rate', 0))
                if rate > 0:  # Only process non-zero rates
                    sgst = float(entry.get('sgst', 0))
                    cgst = float(entry.get('cgst', 0))
                    igst = float(entry.get('igst', 0))
                    total_tax = sgst + cgst + igst
                    
                    if total_tax > 0:
                        base_amount = total_tax / (rate / 100)
                        tax_by_rate[rate] = {
                            'base_amount': base_amount,
                            'sgst': sgst,
                            'cgst': cgst, 
                            'igst': igst,
                            'hsn_sac': entry.get('hsn_sac', ''),
                            'tax_total': total_tax,
                            'total': base_amount + total_tax
                        }
            
            return tax_by_rate
            
        except Exception as e:
            logger.error(f"Error parsing tax data: {e}")
            return {}
    
    def parse_neon_cost_data(self, cost_details_str):
        """Parse additional costs from Neon"""
        if not cost_details_str:
            return []
            
        try:
            # Handle double JSON encoding
            if isinstance(cost_details_str, str) and cost_details_str.startswith('"') and cost_details_str.endswith('"'):
                cost_details_str = json.loads(cost_details_str)
            
            cost_data = json.loads(cost_details_str) if isinstance(cost_details_str, str) else cost_details_str
            
            if not isinstance(cost_data, list):
                return []
            
            return cost_data
            
        except Exception as e:
            logger.error(f"Error parsing cost data: {e}")
            return []
    
    def create_fixed_gst_entries(self, neon_tax_data):
        """Create fixed GST entries using Retro's expected structure"""
        gst_entries = []
        
        for rate_info in self.fixed_gst_rates:
            rate = rate_info["Rate"]
            
            if rate in neon_tax_data:
                # Use real Neon data
                neon_data = neon_tax_data[rate]
                entry = {
                    "Rate": rate,
                    "Amount": int(neon_data['base_amount']),
                    "HSN_SAC": neon_data['hsn_sac'],
                    "TaxTotal": int(neon_data['tax_total']),
                    "Total": int(neon_data['total']),
                    "GSTType": "na",
                    "IGST": int(neon_data['igst']),
                    "CGST": int(neon_data['cgst']),
                    "SGST": int(neon_data['sgst']),
                    "externalid": "",
                    "GSTRate": rate_info["GSTRate"]
                }
            else:
                # Zero entry for missing rates
                entry = {
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
                    "GSTRate": rate_info["GSTRate"]
                }
            
            gst_entries.append(entry)
        
        return gst_entries
    
    def create_fixed_cost_entries(self, neon_cost_data):
        """Create fixed cost entries using Retro's expected structure"""
        cost_entries = []
        
        for cost_info in self.fixed_cost_types:
            # For now, set all to zero (can be enhanced later)
            entry = {
                "Name": cost_info["Name"],
                "HSN_SAC": "",
                "Amount": 0,
                "GSTRate": "",
                "TaxTotal": 0,
                "Total": 0,
                "TaxAmount": 0,
                "externalId": "",
                "AdditionalCost": cost_info["AdditionalCost"]
            }
            cost_entries.append(entry)
        
        return cost_entries
    
    async def connect_neon(self):
        """Connect to Neon database"""
        try:
            self.neon_conn = await asyncpg.connect(self.neon_connection_string)
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Neon: {e}")
            return False
    
    async def authenticate(self):
        """Authenticate with Retro API and get fresh session cookies"""
        try:
            auth_url = f"{self.auth_api_url}/InvoiceManager/DoLogin"
            
            async with aiohttp.ClientSession() as session:
                # Prepare login data
                auth_data = aiohttp.FormData()
                auth_data.add_field('UserName', self.username)
                auth_data.add_field('Password', self.password)
                
                logger.info("🔐 Authenticating with Retro API...")
                
                async with session.post(auth_url, data=auth_data) as response:
                    if response.status == 200:
                        # Extract session cookies
                        cookies = {}
                        for cookie in response.cookies:
                            cookies[cookie.key] = cookie.value
                        
                        if cookies:
                            self.session_cookies = cookies
                            logger.info(f"✅ Authentication successful! Got session cookies: {list(cookies.keys())}")
                            return True
                        else:
                            logger.error("❌ Authentication failed: No session cookies received")
                            return False
                    else:
                        logger.error(f"❌ Authentication failed: HTTP {response.status}")
                        response_text = await response.text()
                        logger.error(f"Response: {response_text[:200]}...")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Authentication error: {e}")
            return False
    
    async def send_to_retro(self, neon_data):
        """Send data to Retro using the proven FIXED DATA STRUCTURE approach"""
        try:
            # Parse Neon data
            tax_by_rate = self.parse_neon_tax_data(neon_data.get('tax_details', ''))
            cost_data = self.parse_neon_cost_data(neon_data.get('additional_costs', '[]'))
            
            # Calculate totals from GST data to match line items
            calculated_total = 0
            gst_total = 0
            for rate, tax_info in tax_by_rate.items():
                if rate > 0:  # Only count non-zero tax rates
                    base = tax_info['base_amount']
                    tax = tax_info['tax_total']
                    calculated_total += base + tax
                    gst_total += tax
            
            # Use calculated total instead of Neon's total
            total_amount = calculated_total if calculated_total > 0 else float(neon_data.get('total_amount', 0))
            sub_total = total_amount - gst_total
            
            # Create reference number
            timestamp = datetime.now().strftime('%m%d%H%M%S')
            reference_number = f"NEON-PROD-{timestamp}"
            
            logger.info(f"📤 Sending to Retro API...")
            logger.info(f"Invoice: {neon_data.get('invoice_no', 'Unknown')}")
            logger.info(f"Reference: {reference_number}")
            logger.info(f"Total Amount: {total_amount}")
            logger.info(f"SubTotal: {sub_total}")
            logger.info(f"GST Total: {gst_total}")
            logger.info(f"Additional Cost Total: 0")
            
            async with aiohttp.ClientSession() as session:
                form_data = aiohttp.FormData()
                
                # Add masterEdit first (CRITICAL!)
                form_data.add_field('masterEdit', 'false')
                
                # Add main data field
                main_data = {
                    "Status": "",
                    "ApprovalStatus": "",
                    "InternalReference": "",
                    "DryDockInvoice": False,
                    "Date": "2025-07-15T18:30:00.000Z",  # Hardcoded for now
                    "DueDate": "2025-07-16T18:30:00.000Z",
                    "ReceivedDate": "2025-07-16T18:30:00.000Z",
                    "CounterParty": "15!G!717acd53-286b-413f-936e-84b2505a6fe3",
                    "ReferenceNumber": reference_number,
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
                    "TotalAmount": total_amount,
                    "AdditionalCostTotal": None,
                    "GSTTotal": None,
                    "CostCenter": "14!G!d490d9af-a497-4ea6-b807-cbbeae42c35b",
                    "CorrespondingProformaInvoice": "",
                    "CorrespondingProformaInvoiceExternalId": "",
                    "RCMApplicable": False,
                    "CargoType": "9!G!e2c8c531-8387-4e07-afac-24f8727ffa1b",
                    "CharterType": "tc",
                    "PurchaseOrderId": neon_data.get('purchase_order_no', ''),
                    "PurchaseOrderRetroNETReference": "",
                    "isServicePurchaseOrder": True,
                    "TakeOverExpense": False
                }
                
                form_data.add_field('data', json.dumps(main_data))
                
                # Add fixed GST entries
                gst_entries = self.create_fixed_gst_entries(tax_by_rate)
                for entry in gst_entries:
                    form_data.add_field('gstData', json.dumps(entry))
                
                # Add fixed cost entries
                cost_entries = self.create_fixed_cost_entries(cost_data)
                for entry in cost_entries:
                    form_data.add_field('aCostData', json.dumps(entry))
                
                # Send request with dynamic session cookies
                headers = {
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'keep-alive',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                # Add session cookies if available
                if self.session_cookies:
                    cookie_string = '; '.join([f"{key}={value}" for key, value in self.session_cookies.items()])
                    headers['Cookie'] = cookie_string
                    logger.info(f"🍪 Using session cookies: {cookie_string}")
                
                async with session.post(self.retro_api_url, data=form_data, headers=headers) as response:
                    logger.info(f"📡 Response Status: {response.status}")
                    response_text = await response.text()
                    logger.info(f"📡 Response: {response_text}")
                    
                    if response.status == 200:
                        logger.info(f"🎉 SUCCESS! Invoice migrated with reference: {reference_number}")
                        return {"success": True, "reference": reference_number, "response": response_text}
                    else:
                        logger.error(f"❌ Failed to send to Retro: {response.status}")
                        return {"success": False, "error": f"HTTP {response.status}: {response_text}"}
                        
        except Exception as e:
            logger.error(f"Error sending to Retro: {e}")
            return {"success": False, "error": str(e)}
    
    async def run_migration(self, neon_table="invoices", limit=10, record_id=None):
        """Main migration method compatible with api_server.py"""
        try:
            # Connect to Neon
            if not await self.connect_neon():
                return {"success": False, "error": "Database connection failed"}
            
            # Authenticate with Retro
            if not await self.authenticate():
                return {"success": False, "error": "Authentication failed"}
            
            # Fetch data
            if record_id:
                # Single record
                query = """
                SELECT invoice_no, vendor_id, invoice_date, invoice_due_date, received_date, 
                       total_amount, additional_costs_total, tax_details_total, 
                       tax_details, additional_costs, purchase_order_no, office_vessel
                FROM invoices WHERE id = $1
                """
                record = await self.neon_conn.fetchrow(query, int(record_id))
                if not record:
                    await self.neon_conn.close()
                    return {"success": False, "error": f"No record found for ID: {record_id}"}
                
                records = [dict(record)]
            else:
                # Multiple records
                query = f"""
                SELECT invoice_no, vendor_id, invoice_date, invoice_due_date, received_date, 
                       total_amount, additional_costs_total, tax_details_total, 
                       tax_details, additional_costs, purchase_order_no, office_vessel
                FROM invoices 
                WHERE tax_details IS NOT NULL AND tax_details != '[]' 
                LIMIT {limit}
                """
                rows = await self.neon_conn.fetch(query)
                records = [dict(row) for row in rows]
            
            await self.neon_conn.close()
            
            # Process records
            results = {"total": len(records), "successful": 0, "failed": 0, "details": []}
            
            for record in records:
                result = await self.send_to_retro(record)
                if result["success"]:
                    results["successful"] += 1
                    results["details"].append({"reference": result["reference"], "status": "success"})
                else:
                    results["failed"] += 1
                    results["details"].append({"invoice": record.get("invoice_no"), "error": result["error"]})
            
            return results
            
        except Exception as e:
            logger.error(f"Migration error: {e}")
            return {"success": False, "error": str(e)}