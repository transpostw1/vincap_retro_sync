#!/usr/bin/env python3
"""
API-based Neon to Retro API Data Mapper
Maps data from Neon database to Retro API via HTTP requests.
"""

import os
import logging
import asyncio
import aiohttp # type: ignore
import json
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncpg

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class APINeonToRetroMapper:
    """API-based data mapper that sends data to Retro API endpoint"""
    
    def __init__(self, neon_connection_string: str, auth_api_url: str, retro_api_url: str, username: str, password: str):
        self.neon_connection_string = neon_connection_string
        self.auth_api_url = auth_api_url
        self.retro_api_url = retro_api_url
        self.username = username
        self.password = password
        self.neon_conn = None
        self.auth_token = None
        self.session_cookies = None
        
        # Field mappings: Neon field -> API parameter name
        self.field_mappings = {
            # Core invoice fields that match API parameters
            "vendor_id": "vendor_id",
            "org_id": "org_id", 
            "invoice_type": "invoice_type",
            "corresponding_proforma_invoice": "corresponding_proforma_invoice",
            "invoice_no": "invoice_no",
            "invoice_date": "invoice_date",
            "invoice_due_date": "invoice_due_date",
            "purchase_order_no": "purchase_order_no",
            "received_date": "received_date",
            "office_vessel": "office_vessel",
            "currency": "currency",
            "total_amount": "total_amount",
            "additional_costs": "additional_costs",
            "additional_costs_total": "additional_costs_total",
            "tax_details": "tax_details",
            "tax_details_total": "tax_details_total",
            "igst_total": "igst_total",
            "department": "department",
            "assignee": "assignee",
            "invoice_file": "invoice_file",
            "supporting_documents": "supporting_documents",
        }
        
        # Fields that need special handling or transformation
        self.special_fields = {
            "id": None,  # Not used in API
            "created_at": None,  # Not used in API
            "updated_at": None,  # Not used in API
            # "invoices_user_id_fkey": None,  # Not used in API
        }
        
        # Field transformation rules
        self.date_fields = ["invoice_date", "invoice_due_date", "received_date"]
        self.numeric_fields = ["total_amount", "additional_costs_total", "igst_total", "tax_details_total"]
        self.json_fields = ["additional_costs", "tax_details"]

    async def authenticate(self):
        """Authenticate with the Retro API"""
        try:
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'Origin': self.auth_api_url,
                'Referer': f'{self.auth_api_url}/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
            }
            
            auth_url = f"{self.auth_api_url}/Authentication/AuthenticateUser?userName={self.username}&password={self.password}"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(auth_url, headers=headers, data='') as response:
                    if response.status == 200:
                        response_text = await response.text()
                        try:
                            # Parse JSON manually since content-type is text/html
                            auth_data = json.loads(response_text)
                            logger.info(f"Auth response: {auth_data}")
                            
                            # Check if authentication was successful
                            if isinstance(auth_data, list) and len(auth_data) > 0:
                                auth_result = auth_data[0]
                                if auth_result.get('response') == True:
                                    logger.info("Successfully authenticated with Retro API")
                                    # Store session cookies for subsequent requests
                                    self.session_cookies = response.cookies
                                    return True
                            
                            logger.error("Authentication failed - invalid response format")
                            return False
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse JSON response: {e}")
                            return False
                    else:
                        response_text = await response.text()
                        logger.error(f"Authentication failed with status {response.status}: {response_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False

    async def connect_neon(self):
        """Establish connection to Neon database"""
        try:
            self.neon_conn = await asyncpg.connect(self.neon_connection_string)
            logger.info("Successfully connected to Neon database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Neon database: {e}")
            return False

    async def fetch_neon_data(self, table_name: str = "invoices", limit: Optional[int] = None, record_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch data from Neon database"""
        try:
            # Get all Neon fields that have mappings or need special handling
            neon_fields = list(self.field_mappings.keys()) + list(self.special_fields.keys())
            fields_str = ", ".join(neon_fields)
            
            if record_id:
                # Fetch specific record by id (primary key)
                query = f"SELECT {fields_str} FROM {table_name} WHERE id = $1"
                logger.info(f"Executing query: {query} with record_id: {record_id}")
                rows = await self.neon_conn.fetch(query, record_id)
            else:
                # Fetch all records with optional limit
                query = f"SELECT {fields_str} FROM {table_name}"
                if limit:
                    query += f" LIMIT {limit}"
                logger.info(f"Executing query: {query}")
                rows = await self.neon_conn.fetch(query)
            
            # Convert to list of dictionaries
            data = []
            for row in rows:
                row_dict = {}
                for i, field in enumerate(neon_fields):
                    row_dict[field] = row[i]
                data.append(row_dict)
            
            logger.info(f"Fetched {len(data)} records from Neon database")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching data from Neon: {e}")
            return []

    def transform_data_for_api(self, neon_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform Neon data to API format"""
        transformed_data = []
        
        for record in neon_data:
            api_data = {}
            
            # Map fields based on the mapping dictionary
            for neon_field, api_field in self.field_mappings.items():
                neon_value = record.get(neon_field)
                
                # Apply field-specific transformations
                transformed_value = self._apply_field_transformations(neon_field, neon_value)
                api_data[api_field] = transformed_value
            
            transformed_data.append(api_data)
        
        logger.info(f"Transformed {len(transformed_data)} records for API")
        return transformed_data

    def _apply_field_transformations(self, neon_field: str, value: Any) -> Any:
        """Apply field-specific transformations for API"""
        if value is None:
            return ""
            
        # Date field transformations - format as string for API
        if neon_field in self.date_fields:
            if isinstance(value, str):
                try:
                    # Parse and format date to ISO format with timezone
                    parsed_date = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    return parsed_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                except:
                    try:
                        # Try parsing as date only
                        parsed_date = datetime.strptime(value, '%Y-%m-%d')
                        return parsed_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    except:
                        return str(value).strip()
            elif isinstance(value, datetime):
                return value.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            else:
                return str(value).strip()
        
        # Numeric field transformations
        if neon_field in self.numeric_fields:
            try:
                return str(float(value)) if value is not None else "0.00"
            except:
                return "0.00"
        
        # JSON field transformations
        if neon_field in self.json_fields:
            if isinstance(value, str):
                try:
                    # Validate JSON format
                    json.loads(value)
                    return value
                except:
                    # If not valid JSON, return as string
                    return value
            elif isinstance(value, (dict, list)):
                return json.dumps(value)
            else:
                return str(value)
        
        # Default: return as string, trimmed
        return str(value).strip()

    def _transform_for_retro_api(self, api_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform API data to match Retro API format"""
        try:
            # Base data structure based on the Postman example
            retro_data = {
                'data': {
                    'Status': '',
                    'ApprovalStatus': '',
                    'InternalReference': '',
                    'DryDockInvoice': False,
                    'Date': self._format_date_for_api(api_data.get('invoice_date', '')),
                    'DueDate': self._format_date_for_api(api_data.get('invoice_due_date', '')),
                    'ReceivedDate': self._format_date_for_api(api_data.get('received_date', '')),
                    'CounterParty': api_data.get('vendor_id', ''),
                    'ReferenceNumber': api_data.get('invoice_no', ''),
                    'Type': api_data.get('invoice_type', ''),
                    'SubTotal': None,
                    'Remark': f"For testing on {datetime.now().strftime('%d/%m/%Y')}",
                    'Currency': api_data.get('currency', ''),
                    'Location': api_data.get('office_vessel', ''),
                    'Paid': False,
                    'Due': False,
                    'Overdue': False,
                    'PendingAssignment': True,
                    'externalId': '',
                    'Organization': api_data.get('org_id', ''),
                    'Department': api_data.get('department', ''),
                    'TotalAmount': float(api_data.get('total_amount', 0)),
                    'AdditionalCostTotal': None,
                    'GSTTotal': None,
                    'CostCenter': '14!G!d490d9af-a497-4ea6-b807-cbbeae42c35b',
                    'CorrespondingProformaInvoice': api_data.get('corresponding_proforma_invoice', ''),
                    'CorrespondingProformaInvoiceExternalId': '',
                    'RCMApplicable': False,
                    'CargoType': '9!G!e2c8c531-8387-4e07-afac-24f8727ffa1b',
                    'CharterType': 'tc',
                    'PurchaseOrderId': api_data.get('purchase_order_no', ''),
                    'PurchaseOrderRetroNETReference': '',
                    'isServicePurchaseOrder': True,
                    'TakeOverExpense': False
                },
                'masterEdit': 'false'
            }
            
            # Convert data to JSON string
            retro_data['data'] = json.dumps(retro_data['data'])
            
            # Store GST data entries separately for multiple form fields
            gst_data_entries = []
            
            # Create a map of existing tax data from Neon
            existing_tax_data = {}
            if api_data.get('tax_details'):
                logger.info(f"Processing tax_details: {api_data['tax_details']}")
                try:
                    tax_details = json.loads(api_data['tax_details'])
                    logger.info(f"Parsed tax_details: {tax_details}")
                    for tax in tax_details:
                        tax_rate = float(tax.get('tax_rate', 0))
                        existing_tax_data[tax_rate] = tax
                except Exception as e:
                    logger.error(f"Error processing tax_details: {e}")
            
            # ALWAYS add ALL required GST rates (0%, 3%, 5%, 12%, 18%, 28%)
            # This ensures the Retro API receives all expected rates
            required_gst_rates = [0, 3, 5, 12, 18, 28]
            for rate in required_gst_rates:
                if rate in existing_tax_data:
                    # Use data from Neon
                    tax = existing_tax_data[rate]
                    base_amount = float(tax.get('amount', 0))
                    tax_amount = (base_amount * rate) / 100
                    total_amount = base_amount + tax_amount
                    
                    logger.info(f"GST Entry for {rate}%: Amount={base_amount}, TaxAmount={tax_amount}, Total={total_amount}")
                    
                    gst_data_entries.append(json.dumps({
                        'Rate': rate,
                        'Amount': base_amount,
                        'HSN_SAC': tax.get('hsn_sac', ''),
                        'TaxTotal': tax_amount,
                        'Total': total_amount,
                        'GSTType': 'na',
                        'IGST': float(tax.get('igst', 0)),
                        'CGST': float(tax.get('cgst', 0)),
                        'SGST': float(tax.get('sgst', 0)),
                        'externalid': '',
                        'GSTRate': f"22!G!{self._generate_uuid()}"
                    }))
                else:
                    # Send empty entry for this rate
                    logger.info(f"GST Entry for {rate}%: No data in Neon, sending empty entry")
                    gst_data_entries.append(json.dumps({
                        'Rate': rate,
                        'Amount': 0,
                        'HSN_SAC': '',
                        'TaxTotal': 0,
                        'Total': 0,
                        'GSTType': 'na',
                        'IGST': 0,
                        'CGST': 0,
                        'SGST': 0,
                        'externalid': '',
                        'GSTRate': f"22!G!{self._generate_uuid()}"
                    }))
            
            # Store additional cost data entries separately
            a_cost_data_entries = []
            
            # Add additional costs if present
            if api_data.get('additional_costs'):
                logger.info(f"Processing additional_costs: {api_data['additional_costs']}")
                try:
                    additional_costs = json.loads(api_data['additional_costs'])
                    logger.info(f"Parsed additional_costs: {additional_costs}")
                    for i, cost in enumerate(additional_costs):
                        # Extract values from Neon data
                        cost_amount = float(cost.get('amount', 0))
                        cost_tax_rate = float(cost.get('tax_rate', 0))
                        
                        # Calculate tax amount and total
                        cost_tax_amount = (cost_amount * cost_tax_rate) / 100
                        cost_total = cost_amount + cost_tax_amount
                        
                        logger.info(f"Cost Entry {i}: Name={cost.get('type', '')}, Amount={cost_amount}, TaxRate={cost_tax_rate}%, TaxAmount={cost_tax_amount}, Total={cost_total}")
                        
                        a_cost_data_entries.append(json.dumps({
                            'Name': cost.get('type', ''),
                            'HSN_SAC': cost.get('hsn_sac', ''),
                            'Amount': cost_amount,
                            'GSTRate': f"22!G!{self._generate_uuid()}" if cost_tax_rate > 0 else '',
                            'TaxTotal': cost_tax_amount,
                            'Total': cost_total,
                            'TaxAmount': cost_tax_amount,
                            'externalId': '',
                            'AdditionalCost': f"1!G!{self._generate_uuid()}"
                        }))
                except Exception as e:
                    logger.error(f"Error processing additional_costs: {e}")
                    pass
            
            # Add default additional cost entries if none present
            if not a_cost_data_entries:
                default_costs = ['Cess', 'Courier Charge', 'Transportation Charge', 'Delivery Charge']
                for cost_name in default_costs:
                    a_cost_data_entries.append(json.dumps({
                        'Name': cost_name,
                        'HSN_SAC': '',
                        'Amount': 0,
                        'GSTRate': '',
                        'TaxTotal': 0,
                        'Total': 0,
                        'TaxAmount': 0,
                        'externalId': '',
                        'AdditionalCost': f"1!G!{self._generate_uuid()}"
                    }))
            
            # Store the entries for later use in send_to_api
            retro_data['_gst_data_entries'] = gst_data_entries
            retro_data['_a_cost_data_entries'] = a_cost_data_entries
            
            return retro_data
            
        except Exception as e:
            logger.error(f"Error transforming data for Retro API: {e}")
            return api_data

    def _format_date_for_api(self, date_value: str) -> str:
        """Format date for Retro API (ISO format with timezone)"""
        if not date_value or date_value == '' or date_value == 'null':
            return None
        
        try:
            # Try parsing as ISO format first
            if 'T' in date_value or 'Z' in date_value:
                parsed_date = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            else:
                # Try parsing as date only
                parsed_date = datetime.strptime(date_value, '%Y-%m-%d')
            
            # Format as ISO with timezone
            return parsed_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        except:
            # If parsing fails, return None instead of empty string
            return None

    def _generate_uuid(self) -> str:
        """Generate a simple UUID-like string"""
        import uuid
        return str(uuid.uuid4())

    async def send_to_api(self, api_data: Dict[str, Any]) -> bool:
        """Send single record to API endpoint"""
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
            
            # Transform data to match the expected format
            transformed_data = self._transform_for_retro_api(api_data)
            
            # Debug: Log the actual data being sent
            logger.info(f"Debug - Data being sent to API:")
            logger.info(f"  Raw data JSON: {transformed_data.get('data', 'NOT_FOUND')}")
            
            # Prepare form data
            form_data = aiohttp.FormData()
            
            # Add regular fields
            for key, value in transformed_data.items():
                if key.startswith('_'):
                    continue  # Skip internal fields
                if value is not None and value != "":
                    form_data.add_field(key, str(value))
            
            # Add multiple gstData entries
            if '_gst_data_entries' in transformed_data:
                logger.info(f"Adding {len(transformed_data['_gst_data_entries'])} gstData entries")
                for gst_entry in transformed_data['_gst_data_entries']:
                    form_data.add_field('gstData', gst_entry)
            else:
                logger.warning("No gstData entries found in transformed data")
            
            # Add multiple aCostData entries
            if '_a_cost_data_entries' in transformed_data:
                logger.info(f"Adding {len(transformed_data['_a_cost_data_entries'])} aCostData entries")
                for cost_entry in transformed_data['_a_cost_data_entries']:
                    form_data.add_field('aCostData', cost_entry)
            else:
                logger.warning("No aCostData entries found in transformed data")
            
            async with aiohttp.ClientSession(cookies=self.session_cookies) as session:
                async with session.post(
                    f"{self.retro_api_url}/InvoiceManager/AddUpdateInvoice",
                    headers=headers,
                    data=form_data
                ) as response:
                    response_text = await response.text()
                    logger.info(f"API Response for {api_data.get('invoice_no', 'Unknown')}: Status={response.status}, Response={response_text}")
                    
                    if response.status == 200 or response.status == 201:
                        logger.info(f"Successfully sent data to API: {api_data.get('invoice_no', 'Unknown')}")
                        return True
                    else:
                        logger.error(f"API request failed with status {response.status}: {response_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error sending data to API: {e}")
            return False

    async def process_records(self, transformed_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process all records and send to API"""
        total_records = len(transformed_data)
        successful_sends = 0
        failed_sends = 0
        
        logger.info(f"Starting to process {total_records} records...")
        
        for i, record in enumerate(transformed_data, 1):
            logger.info(f"Processing record {i}/{total_records}: {record.get('invoice_no', 'Unknown')}")
            
            success = await self.send_to_api(record)
            if success:
                successful_sends += 1
            else:
                failed_sends += 1
            
            # Small delay to avoid overwhelming the API
            await asyncio.sleep(0.5)
        
        return {
            "total": total_records,
            "successful": successful_sends,
            "failed": failed_sends
        }

    async def run_migration(self, neon_table: str = "invoices", limit: Optional[int] = None, record_id: Optional[str] = None):
        """Run the complete migration process"""
        logger.info("Starting Neon to Retro API migration...")
        
        # Authenticate first
        auth_success = await self.authenticate()
        if not auth_success:
            logger.error("Failed to authenticate with Retro API")
            return False
        
        # Connect to Neon database
        neon_connected = await self.connect_neon()
        if not neon_connected:
            logger.error("Failed to connect to Neon database")
            return False
        
        try:
            # Fetch data from Neon
            neon_data = await self.fetch_neon_data(neon_table, limit, record_id)
            if not neon_data:
                logger.warning("No data found in Neon database")
                return False
            
            # Transform data for API
            transformed_data = self.transform_data_for_api(neon_data)
            
            # Send to API
            results = await self.process_records(transformed_data)
            
            logger.info(f"Migration completed. Results: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False
        
        finally:
            # Close connection
            if self.neon_conn:
                await self.neon_conn.close()

    def print_mappings(self):
        """Print the current field mappings for reference"""
        print("\n=== Field Mappings (Neon -> API) ===")
        print(f"{'Neon Field':<30} {'API Parameter':<30}")
        print("-" * 60)
        
        for neon_field, api_field in self.field_mappings.items():
            print(f"{neon_field:<30} {api_field:<30}")
        
        print(f"\nFields not used in API:")
        for field in self.special_fields.keys():
            print(f"  - {field}")
        
        print(f"\nSpecial field types:")
        print(f"  Date fields: {', '.join(self.date_fields)}")
        print(f"  Numeric fields: {', '.join(self.numeric_fields)}")
        print(f"  JSON fields: {', '.join(self.json_fields)}")

def main():
    """Main function to run the migration"""
    # Check for command line arguments
    record_id = None
    if len(sys.argv) > 1:
        record_id = sys.argv[1]
        print(f"Processing specific record with ID: {record_id}")
    
    # Configuration
    neon_connection_string = os.getenv(
        "NEON_CONNECTION_STRING", 
        "postgresql://neondb_owner:npg_ziNBtp5sX4Fv@ep-quiet-forest-a53t111o-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    )
    
    auth_api_url = os.getenv("AUTH_API_URL", "http://192.168.1.25:801")
    retro_api_url = os.getenv("RETRO_API_URL", "http://192.168.1.25:801")
    username = os.getenv("API_USERNAME", "akshay")
    password = os.getenv("API_PASSWORD", "retroinv@123")
    
    # Create mapper instance
    mapper = APINeonToRetroMapper(neon_connection_string, auth_api_url, retro_api_url, username, password)
    
    # Print mappings for reference
    mapper.print_mappings()
    
    # Run migration
    asyncio.run(mapper.run_migration(
        neon_table="invoices",
        limit=10 if not record_id else None,  # Only use limit if not processing specific record
        record_id=record_id
    ))

if __name__ == "__main__":
    main() 