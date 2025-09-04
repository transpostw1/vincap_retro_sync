#!/usr/bin/env python3
"""
API-based Neon to Retro API Data Mapper - FIXED VERSION
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
            
            timeout = aiohttp.ClientTimeout(total=60)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(auth_url, headers=headers, data='') as response:
                    if response.status == 200:
                        response_text = await response.text()
                        try:
                            auth_data = json.loads(response_text)
                            logger.info(f"Auth response: {auth_data}")
                            
                            if isinstance(auth_data, list) and len(auth_data) > 0:
                                auth_result = auth_data[0]
                                if auth_result.get('response') == True:
                                    logger.info("Successfully authenticated with Retro API")
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
            neon_fields = list(self.field_mappings.keys()) + list(self.special_fields.keys())
            fields_str = ", ".join(neon_fields)
            
            if record_id:
                try:
                    record_id_int = int(record_id)
                except (ValueError, TypeError):
                    logger.error(f"Invalid record_id: {record_id}. Must be a valid integer.")
                    return []
                
                query = f"SELECT {fields_str} FROM {table_name} WHERE id = $1"
                logger.info(f"Executing query: {query} with record_id: {record_id_int}")
                rows = await self.neon_conn.fetch(query, record_id_int)
            else:
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
                transformed_value = self._apply_field_transformations(neon_field, neon_value)
                api_data[api_field] = transformed_value
            
            transformed_data.append(api_data)
        
        logger.info(f"Transformed {len(transformed_data)} records for API")
        return transformed_data

    def _apply_field_transformations(self, neon_field: str, value: Any) -> Any:
        """Apply field-specific transformations for API"""
        if value is None:
            return ""
            
        # Date field transformations
        if neon_field in self.date_fields:
            if isinstance(value, str):
                try:
                    parsed_date = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    return parsed_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                except:
                    try:
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
        
        # JSON field transformations - KEEP AS PARSED DATA, DON'T RE-ENCODE
        if neon_field in self.json_fields:
            if isinstance(value, str):
                try:
                    return json.loads(value)  # Parse and return as Python object
                except:
                    return value
            elif isinstance(value, (dict, list)):
                return value  # Already parsed, return as-is
            else:
                return str(value)
        
        # Default: return as string, trimmed
        return str(value).strip()

    def _parse_json_field(self, value: Any) -> Any:
        """Safely parse JSON field that might be double-encoded"""
        if value is None:
            return None
            
        # If it's already a list or dict, return as-is
        if isinstance(value, (list, dict)):
            return value
            
        # If it's a string, try to parse it
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                # Check if it's still a string (double-encoded)
                if isinstance(parsed, str):
                    try:
                        return json.loads(parsed)
                    except:
                        return parsed
                return parsed
            except:
                return value
                
        return value

    def _transform_for_retro_api(self, api_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform API data to match Retro API format"""
        try:
            logger.info("Starting data transformation for Retro API...")
            logger.info(f"Input total_amount: {api_data.get('total_amount', 'NOT_FOUND')} (type: {type(api_data.get('total_amount'))})")
            
            # Base data structure
            retro_data = {
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
                'TotalAmount': int(float(api_data.get('total_amount', 0))),
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
            }
            
            logger.info(f"Calculated TotalAmount: {retro_data['TotalAmount']}")
            
            # Process GST data
            gst_data_entries = []
            existing_tax_data = {}
            
            # Parse tax details
            tax_details = self._parse_json_field(api_data.get('tax_details'))
            logger.info(f"Parsed tax_details: {tax_details} (type: {type(tax_details)})")
            
            if isinstance(tax_details, list) and len(tax_details) > 0:
                for tax in tax_details:
                    if isinstance(tax, dict):
                        tax_rate = float(tax.get('tax_rate', 0))
                        sgst = float(tax.get('sgst', 0))
                        cgst = float(tax.get('cgst', 0))
                        igst = float(tax.get('igst', 0))
                        
                        # Calculate base amount from tax values
                        if tax_rate > 0:
                            base_amount = (sgst + cgst + igst) / tax_rate * 100
                        else:
                            base_amount = 0
                        
                        existing_tax_data[tax_rate] = {
                            'base_amount': base_amount,
                            'sgst': sgst,
                            'cgst': cgst,
                            'igst': igst,
                            'hsn_sac': tax.get('hsn_sac', '')
                        }
                        logger.info(f"Found tax rate {tax_rate}% with base amount {base_amount}")
            
            # Create GST entries for all required rates
            required_gst_rates = [0.0, 3.0, 5.0, 12.0, 18.0, 28.0]
            
            for rate in required_gst_rates:
                if rate in existing_tax_data:
                    tax_data = existing_tax_data[rate]
                    base_amount = int(tax_data['base_amount'])
                    tax_amount = int((tax_data['base_amount'] * rate) / 100)
                    total_amount = base_amount + tax_amount
                    
                    gst_entry = {
                        'Rate': int(rate),
                        'Amount': base_amount,
                        'HSN_SAC': tax_data['hsn_sac'],
                        'TaxTotal': tax_amount,
                        'Total': total_amount,
                        'GSTType': 'na',
                        'IGST': int(tax_data['igst']),
                        'CGST': int(tax_data['cgst']),
                        'SGST': int(tax_data['sgst']),
                        'externalid': '',
                        'GSTRate': f"22!G!{self._generate_uuid()}"
                    }
                else:
                    # Empty entry for rates not in data
                    gst_entry = {
                        'Rate': int(rate),
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
                    }
                
                gst_data_entries.append(gst_entry)
                logger.info(f"Added GST entry for {rate}%: Amount={gst_entry['Amount']}")
            
            # Process additional costs
            a_cost_data_entries = []
            additional_costs = self._parse_json_field(api_data.get('additional_costs'))
            logger.info(f"Parsed additional_costs: {additional_costs} (type: {type(additional_costs)})")
            
            if isinstance(additional_costs, list) and len(additional_costs) > 0:
                for cost in additional_costs:
                    if isinstance(cost, dict):
                        cost_amount = int(float(cost.get('amount', 0)))
                        cost_tax_rate = float(cost.get('tax_rate', 0))
                        cost_tax_amount = int((cost_amount * cost_tax_rate) / 100)
                        cost_total = cost_amount + cost_tax_amount
                        
                        cost_entry = {
                            'Name': cost.get('type', ''),
                            'HSN_SAC': cost.get('hsn_sac', ''),
                            'Amount': cost_amount,
                            'GSTRate': f"22!G!{self._generate_uuid()}" if cost_tax_rate > 0 else '',
                            'TaxTotal': cost_tax_amount,
                            'Total': cost_total,
                            'TaxAmount': 0,  # This seems to always be 0 in the example
                            'externalId': '',
                            'AdditionalCost': f"1!G!{self._generate_uuid()}"
                        }
                        
                        a_cost_data_entries.append(cost_entry)
                        logger.info(f"Added cost entry: {cost.get('type', '')} - Amount={cost_amount}")
            
            # Add default cost entries if none present
            if not a_cost_data_entries:
                default_costs = ['Cess', 'Courier Charge', 'Transportation Charge', 'Delivery Charge']
                for cost_name in default_costs:
                    cost_entry = {
                        'Name': cost_name,
                        'HSN_SAC': '',
                        'Amount': 0,
                        'GSTRate': '',
                        'TaxTotal': 0,
                        'Total': 0,
                        'TaxAmount': 0,
                        'externalId': '',
                        'AdditionalCost': f"1!G!{self._generate_uuid()}"
                    }
                    a_cost_data_entries.append(cost_entry)
            
            # Return the complete structure
            result = {
                'data': retro_data,
                'gst_entries': gst_data_entries,
                'cost_entries': a_cost_data_entries,
                'masterEdit': False
            }
            
            logger.info(f"Final GST entries count: {len(gst_data_entries)}")
            logger.info(f"Final Cost entries count: {len(a_cost_data_entries)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error transforming data for Retro API: {e}")
            return api_data

    def _format_date_for_api(self, date_value: str) -> str:
        """Format date for Retro API (ISO format with timezone)"""
        if not date_value or date_value == '' or date_value == 'null':
            return None
        
        try:
            if 'T' in date_value or 'Z' in date_value:
                parsed_date = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            else:
                parsed_date = datetime.strptime(date_value, '%Y-%m-%d')
            
            return parsed_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        except:
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
            
            logger.info("=" * 80)
            logger.info("SENDING DATA TO RETRO API")
            logger.info("=" * 80)
            
            # Log original data
            invoice_no = api_data.get('invoice_no', 'NOT_FOUND')
            logger.info(f"Original invoice_no: {invoice_no}")
            logger.info(f"Original total_amount: {api_data.get('total_amount', 'NOT_FOUND')}")
            
            # Prepare form data
            form_data = aiohttp.FormData()
            
            # Add the main data field (JSON encoded)
            form_data.add_field('data', json.dumps(transformed_data['data']))
            form_data.add_field('masterEdit', 'false')
            
            # Add GST data entries (each as a separate form field)
            for gst_entry in transformed_data['gst_entries']:
                form_data.add_field('gstData', json.dumps(gst_entry))
                logger.info(f"Added gstData: Rate={gst_entry['Rate']}%, Amount={gst_entry['Amount']}")
            
            # Add additional cost entries (each as a separate form field)
            for cost_entry in transformed_data['cost_entries']:
                form_data.add_field('aCostData', json.dumps(cost_entry))
                logger.info(f"Added aCostData: {cost_entry['Name']} - Amount={cost_entry['Amount']}")
            
            logger.info(f"URL: {self.retro_api_url}/InvoiceManager/AddUpdateInvoice")
            logger.info("=" * 80)
            
            # Send the request
            timeout = aiohttp.ClientTimeout(total=60)
            async with aiohttp.ClientSession(cookies=self.session_cookies, timeout=timeout) as session:
                async with session.post(
                    f"{self.retro_api_url}/InvoiceManager/AddUpdateInvoice",
                    headers=headers,
                    data=form_data
                ) as response:
                    response_text = await response.text()
                    logger.info(f"API Response for {invoice_no}: Status={response.status}")
                    logger.info(f"Response body: {response_text}")
                    
                    # Parse response to check for errors
                    success = await self._handle_api_response(response_text, response.status, invoice_no)
                    
                    if success:
                        # Verify the invoice was created correctly
                        await self._verify_invoice_creation(invoice_no, api_data.get('total_amount', 0))
                    
                    return success
                        
        except Exception as e:
            logger.error(f"Error sending data to API: {e}")
            return False

    async def _handle_api_response(self, response_text: str, status_code: int, invoice_no: str) -> bool:
        """Handle and parse API response to determine success/failure"""
        try:
            # Try to parse the response as JSON
            if response_text.strip():
                try:
                    response_data = json.loads(response_text)
                    
                    # Handle different response formats
                    if isinstance(response_data, list) and len(response_data) > 0:
                        response_item = response_data[0]
                        
                        if isinstance(response_item, dict):
                            response_status = response_item.get('response', True)
                            message = response_item.get('message', '')
                            
                            if response_status is False:
                                logger.error(f"API Error for {invoice_no}: {message}")
                                
                                # Check for specific error types
                                if "matching supplier reference number already exists" in message:
                                    logger.warning(f"Invoice {invoice_no} already exists in system")
                                elif "Invalid Operation" in message:
                                    logger.error(f"Invalid operation for invoice {invoice_no}: {message}")
                                
                                return False
                            else:
                                logger.info(f"Successfully created invoice {invoice_no}")
                                return True
                    
                    # If we can't parse the structure, check status code
                    if status_code == 200 or status_code == 201:
                        logger.info(f"API request successful for {invoice_no} (status {status_code})")
                        return True
                    else:
                        logger.error(f"API request failed for {invoice_no} with status {status_code}")
                        return False
                        
                except json.JSONDecodeError:
                    # Not JSON, check status code
                    if status_code == 200 or status_code == 201:
                        logger.info(f"API request successful for {invoice_no} (status {status_code})")
                        return True
                    else:
                        logger.error(f"API request failed for {invoice_no} with status {status_code}")
                        return False
            else:
                # Empty response, check status code
                if status_code == 200 or status_code == 201:
                    logger.info(f"API request successful for {invoice_no} (status {status_code})")
                    return True
                else:
                    logger.error(f"API request failed for {invoice_no} with status {status_code}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error parsing API response for {invoice_no}: {e}")
            return False

    async def _verify_invoice_creation(self, invoice_no: str, expected_amount: float):
        """Verify that the invoice was created correctly by fetching it back"""
        try:
            logger.info(f"Verifying invoice creation for {invoice_no}...")
            
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate',
                'Cookie': '; '.join([f'{name}={value}' for name, value in self.session_cookies.items()]),
            }
            
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{self.retro_api_url}/InvoiceManager/GetAllInvoicePendingAssignment",
                    headers=headers,
                    data=""
                ) as response:
                    if response.status == 200:
                        response_text = await response.text()
                        
                        try:
                            # Parse the response (might be double-encoded JSON)
                            response_data = json.loads(response_text)
                            
                            # Handle double-encoded JSON
                            if isinstance(response_data, str):
                                response_data = json.loads(response_data)
                            
                            # Look for our invoice in the data
                            if isinstance(response_data, dict) and 'Data' in response_data:
                                invoices = response_data['Data']
                                
                                for invoice in invoices:
                                    if invoice.get('ReferenceNumber') == invoice_no:
                                        actual_amount = invoice.get('TotalAmount', 0)
                                        logger.info(f"Verification successful for {invoice_no}:")
                                        logger.info(f"  Expected amount: {expected_amount}")
                                        logger.info(f"  Actual amount: {actual_amount}")
                                        logger.info(f"  Status: {invoice.get('Status', 'Unknown')}")
                                        logger.info(f"  Organization: {invoice.get('Organization', 'Unknown')}")
                                        logger.info(f"  Currency: {invoice.get('Currency', 'Unknown')}")
                                        
                                        # Check if amounts match
                                        if abs(float(actual_amount) - float(expected_amount)) < 0.01:
                                            logger.info(f"Amount verification PASSED for {invoice_no}")
                                        else:
                                            logger.warning(f"Amount verification FAILED for {invoice_no} - Expected: {expected_amount}, Got: {actual_amount}")
                                        
                                        return True
                                
                                logger.warning(f"Invoice {invoice_no} not found in pending assignments")
                                return False
                            else:
                                logger.warning(f"Unexpected response format for verification: {type(response_data)}")
                                return False
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse verification response: {e}")
                            return False
                    else:
                        logger.error(f"Verification request failed with status {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error verifying invoice creation: {e}")
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
    record_id = None
    if len(sys.argv) > 1:
        record_id = sys.argv[1]
        try:
            int(record_id)
            print(f"Processing specific record with ID: {record_id}")
        except ValueError:
            print(f"Error: Invalid record_id '{record_id}'. Must be a valid integer.")
            return
    else:
        print("Processing invoices with default limit (10 records)")
    
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
        limit=10 if not record_id else None,
        record_id=record_id
    ))

if __name__ == "__main__":
    main()