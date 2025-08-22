#!/usr/bin/env python3
"""
Refactored API-based Neon to Retro API Data Mapper
Better follows SOLID principles with separated concerns.
"""

import os
import logging
import asyncio
import aiohttp # type: ignore
import json
import sys
from typing import Dict, List, Any, Optional, Protocol
from datetime import datetime
import asyncpg
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# INTERFACES (Dependency Inversion Principle)
# ============================================================================

class DatabaseConnector(Protocol):
    """Interface for database operations"""
    async def connect(self) -> bool:
        ...
    
    async def fetch_data(self, table_name: str, limit: Optional[int] = None, record_id: Optional[str] = None) -> List[Dict[str, Any]]:
        ...
    
    async def close(self):
        ...

class Authenticator(Protocol):
    """Interface for authentication operations"""
    async def authenticate(self) -> bool:
        ...
    
    def get_session_cookies(self):
        ...

class DataTransformer(Protocol):
    """Interface for data transformation operations"""
    def transform_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ...
    
    def transform_for_api(self, data: Dict[str, Any]) -> Dict[str, Any]:
        ...

class APIClient(Protocol):
    """Interface for API communication"""
    async def send_data(self, data: Dict[str, Any]) -> bool:
        ...

# ============================================================================
# CONCRETE IMPLEMENTATIONS (Single Responsibility Principle)
# ============================================================================

class NeonDatabaseConnector:
    """Handles Neon database operations only"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None
        
        # Field mappings: Neon field -> API parameter name
        self.field_mappings = {
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
        
        self.special_fields = {
            "id": None,
            "created_at": None,
            "updated_at": None,
        }

    async def connect(self) -> bool:
        """Establish connection to Neon database"""
        try:
            self.connection = await asyncpg.connect(self.connection_string)
            logger.info("Successfully connected to Neon database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Neon database: {e}")
            return False

    async def fetch_data(self, table_name: str = "invoices", limit: Optional[int] = None, record_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch data from Neon database"""
        try:
            neon_fields = list(self.field_mappings.keys()) + list(self.special_fields.keys())
            fields_str = ", ".join(neon_fields)
            
            if record_id:
                query = f"SELECT {fields_str} FROM {table_name} WHERE id = $1"
                logger.info(f"Executing query: {query} with record_id: {record_id}")
                rows = await self.connection.fetch(query, record_id)
            else:
                query = f"SELECT {fields_str} FROM {table_name}"
                if limit:
                    query += f" LIMIT {limit}"
                logger.info(f"Executing query: {query}")
                rows = await self.connection.fetch(query)
            
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

    async def close(self):
        """Close database connection"""
        if self.connection:
            await self.connection.close()

class RetroAuthenticator:
    """Handles Retro API authentication only"""
    
    def __init__(self, auth_api_url: str, username: str, password: str):
        self.auth_api_url = auth_api_url
        self.username = username
        self.password = password
        self.session_cookies = None

    async def authenticate(self) -> bool:
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

    def get_session_cookies(self):
        """Get session cookies for API requests"""
        return self.session_cookies

class RetroDataTransformer:
    """Handles data transformation only"""
    
    def __init__(self):
        self.date_fields = ["invoice_date", "invoice_due_date", "received_date"]
        self.numeric_fields = ["total_amount", "additional_costs_total", "igst_total", "tax_details_total"]
        self.json_fields = ["additional_costs", "tax_details"]

    def transform_data(self, neon_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform Neon data to API format"""
        transformed_data = []
        
        for record in neon_data:
            api_data = {}
            
            # Map fields based on the mapping dictionary
            field_mappings = {
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
            
            for neon_field, api_field in field_mappings.items():
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
                    if 'T' in value or 'Z' in value:
                        parsed_date = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    else:
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
                    json.loads(value)
                    return value
                except:
                    return value
            elif isinstance(value, (dict, list)):
                return json.dumps(value)
            else:
                return str(value)
        
        return str(value).strip()

    def transform_for_api(self, api_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform API data to match Retro API format"""
        try:
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
            
            retro_data['data'] = json.dumps(retro_data['data'])
            
            # Add GST and cost data entries
            retro_data['_gst_data_entries'] = self._create_gst_entries(api_data)
            retro_data['_a_cost_data_entries'] = self._create_cost_entries(api_data)
            
            return retro_data
            
        except Exception as e:
            logger.error(f"Error transforming data for Retro API: {e}")
            return api_data

    def _format_date_for_api(self, date_value: str) -> str:
        """Format date for Retro API (ISO format with timezone)"""
        if not date_value or date_value == '':
            return ''
        
        try:
            if 'T' in date_value or 'Z' in date_value:
                parsed_date = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            else:
                parsed_date = datetime.strptime(date_value, '%Y-%m-%d')
            
            return parsed_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        except:
            return ''

    def _create_gst_entries(self, api_data: Dict[str, Any]) -> List[str]:
        """Create GST data entries"""
        gst_data_entries = []
        
        if api_data.get('tax_details'):
            try:
                tax_details = json.loads(api_data['tax_details'])
                for tax in tax_details:
                    gst_data_entries.append(json.dumps({
                        'Rate': float(tax.get('tax_rate', 0)),
                        'Amount': float(tax.get('amount', 0)),
                        'HSN_SAC': tax.get('hsn_sac', ''),
                        'TaxTotal': float(tax.get('tax_rate', 0)),
                        'Total': float(tax.get('amount', 0)),
                        'GSTType': 'na',
                        'IGST': float(tax.get('igst', 0)),
                        'CGST': float(tax.get('cgst', 0)),
                        'SGST': float(tax.get('sgst', 0)),
                        'externalid': '',
                        'GSTRate': f"22!G!{self._generate_uuid()}"
                    }))
            except:
                pass
        
        if not gst_data_entries:
            default_gst_rates = [0, 3, 5, 12, 18, 28]
            for rate in default_gst_rates:
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
        
        return gst_data_entries

    def _create_cost_entries(self, api_data: Dict[str, Any]) -> List[str]:
        """Create additional cost data entries"""
        a_cost_data_entries = []
        
        if api_data.get('additional_costs'):
            try:
                additional_costs = json.loads(api_data['additional_costs'])
                for cost in additional_costs:
                    a_cost_data_entries.append(json.dumps({
                        'Name': cost.get('type', ''),
                        'HSN_SAC': cost.get('hsn_sac', ''),
                        'Amount': float(cost.get('amount', 0)),
                        'GSTRate': '',
                        'TaxTotal': 0,
                        'Total': float(cost.get('amount', 0)),
                        'TaxAmount': 0,
                        'externalId': '',
                        'AdditionalCost': f"1!G!{self._generate_uuid()}"
                    }))
            except:
                pass
        
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
        
        return a_cost_data_entries

    def _generate_uuid(self) -> str:
        """Generate a simple UUID-like string"""
        import uuid
        return str(uuid.uuid4())

class RetroAPIClient:
    """Handles Retro API communication only"""
    
    def __init__(self, retro_api_url: str, session_cookies):
        self.retro_api_url = retro_api_url
        self.session_cookies = session_cookies

    async def send_data(self, api_data: Dict[str, Any]) -> bool:
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
            transformer = RetroDataTransformer()
            transformed_data = transformer.transform_for_api(api_data)
            
            # Debug: Log the actual data being sent
            logger.info(f"Debug - Data being sent to API:")
            logger.info(f"  Raw data JSON: {transformed_data.get('data', 'NOT_FOUND')}")
            
            # Prepare form data
            form_data = aiohttp.FormData()
            
            # Add regular fields
            for key, value in transformed_data.items():
                if key.startswith('_'):
                    continue
                if value is not None and value != "":
                    form_data.add_field(key, str(value))
            
            # Add multiple gstData entries
            if '_gst_data_entries' in transformed_data:
                for gst_entry in transformed_data['_gst_data_entries']:
                    form_data.add_field('gstData', gst_entry)
            
            # Add multiple aCostData entries
            if '_a_cost_data_entries' in transformed_data:
                for cost_entry in transformed_data['_a_cost_data_entries']:
                    form_data.add_field('aCostData', cost_entry)
            
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

# ============================================================================
# MAIN ORCHESTRATOR (Open/Closed Principle)
# ============================================================================

class NeonToRetroMapper:
    """Main orchestrator that coordinates all operations"""
    
    def __init__(self, 
                 db_connector: DatabaseConnector,
                 authenticator: Authenticator,
                 data_transformer: DataTransformer,
                 api_client: APIClient):
        self.db_connector = db_connector
        self.authenticator = authenticator
        self.data_transformer = data_transformer
        self.api_client = api_client

    async def run_migration(self, neon_table: str = "invoices", limit: Optional[int] = None, record_id: Optional[str] = None):
        """Run the complete migration process"""
        logger.info("Starting Neon to Retro API migration...")
        
        # Authenticate first
        auth_success = await self.authenticator.authenticate()
        if not auth_success:
            logger.error("Failed to authenticate with Retro API")
            return False
        
        # Connect to database
        db_connected = await self.db_connector.connect()
        if not db_connected:
            logger.error("Failed to connect to database")
            return False
        
        try:
            # Fetch data
            neon_data = await self.db_connector.fetch_data(neon_table, limit, record_id)
            if not neon_data:
                logger.warning("No data found in database")
                return False
            
            # Transform data
            transformed_data = self.data_transformer.transform_data(neon_data)
            
            # Send to API
            results = await self._process_records(transformed_data)
            
            logger.info(f"Migration completed. Results: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False
        
        finally:
            await self.db_connector.close()

    async def _process_records(self, transformed_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process all records and send to API"""
        total_records = len(transformed_data)
        successful_sends = 0
        failed_sends = 0
        
        logger.info(f"Starting to process {total_records} records...")
        
        for i, record in enumerate(transformed_data, 1):
            logger.info(f"Processing record {i}/{total_records}: {record.get('invoice_no', 'Unknown')}")
            
            success = await self.api_client.send_data(record)
            if success:
                successful_sends += 1
            else:
                failed_sends += 1
            
            await asyncio.sleep(0.5)
        
        return {
            "total": total_records,
            "successful": successful_sends,
            "failed": failed_sends
        }

# ============================================================================
# FACTORY (Dependency Injection)
# ============================================================================

class MapperFactory:
    """Factory for creating mapper instances with proper dependencies"""
    
    @staticmethod
    def create_mapper(neon_connection_string: str, auth_api_url: str, retro_api_url: str, username: str, password: str):
        """Create a mapper instance with all dependencies"""
        db_connector = NeonDatabaseConnector(neon_connection_string)
        authenticator = RetroAuthenticator(auth_api_url, username, password)
        data_transformer = RetroDataTransformer()
        
        # Note: api_client will be created after authentication
        def create_api_client():
            return RetroAPIClient(retro_api_url, authenticator.get_session_cookies())
        
        return NeonToRetroMapper(db_connector, authenticator, data_transformer, create_api_client)

# ============================================================================
# MAIN FUNCTION
# ============================================================================

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
    
    # Create mapper using factory
    mapper = MapperFactory.create_mapper(neon_connection_string, auth_api_url, retro_api_url, username, password)
    
    # Run migration
    asyncio.run(mapper.run_migration(
        neon_table="invoices",
        limit=10 if not record_id else None,
        record_id=record_id
    ))

if __name__ == "__main__":
    main() 