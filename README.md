# Neon to Retro API Data Mapper

This project provides a Python script to map and migrate data from a Neon database to a Retro API endpoint via HTTP requests.

## Features

- **API Integration**: Sends data to Retro API endpoint using HTTP POST requests
- **Authentication**: Handles session-based authentication with Retro API
- **Field Mapping**: Maps fields between Neon DB and Retro API based on provided mapping table
- **Data Transformation**: Automatic data type conversion and formatting
- **Error Handling**: Comprehensive error handling with retry logic
- **Logging**: Detailed logging for monitoring and debugging
- **Configuration Management**: Environment-based configuration

## Files

- `api_mapper.py` - Main API-based data mapper script
- `requirements.txt` - Python dependencies
- `README.md` - This file

## Installation

1. **Install Python dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

## Configuration

### Environment Variables

Set the following environment variables for database connections and API endpoints:

```bash
# Neon Database
export NEON_CONNECTION_STRING="postgresql://username:password@host:port/database"

# Retro API Configuration
export AUTH_API_URL="http://192.168.1.25:801"
export RETRO_API_URL="http://192.168.1.25:801"
export API_USERNAME="akshay"
export API_PASSWORD="retroinv@123"

# Migration Settings
export NEON_TABLE="invoices"
export BATCH_SIZE="1000"
export MAX_RECORDS="0"  # 0 for no limit
export LOG_LEVEL="INFO"
```

### Field Mappings

The field mappings are defined in the script based on your provided mapping table:

| Neon Field                     | Retro API Parameter            | Status    |
| ------------------------------ | ------------------------------ | --------- |
| vendor_id                      | vendor_id                      | ✅ Mapped |
| org_id                         | org_id                         | ✅ Mapped |
| invoice_type                   | invoice_type                   | ✅ Mapped |
| corresponding_proforma_invoice | corresponding_proforma_invoice | ✅ Mapped |
| invoice_no                     | invoice_no                     | ✅ Mapped |
| invoice_date                   | invoice_date                   | ✅ Mapped |
| invoice_due_date               | invoice_due_date               | ✅ Mapped |
| purchase_order_no              | purchase_order_no              | ✅ Mapped |
| received_date                  | received_date                  | ✅ Mapped |
| office_vessel                  | office_vessel                  | ✅ Mapped |
| currency                       | currency                       | ✅ Mapped |
| total_amount                   | total_amount                   | ✅ Mapped |
| additional_costs               | additional_costs               | ✅ Mapped |
| additional_costs_total         | additional_costs_total         | ✅ Mapped |
| tax_details                    | tax_details                    | ✅ Mapped |
| tax_details_total              | tax_details_total              | ✅ Mapped |
| igst_total                     | igst_total                     | ✅ Mapped |
| department                     | department                     | ✅ Mapped |
| assignee                       | assignee                       | ✅ Mapped |
| invoice_file                   | invoice_file                   | ✅ Mapped |
| supporting_documents           | supporting_documents           | ✅ Mapped |

## Usage

### Basic Usage

```bash
python api_mapper.py
```

### Custom Configuration

You can modify the configuration by setting environment variables:

```bash
# Example: Limit records for testing
export MAX_RECORDS="10"

# Example: Use different table
export NEON_TABLE="test_invoices"
```

## Authentication Flow

The script handles authentication automatically:

1. **Login Request**: POST to `http://192.168.1.25:801/Authentication/AuthenticateUser`
2. **Session Management**: Uses ASP.NET session cookies for authentication
3. **API Requests**: Sends data to `http://192.168.1.25:801/InvoiceManager/AddUpdateInvoice` with session cookies

## Data Transformations

The script automatically applies the following transformations:

### Date Fields

- `invoice_date`, `invoice_due_date`, `received_date`
- Converts to `YYYY-MM-DD` format for API

### Numeric Fields

- `total_amount`, `additional_costs_total`, `igst_total`, `tax_details_total`
- Converts to string format with proper decimal handling

### JSON Fields

- `additional_costs`, `tax_details`
- Validates and preserves JSON structure

## Output

The script provides detailed output including:

- Field mappings display
- Authentication status
- Migration progress
- Success/failure statistics
- Error details

Example output:

```
=== Field Mappings (Neon -> API) ===
Neon Field                       API Parameter
------------------------------------------------------------
vendor_id                        vendor_id
org_id                           org_id
invoice_type                     invoice_type
...

2024-01-15 10:30:15 - INFO - Successfully authenticated with Retro API
2024-01-15 10:30:16 - INFO - Successfully connected to Neon database
2024-01-15 10:30:17 - INFO - Fetched 100 records from Neon database
2024-01-15 10:30:18 - INFO - Transformed 100 records for API
2024-01-15 10:30:19 - INFO - Successfully sent data to API: INV-001
...
2024-01-15 10:35:20 - INFO - Migration completed. Results: {'total': 100, 'successful': 98, 'failed': 2}
```

## Error Handling

The script includes comprehensive error handling:

- **Authentication Retries**: Handles authentication failures gracefully
- **Connection Retries**: Automatically retries failed database connections
- **API Error Handling**: Logs and continues on API request failures
- **Data Validation**: Validates data types and formats
- **Logging**: Detailed error logging for debugging

## Performance Optimization

- **Batch Processing**: Configurable batch sizes for optimal performance
- **Connection Pooling**: Efficient database connection management
- **Memory Management**: Processes data in chunks to avoid memory issues
- **Rate Limiting**: Built-in delays to avoid overwhelming the API

## Troubleshooting

### Common Issues

1. **Authentication Failed**

   - Verify username and password
   - Check if the authentication API is accessible
   - Ensure proper network connectivity

2. **Database Connection Failed**

   - Verify Neon database connection string
   - Check network connectivity
   - Ensure database credentials are correct

3. **API Request Failed**

   - Check if the Retro API endpoint is accessible
   - Verify session cookies are being sent
   - Check API response for specific error messages

4. **Data Transformation Errors**

   - Check the data transformation logic in the script
   - Verify field mappings in the configuration

### Debug Mode

Enable debug logging for detailed information:

```bash
export LOG_LEVEL="DEBUG"
```

## Security Considerations

- Store database credentials securely (use environment variables)
- Use encrypted connections when possible
- Implement proper access controls
- Regularly update dependencies
- Never commit credentials to version control

## API Endpoints

### Authentication

- **URL**: `http://192.168.1.25:801/Authentication/AuthenticateUser`
- **Method**: POST
- **Parameters**: `userName`, `password`
- **Response**: `[{"response": true, "message": "Success", "redirectLink": "/InvoiceManager/Overview"}]`

### Invoice Submission

- **URL**: `http://192.168.1.25:801/InvoiceManager/AddUpdateInvoice`
- **Method**: POST
- **Content-Type**: `application/x-www-form-urlencoded`
- **Authentication**: Session cookies

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is provided as-is for educational and development purposes.
