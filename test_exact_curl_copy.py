import requests
import json

def test_exact_curl_copy():
    """Test the EXACT curl format that works, with a new reference number"""
    
    # Step 1: Authenticate
    auth_url = "http://192.168.1.25:801/Authentication/AuthenticateUser"
    auth_params = {
        'userName': 'akshay',
        'password': 'retroinv@123'
    }
    
    print("🔐 Authenticating...")
    auth_response = requests.post(auth_url, params=auth_params)
    session_cookies = auth_response.cookies.get_dict()
    print(f"✅ Authentication successful")
    
    # Step 2: Send EXACT copy of working curl format
    invoice_url = "http://192.168.1.25:801/InvoiceManager/AddUpdateInvoice"
    
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
    
    # EXACT working format from curl - just changing reference number
    form_data = {
        'data': json.dumps({
            "Status": "",
            "ApprovalStatus": "",
            "InternalReference": "",
            "DryDockInvoice": False,
            "Date": "2025-07-15T18:30:00.000Z",
            "DueDate": "2025-07-16T18:30:00.000Z",
            "ReceivedDate": "2025-07-16T18:30:00.000Z",
            "CounterParty": "15!G!717acd53-286b-413f-936e-84b2505a6fe3",
            "ReferenceNumber": "CURL-COPY-TEST-001",  # Changed this
            "Type": "35!G!4333c68d-28bd-4607-b18c-c97cc6580db5",
            "SubTotal": None,
            "Remark": "Exact copy of working curl format",
            "Currency": "17!G!00098981-b3ec-45bb-bc3c-f29c7cdc07b0",
            "Location": "26!G!f35de236-4bdc-48b8-b3d2-9bd5c8273843",
            "Paid": False,
            "Due": False,
            "Overdue": False,
            "PendingAssignment": True,
            "externalId": "",
            "Organization": "28!G!de9c5634-6948-446f-9ff8-8e824994b423",
            "Department": "18!G!b5ae33a7-bf63-426e-95d0-b5c9c9e3803f",
            "TotalAmount": 7210,  # EXACT working value
            "AdditionalCostTotal": None,
            "GSTTotal": None,
            "CostCenter": "14!G!d490d9af-a497-4ea6-b807-cbbeae42c35b",
            "CorrespondingProformaInvoice": "",
            "CorrespondingProformaInvoiceExternalId": "",
            "RCMApplicable": False,
            "CargoType": "9!G!e2c8c531-8387-4e07-afac-24f8727ffa1b",
            "CharterType": "tc",
            "PurchaseOrderId": "CRG/SER/ENG/23/0500-A",
            "PurchaseOrderRetroNETReference": "6c285600-93ac-4bc4-bef5-487c2ca3504e",
            "isServicePurchaseOrder": True,
            "TakeOverExpense": False
        }),
        
        'masterEdit': 'false'
    }
    
    # Add multiple aCostData entries exactly as in curl
    acost_entries = [
        '{"Name":"Cess","HSN_SAC":"","Amount":0,"GSTRate":"","TaxTotal":0,"Total":0,"TaxAmount":0,"externalId":"","AdditionalCost":"1!G!0ff1217d-fcd0-4ec1-9f5b-0543af73d7ed"}',
        '{"Name":"Courier Charge","HSN_SAC":"TESTING","Amount":2000,"GSTRate":"22!G!906e8b3a-8817-4fc1-8e9c-fc8c5f6b3fbe","TaxTotal":60,"Total":2060,"TaxAmount":0,"externalId":"","AdditionalCost":"1!G!f34767f5-cd75-4aa8-95f7-0e78167fc493"}',
        '{"Name":"Transportation Charge","HSN_SAC":"","Amount":0,"GSTRate":"","TaxTotal":0,"Total":0,"TaxAmount":0,"externalId":"","AdditionalCost":"1!G!482e3378-f20e-4d68-9b23-6885f8b9aaf5"}',
        '{"Name":"Delivery Charge","HSN_SAC":"","Amount":0,"GSTRate":"","TaxTotal":0,"Total":0,"TaxAmount":0,"externalId":"","AdditionalCost":"1!G!0a46e8f4-9f8d-401e-b9cb-fd420db1de7b"}'
    ]
    
    # Add multiple gstData entries exactly as in curl  
    gst_entries = [
        '{"Rate":0,"Amount":0,"HSN_SAC":"","TaxTotal":0,"Total":0,"GSTType":"na","IGST":0,"CGST":0,"SGST":0,"externalid":"","GSTRate":"22!G!26fd346e-e9f1-4e50-8c26-05859f753250"}',
        '{"Rate":3,"Amount":5000,"HSN_SAC":"TESTING","TaxTotal":150,"Total":5150,"GSTType":"na","IGST":0,"CGST":0,"SGST":0,"externalid":"","GSTRate":"22!G!906e8b3a-8817-4fc1-8e9c-fc8c5f6b3fbe"}',
        '{"Rate":5,"Amount":0,"HSN_SAC":"","TaxTotal":0,"Total":0,"GSTType":"na","IGST":0,"CGST":0,"SGST":0,"externalid":"","GSTRate":"22!G!b0f98850-b5ba-4f71-8536-2121b7c06ad7"}',
        '{"Rate":12,"Amount":0,"HSN_SAC":"","TaxTotal":0,"Total":0,"GSTType":"na","IGST":0,"CGST":0,"SGST":0,"externalid":"","GSTRate":"22!G!030734df-53ae-4cb9-803d-c8929f11152c"}',
        '{"Rate":18,"Amount":0,"HSN_SAC":"","TaxTotal":0,"Total":0,"GSTType":"na","IGST":0,"CGST":0,"SGST":0,"externalid":"","GSTRate":"22!G!01482a49-1aee-4a46-8043-78a74c95b034"}',
        '{"Rate":28,"Amount":0,"HSN_SAC":"","TaxTotal":0,"Total":0,"GSTType":"na","IGST":0,"CGST":0,"SGST":0,"externalid":"","GSTRate":"22!G!96a553ec-2f36-4f3f-a66a-77780c5bb1ec"}'
    ]
    
    # Simulate multiple form fields with same name using requests
    form_list = []
    form_list.append(('data', form_data['data']))
    form_list.append(('masterEdit', form_data['masterEdit']))
    
    for entry in acost_entries:
        form_list.append(('aCostData', entry))
        
    for entry in gst_entries:
        form_list.append(('gstData', entry))
    
    print("📤 Sending EXACT curl copy format...")
    print(f"Invoice Reference: CURL-COPY-TEST-001")
    print(f"TotalAmount: 7210")
    print(f"GST Entry with Amount: 5000, TaxTotal: 150, Total: 5150")
    print(f"Additional Cost with Amount: 2000, TaxTotal: 60, Total: 2060")
    
    # Send the request
    response = requests.post(invoice_url, headers=headers, data=form_list, cookies=session_cookies)
    
    print(f"📡 API Response Status: {response.status_code}")
    print(f"📡 API Response: {response.text}")
    
    if response.status_code == 200:
        try:
            response_data = response.json()
            if isinstance(response_data, list) and len(response_data) > 0:
                if response_data[0].get('response') == True:
                    print("✅ SUCCESS! Invoice should appear in Retro with NON-ZERO amounts!")
                    return True
                else:
                    print(f"❌ API Error: {response_data[0].get('message', 'Unknown error')}")
                    return False
        except:
            print(f"❌ Failed to parse response: {response.text}")
            return False
    else:
        print(f"❌ HTTP Error: {response.status_code}")
        return False

if __name__ == "__main__":
    test_exact_curl_copy()