import requests
import json

def test_exact_working_format():
    """Send EXACTLY the same data as our working synthetic test, but with new invoice number"""
    
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
    
    # Step 2: Send EXACT copy of working synthetic test format
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
    
    # EXACT working format from our synthetic test, just with new invoice number
    form_data = {
        'data': json.dumps({
            "Status": "",
            "ApprovalStatus": "",
            "InternalReference": "",
            "DryDockInvoice": False,
            "Date": "2025-07-29T00:00:00.000Z",
            "DueDate": None,
            "ReceivedDate": None,
            "CounterParty": "94d965dd-c564-4ee1-b16b-00396d895457",
            "ReferenceNumber": "TEST-EXACT-WORKING-003",
            "Type": "99edfd0b-bcfc-4c2f-a745-fabb41f0765d",
            "SubTotal": None,
            "Remark": "Exact copy of working format",
            "Currency": "00098981-b3ec-45bb-bc3c-f29c7cdc07b0",
            "Location": "0af391de-d560-4848-a813-5fbbaf151d47",
            "Paid": False,
            "Due": False,
            "Overdue": False,
            "PendingAssignment": True,
            "externalId": "",
            "Organization": "d886152a-248f-40a9-bed9-3ac61c35be5a",
            "Department": "b5ae33a7-bf63-426e-95d0-b5c9c9e3803f",
            "TotalAmount": 1234,
            "AdditionalCostTotal": None,
            "GSTTotal": None,
            "CostCenter": "14!G!d490d9af-a497-4ea6-b807-cbbeae42c35b",
            "CorrespondingProformaInvoice": "65454",
            "CorrespondingProformaInvoiceExternalId": "",
            "RCMApplicable": False,
            "CargoType": "9!G!e2c8c531-8387-4e07-afac-24f8727ffa1b",
            "CharterType": "tc",
            "PurchaseOrderId": "",
            "PurchaseOrderRetroNETReference": "",
            "isServicePurchaseOrder": True,
            "TakeOverExpense": False
        }),
        
        # Additional cost data - exactly as working
        'aCostData': [
            json.dumps({"Name": "Courier Charge", "HSN_SAC": "bb", "Amount": 50, "GSTRate": "", "TaxTotal": 0, "Total": 50, "TaxAmount": 0, "externalId": "", "AdditionalCost": "1!G!2ffbabe8-f887-4f2e-a192-27e4ea748959"})
        ],
        
        # GST data - exactly as working
        'gstData': [
            json.dumps({"Rate": 0, "Amount": 0, "HSN_SAC": "", "TaxTotal": 0, "Total": 0, "GSTType": "na", "IGST": 0, "CGST": 0, "SGST": 0, "externalid": "", "GSTRate": "22!G!4237df5c-4ba1-4c51-b732-7a8a6326ecf0"}),
            json.dumps({"Rate": 3, "Amount": 5000, "HSN_SAC": "ACDC", "TaxTotal": 150, "Total": 5150, "GSTType": "na", "IGST": 0, "CGST": 75, "SGST": 75, "externalid": "", "GSTRate": "22!G!736942ae-ee67-4425-a65a-ea1c3daedecb"}),
            json.dumps({"Rate": 5, "Amount": 0, "HSN_SAC": "", "TaxTotal": 0, "Total": 0, "GSTType": "na", "IGST": 0, "CGST": 0, "SGST": 0, "externalid": "", "GSTRate": "22!G!2ba857d6-92b0-422b-a706-7d2a5bd69fbb"}),
            json.dumps({"Rate": 12, "Amount": 0, "HSN_SAC": "", "TaxTotal": 0, "Total": 0, "GSTType": "na", "IGST": 0, "CGST": 0, "SGST": 0, "externalid": "", "GSTRate": "22!G!fe027519-09aa-4ca8-b745-b080f31095d1"}),
            json.dumps({"Rate": 18, "Amount": 0, "HSN_SAC": "", "TaxTotal": 0, "Total": 0, "GSTType": "na", "IGST": 0, "CGST": 0, "SGST": 0, "externalid": "", "GSTRate": "22!G!90feecfa-b420-4d66-94ab-7ed16a455c41"}),
            json.dumps({"Rate": 28, "Amount": 0, "HSN_SAC": "", "TaxTotal": 0, "Total": 0, "GSTType": "na", "IGST": 0, "CGST": 0, "SGST": 0, "externalid": "", "GSTRate": "22!G!206be905-f3ba-4e72-b190-35d502d03690"})
        ],
        
        'masterEdit': 'false'
    }
    
    print("📤 Sending EXACT working format...")
    print(f"Invoice Reference: TEST-EXACT-WORKING-003")
    print(f"GST[1] Amount: 5000 with 150 tax (3% rate)")
    print(f"Additional Cost: 50 for Courier Charge")
    
    # Send the request
    response = requests.post(invoice_url, headers=headers, data=form_data, cookies=session_cookies)
    
    print(f"📡 API Response Status: {response.status_code}")
    print(f"📡 API Response: {response.text}")
    
    if response.status_code == 200:
        try:
            response_json = response.json()
            if response_json[0].get('response') == True:
                print("✅ SUCCESS! Invoice should appear in Retro")
            else:
                print(f"❌ API Error: {response_json[0].get('message')}")
        except:
            print(f"⚠️ Unexpected response: {response.text}")

if __name__ == "__main__":
    test_exact_working_format()