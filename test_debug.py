#!/usr/bin/env python3
import asyncio
import asyncpg
import json
from datetime import datetime

async def test_database_connection():
    """Test direct database connection to see what records exist"""
    try:
        # Database connection
        conn = await asyncpg.connect(
            host="ep-floral-scene-a5jzx2vp.us-east-2.aws.neon.tech",
            port=5432,
            user="neondb_owner",
            password="7GbGH7fJpQw4",
            database="neondb",
            ssl="require"
        )
        
        print("✅ Connected to Neon database successfully")
        
        # Check what records exist
        query = "SELECT id, invoice_no FROM invoices ORDER BY id LIMIT 20"
        records = await conn.fetch(query)
        
        print(f"\n📊 Found {len(records)} records in database:")
        for record in records:
            print(f"  ID: {record['id']}, Invoice: {record['invoice_no']}")
        
        # Test specific records
        test_ids = [28, 32, 42]
        for test_id in test_ids:
            query = "SELECT id, invoice_no, total_amount FROM invoices WHERE id = $1"
            result = await conn.fetch(query, test_id)
            if result:
                print(f"\n✅ Record {test_id} EXISTS: {result[0]['invoice_no']} - Amount: {result[0]['total_amount']}")
            else:
                print(f"\n❌ Record {test_id} NOT FOUND")
        
        await conn.close()
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_database_connection())