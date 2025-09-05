#!/usr/bin/env python3
"""
Test the complete flow: Authentication + Invoice Creation
"""

import asyncio
import logging
from api_mapper import APINeonToRetroMapper

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_complete_flow():
    """Test the complete flow with authentication + invoice creation"""
    
    # Create mapper instance
    neon_connection_string = "postgresql://neondb_owner:npg_ziNBtp5sX4Fv@ep-quiet-forest-a53t111o-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    auth_api_url = "http://192.168.1.25:801"
    retro_api_url = "http://192.168.1.25:801"
    username = "akshay"
    password = "retroinv@123"
    
    logger.info("🚀 TESTING COMPLETE FLOW: AUTH + INVOICE CREATION")
    logger.info("=" * 60)
    
    mapper = APINeonToRetroMapper(
        neon_connection_string=neon_connection_string,
        auth_api_url=auth_api_url,
        retro_api_url=retro_api_url,
        username=username,
        password=password
    )
    
    try:
        # Run the complete migration for record 42
        logger.info("🎯 Running complete migration for record ID=42...")
        
        results = await mapper.run_migration(
            neon_table="invoices",
            limit=1,
            record_id=42
        )
        
        logger.info("📊 MIGRATION RESULTS:")
        logger.info(f"  Success: {results.get('success', 'Unknown')}")
        logger.info(f"  Total: {results.get('total', 0)}")
        logger.info(f"  Successful: {results.get('successful', 0)}")
        logger.info(f"  Failed: {results.get('failed', 0)}")
        
        if results.get('details'):
            logger.info("📋 Details:")
            for detail in results['details']:
                if 'reference' in detail:
                    logger.info(f"  ✅ Created invoice: {detail['reference']}")
                else:
                    logger.info(f"  ❌ Failed: {detail}")
        
        if results.get('successful', 0) > 0:
            logger.info("🎉 COMPLETE FLOW TEST PASSED!")
            logger.info("✅ Authentication + Invoice Creation working!")
            
            # Get the reference number for verification
            if results.get('details') and len(results['details']) > 0:
                reference = results['details'][0].get('reference')
                if reference:
                    logger.info(f"🔍 VERIFICATION COMMAND:")
                    logger.info(f'curl -X POST "http://192.168.1.25:801/InvoiceManager/GetAllInvoicePendingAssignment" -H "Accept: application/json" -H "Cookie: ASP.NET_SessionId={mapper.session_cookies.get("ASP.NET_SessionId")}" --data "" | jq \'.Data[] | select(.ReferenceNumber=="{reference}")\'')
            
            return True
        else:
            logger.error("💥 COMPLETE FLOW TEST FAILED!")
            return False
            
    except Exception as e:
        logger.error(f"💥 Error during complete flow test: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_complete_flow())
    if success:
        print("\n🎯 AUTHENTICATION + INVOICE CREATION WORKING!")
        print("🚀 READY FOR PRODUCTION DEPLOYMENT!")
    else:
        print("\n💥 ISSUES FOUND - NEED TO FIX BEFORE DEPLOYMENT!")