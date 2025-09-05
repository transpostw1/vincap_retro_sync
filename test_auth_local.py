#!/usr/bin/env python3
"""
Test script to verify authentication is working locally
"""

import asyncio
import logging
from api_mapper import APINeonToRetroMapper

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_authentication():
    """Test the authentication functionality"""
    
    # Create mapper instance with test credentials
    neon_connection_string = "postgresql://neondb_owner:npg_ziNBtp5sX4Fv@ep-quiet-forest-a53t111o-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    auth_api_url = "http://192.168.1.25:801"
    retro_api_url = "http://192.168.1.25:801"
    username = "akshay"
    password = "retroinv@123"
    
    logger.info("🧪 TESTING AUTHENTICATION LOCALLY")
    logger.info("=" * 50)
    
    mapper = APINeonToRetroMapper(
        neon_connection_string=neon_connection_string,
        auth_api_url=auth_api_url,
        retro_api_url=retro_api_url,
        username=username,
        password=password
    )
    
    # Test authentication
    logger.info("1️⃣ Testing authentication...")
    auth_success = await mapper.authenticate()
    
    if auth_success:
        logger.info("✅ Authentication successful!")
        logger.info(f"🍪 Session cookies: {mapper.session_cookies}")
        
        # Test database connection
        logger.info("2️⃣ Testing database connection...")
        db_success = await mapper.connect_neon()
        
        if db_success:
            logger.info("✅ Database connection successful!")
            
            # Test a simple query
            logger.info("3️⃣ Testing simple database query...")
            query = "SELECT id, invoice_no, total_amount FROM invoices WHERE id = 42"
            record = await mapper.neon_conn.fetchrow(query)
            
            if record:
                logger.info(f"✅ Query successful! Found record: ID={record[0]}, Invoice={record[1]}, Amount={record[2]}")
                
                # Close connection
                await mapper.neon_conn.close()
                
                logger.info("🎉 ALL TESTS PASSED!")
                logger.info("Authentication and database connectivity are working!")
                return True
            else:
                logger.error("❌ No record found with ID=42")
                await mapper.neon_conn.close()
                return False
        else:
            logger.error("❌ Database connection failed!")
            return False
    else:
        logger.error("❌ Authentication failed!")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_authentication())
    if success:
        print("\n🎯 READY FOR PRODUCTION DEPLOYMENT!")
    else:
        print("\n💥 ISSUES FOUND - NEED TO FIX BEFORE DEPLOYMENT!")