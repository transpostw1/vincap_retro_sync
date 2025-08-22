#!/usr/bin/env python3
"""
FastAPI Server for Neon to Retro API Data Mapper
Provides REST API endpoints for the data migration functionality.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import asyncio
import os
import logging
from api_mapper import APINeonToRetroMapper

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Neon to Retro API Mapper",
    description="API for migrating data from Neon database to Retro API",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for request/response
class MigrationRequest(BaseModel):
    record_id: Optional[int] = None
    limit: Optional[int] = 10
    table_name: str = "invoices"

class MigrationResponse(BaseModel):
    success: bool
    message: str
    results: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    message: str

# Global mapper instance
mapper = None

def get_mapper():
    """Get or create mapper instance"""
    global mapper
    if mapper is None:
        # Configuration from environment variables
        neon_connection_string = os.getenv(
            "NEON_CONNECTION_STRING", 
            "postgresql://neondb_owner:npg_ziNBtp5sX4Fv@ep-quiet-forest-a53t111o-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
        )
        
        auth_api_url = os.getenv("AUTH_API_URL", "http://192.168.1.25:801")
        retro_api_url = os.getenv("RETRO_API_URL", "http://192.168.1.25:801")
        username = os.getenv("API_USERNAME", "akshay")
        password = os.getenv("API_PASSWORD", "retroinv@123")
        
        mapper = APINeonToRetroMapper(neon_connection_string, auth_api_url, retro_api_url, username, password)
    
    return mapper

@app.get("/", response_model=HealthResponse)
async def root():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        message="Neon to Retro API Mapper is running"
    )

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        message="Neon to Retro API Mapper is running"
    )

@app.post("/push", response_model=MigrationResponse)
async def push_data(request: MigrationRequest):
    """Migrate data from Neon to Retro API"""
    try:
        logger.info(f"Starting migration with request: {request}")
        
        # Get mapper instance
        mapper_instance = get_mapper()
        
        # Run migration
        results = await mapper_instance.run_migration(
            neon_table=request.table_name,
            limit=request.limit,
            record_id=request.record_id if request.record_id else None
        )
        
        if results:
            return MigrationResponse(
                success=True,
                message="Migration completed successfully",
                results=results
            )
        else:
            return MigrationResponse(
                success=False,
                message="Migration failed",
                error="No results returned"
            )
            
    except Exception as e:
        logger.error(f"Migration error: {e}")
        return MigrationResponse(
            success=False,
            message="Migration failed",
            error=str(e)
        )

@app.post("/push/async", response_model=MigrationResponse)
async def push_data_async(request: MigrationRequest, background_tasks: BackgroundTasks):
    """Migrate data asynchronously (fire and forget)"""
    try:
        logger.info(f"Starting async migration with request: {request}")
        
        # Add push task to background
        background_tasks.add_task(run_push_async, request)
        
        return MigrationResponse(
            success=True,
            message="Migration started in background"
        )
        
    except Exception as e:
        logger.error(f"Async migration error: {e}")
        return MigrationResponse(
            success=False,
            message="Failed to start migration",
            error=str(e)
        )

async def run_push_async(request: MigrationRequest):
    """Run migration in background"""
    try:
        mapper_instance = get_mapper()
        results = await mapper_instance.run_migration(
            neon_table=request.table_name,
            limit=request.limit,
            record_id=str(request.record_id) if request.record_id else None
        )
        logger.info(f"Background push completed: {results}")
    except Exception as e:
        logger.error(f"Background push failed: {e}")

@app.get("/mappings")
async def get_mappings():
    """Get field mappings information"""
    try:
        mapper_instance = get_mapper()
        
        # Create mappings info
        mappings_info = {
            "field_mappings": mapper_instance.field_mappings,
            "special_fields": list(mapper_instance.special_fields.keys()),
            "date_fields": mapper_instance.date_fields,
            "numeric_fields": mapper_instance.numeric_fields,
            "json_fields": mapper_instance.json_fields
        }
        
        return {
            "success": True,
            "mappings": mappings_info
        }
        
    except Exception as e:
        logger.error(f"Error getting mappings: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/list-records")
async def list_records(limit: int = 5):
    """List available records in the database"""
    try:
        mapper_instance = get_mapper()
        
        # Connect to database
        db_success = await mapper_instance.connect_neon()
        if not db_success:
            return {"success": False, "error": "Database connection failed"}
        
        # Fetch records
        query = f"SELECT id, invoice_no, invoice_date, total_amount FROM invoices LIMIT {limit}"
        rows = await mapper_instance.neon_conn.fetch(query)
        
        # Close connection
        await mapper_instance.neon_conn.close()
        
        records = []
        for row in rows:
            records.append({
                "id": row[0],
                "invoice_no": row[1],
                "invoice_date": str(row[2]) if row[2] else None,
                "total_amount": float(row[3]) if row[3] else 0
            })
        
        return {
            "success": True,
            "records": records,
            "total_found": len(records)
        }
        
    except Exception as e:
        logger.error(f"Error listing records: {e}")
        return {"success": False, "error": str(e)}

@app.get("/test-connection")
async def test_connection():
    """Test database and API connections"""
    try:
        mapper_instance = get_mapper()
        
        # Test authentication
        auth_success = await mapper_instance.authenticate()
        
        # Test database connection
        db_success = await mapper_instance.connect_neon()
        
        results = {
            "authentication": "success" if auth_success else "failed",
            "database_connection": "success" if db_success else "failed"
        }
        
        # Close database connection
        if mapper_instance.neon_conn:
            await mapper_instance.neon_conn.close()
        
        return {
            "success": True,
            "connection_tests": results
        }
        
    except Exception as e:
        logger.error(f"Connection test error: {e}")
        return {
            "success": False,
            "error": str(e)
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 