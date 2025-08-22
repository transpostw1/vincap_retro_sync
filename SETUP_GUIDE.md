# 🚀 Neon to Retro API Mapper - Setup Guide

## 📦 **What You'll Receive**

- `api-mapper.tar` - Docker image (140MB)
- `docker-compose.yml` - Docker Compose configuration
- `SETUP_GUIDE.md` - This guide

## 🛠️ **Prerequisites**

- Docker Desktop installed and running
- PowerShell or Command Prompt

## 📋 **Quick Setup (3 Steps)**

### **Step 1: Load the Docker Image**

```bash
# Load the Docker image
docker load -i api-mapper.tar
```

### **Step 2: Create docker-compose.yml**

Create a file named `docker-compose.yml` with this content:

```yaml
services:
  api-mapper:
    image: api-mapper:latest
    ports:
      - "8000:8000"
    environment:
      - NEON_CONNECTION_STRING=postgresql://neondb_owner:npg_ziNBtp5sX4Fv@ep-quiet-forest-a53t111o-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require
      - AUTH_API_URL=http://192.168.1.25:801
      - RETRO_API_URL=http://192.168.1.25:801
      - API_USERNAME=akshay
      - API_PASSWORD=retroinv@123
    restart: unless-stopped
```

### **Step 3: Start the Service**

```bash
# Start the API service
docker-compose up -d
```

## ✅ **Verify Installation**

```bash
# Check if service is running
curl http://localhost:8000/health

# Or in PowerShell:
Invoke-RestMethod -Uri "http://localhost:8000/health" -Method GET
```

## 🎯 **Usage Examples**

### **List Available Records**

```bash
# PowerShell
Invoke-RestMethod -Uri "http://localhost:8000/list-records" -Method GET

# Or curl
curl http://localhost:8000/list-records
```

### **Push Single Record**

```bash
# PowerShell
Invoke-RestMethod -Uri "http://localhost:8000/push" -Method POST -ContentType "application/json" -Body '{"record_id": 24}'

# Or curl
curl -X POST http://localhost:8000/push \
  -H "Content-Type: application/json" \
  -d '{"record_id": 24}'
```

### **Push Multiple Records**

```bash
# PowerShell
Invoke-RestMethod -Uri "http://localhost:8000/push" -Method POST -ContentType "application/json" -Body '{"limit": 5}'

# Or curl
curl -X POST http://localhost:8000/push \
  -H "Content-Type: application/json" \
  -d '{"limit": 5}'
```

## 🔧 **API Endpoints**

| Endpoint           | Method | Description              |
| ------------------ | ------ | ------------------------ |
| `/health`          | GET    | Health check             |
| `/list-records`    | GET    | List available records   |
| `/push`            | POST   | Push data to Retro API   |
| `/push/async`      | POST   | Push data asynchronously |
| `/test-connection` | GET    | Test connections         |
| `/mappings`        | GET    | Get field mappings       |

## 📱 **Next.js Integration**

### **API Route Example**

```javascript
// pages/api/push.js
export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const { record_id, limit } = req.body;

    const response = await fetch("http://localhost:8000/push", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        record_id: record_id || null,
        limit: limit || 10,
      }),
    });

    const data = await response.json();
    res.status(200).json(data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}
```

### **React Component Example**

```javascript
// components/PushForm.js
import { useState } from "react";

export default function PushForm() {
  const [recordId, setRecordId] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);

  const handlePush = async (e) => {
    e.preventDefault();
    setLoading(true);

    try {
      const response = await fetch("/api/push", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ record_id: parseInt(recordId) }),
      });

      const data = await response.json();
      setResult(data);
    } catch (error) {
      setResult({ success: false, error: error.message });
    } finally {
      setLoading(false);
    }
  };

  return (
    <form onSubmit={handlePush}>
      <input
        type="number"
        value={recordId}
        onChange={(e) => setRecordId(e.target.value)}
        placeholder="Enter Record ID"
      />
      <button type="submit" disabled={loading}>
        {loading ? "Pushing..." : "Push Record"}
      </button>
      {result && (
        <div className={result.success ? "success" : "error"}>
          {result.message}
        </div>
      )}
    </form>
  );
}
```

## 🐛 **Troubleshooting**

### **Service Won't Start**

```bash
# Check Docker logs
docker-compose logs

# Check if port 8000 is available
netstat -an | findstr :8000
```

### **Connection Issues**

```bash
# Test connections
Invoke-RestMethod -Uri "http://localhost:8000/test-connection" -Method GET
```

### **Image Loading Issues**

```bash
# Verify image was loaded
docker images | findstr api-mapper

# Reload if needed
docker load -i api-mapper.tar
```

## 🚀 **Management Commands**

```bash
# Start service
docker-compose up -d

# Stop service
docker-compose down

# View logs
docker-compose logs -f

# Restart service
docker-compose restart

# Update image (if new version provided)
docker load -i api-mapper.tar
docker-compose up -d
```

## 📞 **Support**

If you encounter any issues:

1. Check the logs: `docker-compose logs`
2. Verify Docker is running
3. Ensure port 8000 is not in use
4. Test connections: `GET /test-connection`

---

**Happy coding! 🎉**
