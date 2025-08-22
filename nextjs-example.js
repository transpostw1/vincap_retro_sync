// Next.js API route example for calling the migration API
// File: pages/api/migrate.js

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const { record_id, limit, table_name } = req.body;

    // Call the migration API
    const response = await fetch('http://localhost:8000/migrate', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        record_id: record_id || null,
        limit: limit || 10,
        table_name: table_name || 'invoices'
      }),
    });

    const data = await response.json();
    
    if (response.ok) {
      res.status(200).json(data);
    } else {
      res.status(response.status).json(data);
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}

// Next.js component example for calling the migration API
// File: components/MigrationForm.js

import { useState } from 'react';

export default function MigrationForm() {
  const [recordId, setRecordId] = useState('');
  const [limit, setLimit] = useState(10);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);

  const handleMigration = async (e) => {
    e.preventDefault();
    setLoading(true);
    setResult(null);

    try {
      const response = await fetch('/api/migrate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          record_id: recordId ? parseInt(recordId) : null,
          limit: parseInt(limit),
          table_name: 'invoices'
        }),
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
    <div className="max-w-md mx-auto mt-8 p-6 bg-white rounded-lg shadow-md">
      <h2 className="text-2xl font-bold mb-4">Data Migration</h2>
      
      <form onSubmit={handleMigration} className="space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700">
            Record ID (optional)
          </label>
          <input
            type="number"
            value={recordId}
            onChange={(e) => setRecordId(e.target.value)}
            placeholder="Leave empty for multiple records"
            className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700">
            Limit
          </label>
          <input
            type="number"
            value={limit}
            onChange={(e) => setLimit(e.target.value)}
            min="1"
            max="100"
            className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
          />
        </div>

        <button
          type="submit"
          disabled={loading}
          className="w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50"
        >
          {loading ? 'Migrating...' : 'Start Migration'}
        </button>
      </form>

      {result && (
        <div className={`mt-4 p-4 rounded-md ${result.success ? 'bg-green-50 text-green-800' : 'bg-red-50 text-red-800'}`}>
          <h3 className="font-medium">{result.success ? 'Success!' : 'Error'}</h3>
          <p className="mt-1">{result.message}</p>
          {result.results && (
            <pre className="mt-2 text-sm bg-gray-100 p-2 rounded">
              {JSON.stringify(result.results, null, 2)}
            </pre>
          )}
          {result.error && (
            <p className="mt-1 text-sm">{result.error}</p>
          )}
        </div>
      )}
    </div>
  );
}

// Example API calls from Next.js

// 1. Migrate a specific record
const migrateSingleRecord = async (recordId) => {
  const response = await fetch('/api/migrate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ record_id: recordId })
  });
  return response.json();
};

// 2. Migrate multiple records
const migrateMultipleRecords = async (limit = 10) => {
  const response = await fetch('/api/migrate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ limit })
  });
  return response.json();
};

// 3. Async migration (fire and forget)
const migrateAsync = async (recordId) => {
  const response = await fetch('/api/migrate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ 
      record_id: recordId,
      async: true 
    })
  });
  return response.json();
};

// 4. Test connections
const testConnections = async () => {
  const response = await fetch('http://localhost:8000/test-connection');
  return response.json();
};

// 5. Get field mappings
const getMappings = async () => {
  const response = await fetch('http://localhost:8000/mappings');
  return response.json();
}; 