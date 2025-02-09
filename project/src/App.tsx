import React, { useState } from 'react';
import { Code2, ArrowRightLeft } from 'lucide-react';
import { SQLConverter } from './lib/sqlConverter';

function App() {
  const [hiveSQL, setHiveSQL] = useState('');
  const [snowflakeSQL, setSnowflakeSQL] = useState('');
  const [error, setError] = useState('');
  const [warnings, setWarnings] = useState<string[]>([]);
  const [isConverting, setIsConverting] = useState(false);

  const handleConvert = () => {
    if (!hiveSQL.trim()) {
      setError('Please enter some Hive SQL to convert');
      return;
    }

    setIsConverting(true);
    setError('');
    setWarnings([]);
    setSnowflakeSQL('');

    try {
      const converter = new SQLConverter();
      const result = converter.convert(hiveSQL.trim());

      if (result.success && result.sql) {
        setSnowflakeSQL(result.sql);
        setWarnings(result.warnings || []);
      } else {
        throw new Error(result.errors?.[0] || 'Conversion failed');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setIsConverting(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 py-8 px-4">
      <div className="max-w-6xl mx-auto">
        <div className="flex items-center gap-2 mb-8">
          <Code2 className="w-8 h-8 text-blue-600" />
          <h1 className="text-3xl font-bold text-gray-900">Hive to Snowflake SQL Converter</h1>
        </div>

        <div className="grid md:grid-cols-2 gap-6">
          <div className="space-y-2">
            <label className="block text-sm font-medium text-gray-700">Hive SQL</label>
            <textarea
              value={hiveSQL}
              onChange={(e) => setHiveSQL(e.target.value)}
              className="w-full h-[400px] p-4 border border-gray-300 rounded-lg font-mono text-sm resize-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="Enter your Hive SQL here..."
              disabled={isConverting}
            />
          </div>

          <div className="space-y-2">
            <label className="block text-sm font-medium text-gray-700">Snowflake SQL</label>
            <textarea
              value={snowflakeSQL}
              readOnly
              className="w-full h-[400px] p-4 border border-gray-300 rounded-lg font-mono text-sm bg-gray-50 resize-none"
              placeholder="Converted Snowflake SQL will appear here..."
            />
          </div>
        </div>

        {warnings.length > 0 && (
          <div className="mt-4 p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
            <h3 className="text-sm font-medium text-yellow-800 mb-2">Warnings:</h3>
            <ul className="list-disc list-inside space-y-1 text-yellow-700 text-sm">
              {warnings.map((warning, index) => (
                <li key={index}>{warning}</li>
              ))}
            </ul>
          </div>
        )}

        {error && (
          <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">
            {error}
          </div>
        )}

        <button
          onClick={handleConvert}
          disabled={isConverting}
          className={`mt-6 flex items-center gap-2 px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed`}
        >
          <ArrowRightLeft className={`w-5 h-5 ${isConverting ? 'animate-spin' : ''}`} />
          {isConverting ? 'Converting...' : 'Convert to Snowflake SQL'}
        </button>

        <div className="mt-8 p-4 bg-blue-50 rounded-lg">
          <h2 className="text-lg font-semibold text-gray-900 mb-2">Supported Conversions</h2>
          <ul className="list-disc list-inside space-y-1 text-gray-600">
            <li>Date/Time Functions (UNIX_TIMESTAMP, FROM_UNIXTIME)</li>
            <li>Conditional Functions (NVL → COALESCE, IF → IFF)</li>
            <li>Array/Map Functions (ARRAY, MAP, SPLIT)</li>
            <li>Aggregate Functions (COLLECT_LIST → ARRAY_AGG)</li>
            <li>String Functions (REGEXP_REPLACE)</li>
            <li>Table Functions (LATERAL VIEW EXPLODE → FLATTEN)</li>
            <li>Data Types (Hive to Snowflake type mappings)</li>
            <li>Distribution (DISTRIBUTE BY → ORDER BY)</li>
          </ul>
        </div>
      </div>
    </div>
  );
}

export default App;