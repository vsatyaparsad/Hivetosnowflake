import { SQLMapping } from '../types/sql';

export const dataTypeMappings: SQLMapping[] = [
  // Numeric Types with Precision/Scale
  { hiveType: 'TINYINT', snowflakeType: 'NUMBER(3,0)' },
  { hiveType: 'SMALLINT', snowflakeType: 'NUMBER(5,0)' },
  { hiveType: 'INT', snowflakeType: 'NUMBER(10,0)' },
  { hiveType: 'INTEGER', snowflakeType: 'NUMBER(10,0)' },
  { hiveType: 'BIGINT', snowflakeType: 'NUMBER(38,0)' },
  { hiveType: 'FLOAT', snowflakeType: 'FLOAT' },
  { hiveType: 'DOUBLE', snowflakeType: 'DOUBLE' },
  { hiveType: 'DOUBLE PRECISION', snowflakeType: 'DOUBLE' },
  { hiveType: 'DECIMAL', snowflakeType: 'NUMBER' },
  { hiveType: 'NUMERIC', snowflakeType: 'NUMBER' },
  
  // String Types
  { hiveType: 'STRING', snowflakeType: 'VARCHAR' },
  { hiveType: 'VARCHAR', snowflakeType: 'VARCHAR' },
  { hiveType: 'CHAR', snowflakeType: 'CHAR' },
  { hiveType: 'TEXT', snowflakeType: 'TEXT' },
  
  // Date/Time Types
  { hiveType: 'TIMESTAMP', snowflakeType: 'TIMESTAMP_NTZ' },
  { hiveType: 'TIMESTAMP WITH LOCAL TIME ZONE', snowflakeType: 'TIMESTAMP_LTZ' },
  { hiveType: 'TIMESTAMP WITH TIME ZONE', snowflakeType: 'TIMESTAMP_TZ' },
  { hiveType: 'DATE', snowflakeType: 'DATE' },
  { hiveType: 'INTERVAL', snowflakeType: 'VARCHAR' },
  
  // Boolean Type
  { hiveType: 'BOOLEAN', snowflakeType: 'BOOLEAN' },
  { hiveType: 'BOOL', snowflakeType: 'BOOLEAN' },
  
  // Binary Types
  { hiveType: 'BINARY', snowflakeType: 'BINARY' },
  { hiveType: 'VARBINARY', snowflakeType: 'BINARY' },
  
  // Complex Types
  { hiveType: 'ARRAY', snowflakeType: 'ARRAY' },
  { hiveType: 'MAP', snowflakeType: 'OBJECT' },
  { hiveType: 'STRUCT', snowflakeType: 'OBJECT' },
  { hiveType: 'UNIONTYPE', snowflakeType: 'VARIANT' }
];