import { ConversionResult } from '../types/sql';

export class SQLConverter {
  private warnings: string[] = [];

  convert(hiveSQL: string): ConversionResult {
    try {
      // Clean and preprocess SQL
      let sql = this.preprocessSQL(hiveSQL);
      
      // Convert specific Hive constructs
      sql = this.convertCreateTable(sql);
      sql = this.convertInsertStatements(sql);
      sql = this.convertFunctions(sql);
      sql = this.convertLateralView(sql);
      sql = this.convertDataTypes(sql);
      sql = this.convertDistributeBy(sql);
      sql = this.convertComplexTypes(sql);
      sql = this.convertFileFormats(sql);
      sql = this.convertBucketingAndSorting(sql);
      sql = this.convertSkewHandling(sql);
      sql = this.convertRecursiveCTE(sql);
      
      return {
        success: true,
        sql: this.formatSQL(sql),
        warnings: this.warnings
      };
    } catch (error) {
      return {
        success: false,
        errors: [error instanceof Error ? error.message : 'Unknown error'],
        warnings: this.warnings
      };
    }
  }

  private preprocessSQL(sql: string): string {
    // Remove all SET commands with detailed patterns
    const setPatterns = [
      // Hive configuration settings
      /SET\s+hive\.[^;]+;/gi,
      // MapReduce settings
      /SET\s+mapred\.[^;]+;/gi,
      // Tez settings
      /SET\s+tez\.[^;]+;/gi,
      // Spark settings
      /SET\s+spark\.[^;]+;/gi,
      // Dynamic partition settings
      /SET\s+hive\.exec\.dynamic\.partition[^;]+;/gi,
      // Parallel execution settings
      /SET\s+hive\.exec\.parallel[^;]+;/gi,
      // Compression settings
      /SET\s+hive\.exec\.compress\.[^;]+;/gi,
      // Memory settings
      /SET\s+mapreduce\.map\.memory\.[^;]+;/gi,
      /SET\s+mapreduce\.reduce\.memory\.[^;]+;/gi,
      // Join settings
      /SET\s+hive\.auto\.convert\.join[^;]+;/gi,
      // Bucketing settings
      /SET\s+hive\.enforce\.bucketing[^;]+;/gi,
      // Statistics settings
      /SET\s+hive\.stats\.[^;]+;/gi,
      // Optimization settings
      /SET\s+hive\.optimize\.[^;]+;/gi,
      // Any remaining SET commands
      /SET\s+[^=;]+=\s*[^;]+;/gi
    ];

    // Apply each pattern and track removed settings
    setPatterns.forEach(pattern => {
      const matches = sql.match(pattern);
      if (matches) {
        matches.forEach(match => {
          this.warnings.push(`Removed Hive setting: ${match.trim()}`);
        });
      }
      sql = sql.replace(pattern, '');
    });
    
    // Remove Hive hints
    sql = sql.replace(/\/\*\+\s*(?:MAPJOIN|STREAMTABLE|BROADCAST|SKEWJOIN)\([^)]*\)\s*\*\//g, '');
    
    // Clean up comments and whitespace
    sql = sql
      .replace(/--.*$/gm, '')
      .replace(/\/\*[\s\S]*?\*\//g, '')
      .replace(/\s+/g, ' ')
      .trim();

    return sql;
  }

  private convertCreateTable(sql: string): string {
    // Convert CREATE TABLE syntax
    sql = sql.replace(
      /CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/gi,
      'CREATE OR REPLACE TABLE $1'
    );

    // Remove PARTITIONED BY clause
    sql = sql.replace(/PARTITIONED\s+BY\s*\([^)]+\)/gi, '');

    // Remove CLUSTERED BY clause
    if (sql.match(/CLUSTERED\s+BY/i)) {
      this.warnings.push("Snowflake doesn't support CLUSTER BY. Converting to clustering keys.");
      sql = sql.replace(/CLUSTERED\s+BY\s*\([^)]+\)\s+INTO\s+\d+\s+BUCKETS/gi, '');
    }

    return sql;
  }

  private convertFileFormats(sql: string): string {
    const formatMap: Record<string, string> = {
      'TEXTFILE': 'CSV',
      'SEQUENCEFILE': 'PARQUET',
      'RCFILE': 'PARQUET',
      'ORC': 'PARQUET',
      'PARQUET': 'PARQUET',
      'AVRO': 'PARQUET',
      'JSONFILE': 'JSON'
    };

    Object.entries(formatMap).forEach(([hiveFormat, snowFormat]) => {
      const regex = new RegExp(`STORED\\s+AS\\s+${hiveFormat}`, 'gi');
      if (sql.match(regex)) {
        this.warnings.push(`Converting ${hiveFormat} to ${snowFormat} format`);
        sql = sql.replace(regex, `FILE_FORMAT = ${snowFormat}`);
      }
    });

    // Remove ROW FORMAT clauses
    sql = sql.replace(/ROW\s+FORMAT\s+[^;]+/gi, '');
    
    // Remove LOCATION clauses
    sql = sql.replace(/LOCATION\s+\'[^\']+\'/gi, '');
    
    // Remove TBLPROPERTIES
    sql = sql.replace(/TBLPROPERTIES\s*\([^)]*\)/gi, '');

    return sql;
  }

  private convertBucketingAndSorting(sql: string): string {
    // Convert CLUSTERED BY to clustering keys
    const clusterMatch = sql.match(/CLUSTERED\s+BY\s*\(([^)]+)\)/i);
    if (clusterMatch) {
      const columns = clusterMatch[1].split(',').map(col => col.trim());
      this.warnings.push(`Converting CLUSTERED BY to Snowflake clustering keys`);
      sql = sql.replace(
        /CLUSTERED\s+BY\s*\([^)]+\)\s+INTO\s+\d+\s+BUCKETS/gi,
        `CLUSTER BY (${columns.join(', ')})`
      );
    }

    // Convert SORT BY to ORDER BY
    sql = sql.replace(/SORT\s+BY/gi, 'ORDER BY');

    return sql;
  }

  private convertSkewHandling(sql: string): string {
    // Remove skew hints and settings
    if (sql.match(/\/\*\+\s*SKEWJOIN/i)) {
      this.warnings.push("Removing SKEWJOIN hint as Snowflake handles data skew automatically");
      sql = sql.replace(/\/\*\+\s*SKEWJOIN\([^)]*\)\s*\*\//gi, '');
    }

    return sql;
  }

  private convertComplexTypes(sql: string): string {
    // Convert array types
    sql = sql.replace(/ARRAY\s*<([^>]+)>/gi, 'ARRAY');

    // Convert map types
    sql = sql.replace(/MAP\s*<([^,]+),\s*([^>]+)>/gi, 'OBJECT');

    // Convert struct types
    sql = sql.replace(/STRUCT\s*<([^>]+)>/gi, 'OBJECT');

    return sql;
  }

  private convertDistributeBy(sql: string): string {
    // Convert DISTRIBUTE BY to ORDER BY
    if (sql.match(/DISTRIBUTE\s+BY/i)) {
      this.warnings.push("Converting DISTRIBUTE BY to ORDER BY");
      sql = sql.replace(/DISTRIBUTE\s+BY/gi, 'ORDER BY');
    }
    return sql;
  }

  private convertRecursiveCTE(sql: string): string {
    // Handle recursive CTEs
    if (sql.match(/WITH\s+RECURSIVE/i)) {
      this.warnings.push("Snowflake supports recursive CTEs with similar syntax");
    }
    return sql;
  }

  private convertLateralView(sql: string): string {
    // Convert LATERAL VIEW EXPLODE to FLATTEN
    if (sql.match(/LATERAL\s+VIEW\s+EXPLODE/i)) {
      this.warnings.push("Converting LATERAL VIEW EXPLODE to Snowflake's FLATTEN");
      sql = sql.replace(
        /LATERAL\s+VIEW\s+(?:OUTER\s+)?EXPLODE\s*\(([^)]+)\)\s+(\w+)\s+AS\s+(\w+)/gi,
        'CROSS JOIN TABLE(FLATTEN(input => $1)) AS $2($3)'
      );
    }
    return sql;
  }

  private convertDataTypes(sql: string): string {
    const typeMap: Record<string, string> = {
      'STRING': 'VARCHAR',
      'INT': 'NUMBER(38,0)',
      'INTEGER': 'NUMBER(38,0)',
      'BIGINT': 'NUMBER(38,0)',
      'SMALLINT': 'NUMBER(5,0)',
      'TINYINT': 'NUMBER(3,0)',
      'DOUBLE': 'DOUBLE',
      'FLOAT': 'FLOAT',
      'BOOLEAN': 'BOOLEAN',
      'BINARY': 'BINARY',
      'TIMESTAMP': 'TIMESTAMP_NTZ',
      'DATE': 'DATE',
      'DECIMAL': 'NUMBER'
    };

    Object.entries(typeMap).forEach(([hiveType, snowType]) => {
      const regex = new RegExp(`\\b${hiveType}\\b`, 'gi');
      sql = sql.replace(regex, snowType);
    });

    return sql;
  }

  private convertInsertStatements(sql: string): string {
    // Convert INSERT INTO TABLE to INSERT INTO
    sql = sql.replace(/INSERT\s+INTO\s+TABLE\s+/gi, 'INSERT INTO ');

    // Convert INSERT OVERWRITE
    if (sql.match(/INSERT\s+OVERWRITE/i)) {
      this.warnings.push("Converting INSERT OVERWRITE to INSERT INTO");
      sql = sql.replace(/INSERT\s+OVERWRITE\s+(?:TABLE\s+)?(\w+)/gi, 'INSERT INTO $1');
    }

    // Remove PARTITION clause
    sql = sql.replace(/PARTITION\s*\([^)]+\)/gi, '');

    return sql;
  }

  private convertFunctions(sql: string): string {
    // Date/Time functions
    sql = sql.replace(
      /unix_timestamp\(([^)]*)\)/gi,
      (match, args) => args ? 
        `TO_NUMBER(${args}::timestamp_tz AT TIME ZONE 'UTC')` : 
        `TO_NUMBER(CURRENT_TIMESTAMP(3)::timestamp_tz AT TIME ZONE 'UTC')`
    );

    sql = sql.replace(
      /from_unixtime\(([^)]+)\)/gi,
      'TO_TIMESTAMP($1)'
    );

    // Collection functions
    sql = sql.replace(
      /collect_set\(([^)]+)\)/gi,
      'ARRAY_AGG(DISTINCT $1)'
    );

    sql = sql.replace(
      /collect_list\(([^)]+)\)/gi,
      'ARRAY_AGG($1)'
    );

    // JSON functions
    sql = sql.replace(
      /get_json_object\(([^,]+),\s*([^)]+)\)/gi,
      'GET_PATH(PARSE_JSON($1), $2)'
    );

    return sql;
  }

  private formatSQL(sql: string): string {
    const keywords = [
      'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 
      'HAVING', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
      'ON', 'AND', 'OR', 'IN', 'NOT', 'EXISTS', 'BETWEEN',
      'UNION', 'ALL', 'CREATE', 'TABLE', 'INSERT', 'INTO',
      'VALUES', 'UPDATE', 'DELETE', 'ALTER', 'DROP',
      'CROSS JOIN', 'FLATTEN', 'ARRAY_AGG', 'DISTINCT'
    ];

    let formatted = sql;
    
    // Capitalize keywords
    keywords.forEach(keyword => {
      formatted = formatted.replace(
        new RegExp(`\\b${keyword}\\b`, 'gi'),
        keyword
      );
    });

    // Add newlines and indentation
    formatted = formatted
      .replace(/\b(FROM|WHERE|GROUP BY|HAVING|ORDER BY|LIMIT)\b/g, '\n$1')
      .replace(/\b(LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN\b/g, '\n$&')
      .replace(/,\s*([\w\d_]+)/g, ',\n  $1');

    return formatted;
  }
}