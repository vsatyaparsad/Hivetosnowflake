export class HiveToSnowflakeConverter {
  private warnings: string[] = [];

  convert(hiveSQL: string): { success: boolean; sql?: string; warnings?: string[]; errors?: string[] } {
    try {
      // Clean and preprocess SQL
      let sql = this.preprocessSQL(hiveSQL);
      
      // Convert specific Hive constructs
      sql = this.convertCreateTable(sql);
      sql = this.convertInsertStatements(sql);
      sql = this.convertFunctions(sql);
      sql = this.convertLateralView(sql);
      sql = this.convertWindowFunctions(sql);
      sql = this.convertOrderBy(sql);
      sql = this.convertDataTypes(sql);
      
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
    // Remove SET statements
    sql = sql.replace(/^SET\s+[^;]+;/gm, '');
    
    // Remove Hive hints (MAPJOIN, SKEWJOIN, etc.)
    sql = sql.replace(/\/\*\+\s*(?:MAPJOIN|STREAMTABLE|BROADCAST|SKEWJOIN)\([^)]*\)\s*\*\//g, '');
    
    // Clean up whitespace
    return sql.replace(/\s+/g, ' ').trim();
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
      this.warnings.push("Snowflake doesn't support CLUSTER BY. Removing it.");
      sql = sql.replace(/CLUSTERED\s+BY\s*\([^)]+\)\s+INTO\s+\d+\s+BUCKETS/gi, '');
    }

    // Remove storage clauses
    sql = sql.replace(/STORED\s+AS\s+\w+/gi, '');
    sql = sql.replace(/ROW\s+FORMAT\s+[^;]+/gi, '');
    sql = sql.replace(/LOCATION\s+\'[^\']+\'/gi, '');
    sql = sql.replace(/TBLPROPERTIES\s*\([^)]*\)/gi, '');

    return sql;
  }

  private convertInsertStatements(sql: string): string {
    // Convert INSERT INTO TABLE to INSERT INTO
    sql = sql.replace(/INSERT\s+INTO\s+TABLE\s+/gi, 'INSERT INTO ');

    // Convert INSERT OVERWRITE
    if (sql.match(/INSERT\s+OVERWRITE/i)) {
      this.warnings.push("Snowflake doesn't support INSERT OVERWRITE. Converting to INSERT INTO.");
      sql = sql.replace(/INSERT\s+OVERWRITE\s+(?:TABLE\s+)?(\w+)/gi, 'INSERT INTO $1');
    }

    // Remove PARTITION clause
    sql = sql.replace(/PARTITION\s*\([^)]+\)/gi, '');

    return sql;
  }

  private convertFunctions(sql: string): string {
    // Convert DATEDIFF usage
    sql = sql.replace(
      /DATEDIFF\s*\(\s*SECOND\s*,\s*'1970-01-01'\s*,\s*([^)]+)\)/gi,
      'DATEDIFF(SECOND, $1, CURRENT_TIMESTAMP())'
    );

    // Convert date functions
    sql = sql.replace(
      /date_add\(([^,]+),\s*(\d+)\)/gi,
      'DATEADD(DAY, $2, $1)'
    );
    sql = sql.replace(
      /date_sub\(([^,]+),\s*(\d+)\)/gi,
      'DATEADD(DAY, -$2, $1)'
    );

    // Convert JSON functions
    sql = sql.replace(
      /json_tuple\(([^,]+),\s*([^)]+)\)/gi,
      (_, json, fields) => {
        const fieldList = fields.split(',').map(f => f.trim());
        return fieldList.map(field => `GET_PATH(PARSE_JSON(${json}), '${field}')`).join(', ');
      }
    );

    // Convert collection functions (ensure uppercase for Snowflake)
    sql = sql.replace(/array_agg\(/gi, 'ARRAY_AGG(');
    sql = sql.replace(/collect_set\(([^)]+)\)/gi, 'ARRAY_AGG(DISTINCT $1)');
    sql = sql.replace(/collect_list\(([^)]+)\)/gi, 'ARRAY_AGG($1)');

    return sql;
  }

  private convertLateralView(sql: string): string {
    // Convert LATERAL VIEW EXPLODE to FLATTEN
    if (sql.match(/LATERAL\s+VIEW\s+EXPLODE/i)) {
      this.warnings.push("Converting LATERAL VIEW EXPLODE to Snowflake's FLATTEN.");
      sql = sql.replace(
        /LATERAL\s+VIEW\s+(?:OUTER\s+)?EXPLODE\s*\(([^)]+)\)\s+(\w+)\s+AS\s+(\w+)/gi,
        'CROSS JOIN TABLE(FLATTEN(input => $1)) AS $2($3)'
      );
    }
    return sql;
  }

  private convertWindowFunctions(sql: string): string {
    // Convert RANGE BETWEEN to ROWS BETWEEN
    if (sql.match(/RANGE\s+BETWEEN/i)) {
      this.warnings.push("Converting RANGE BETWEEN to ROWS BETWEEN for window functions.");
      sql = sql.replace(
        /RANGE\s+BETWEEN\s+([^)]+)\s+AND\s+([^)]+)/gi,
        'ROWS BETWEEN $1 AND $2'
      );
    }
    return sql;
  }

  private convertOrderBy(sql: string): string {
    // Handle multiple ORDER BY clauses
    const orderByCount = (sql.match(/ORDER\s+BY/gi) || []).length;
    if (orderByCount > 1) {
      this.warnings.push("Multiple ORDER BY clauses found. Keeping only the last one.");
      const parts = sql.split(/ORDER\s+BY/i);
      sql = parts[0] + 'ORDER BY' + parts[parts.length - 1];
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
      'DATE': 'DATE'
    };

    let result = sql;
    for (const [hiveType, snowType] of Object.entries(typeMap)) {
      result = result.replace(
        new RegExp(`\\b${hiveType}\\b`, 'gi'),
        snowType
      );
    }

    // Convert complex types
    result = result.replace(/ARRAY\s*<[^>]+>/gi, 'ARRAY');
    result = result.replace(/MAP\s*<[^>]+>/gi, 'OBJECT');
    result = result.replace(/STRUCT\s*<[^>]+>/gi, 'OBJECT');

    return result;
  }

  private formatSQL(sql: string): string {
    const keywords = [
      'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 
      'HAVING', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
      'ON', 'AND', 'OR', 'IN', 'NOT', 'EXISTS', 'BETWEEN',
      'UNION', 'ALL', 'CREATE', 'TABLE', 'INSERT', 'INTO',
      'VALUES', 'UPDATE', 'DELETE', 'ALTER', 'DROP',
      'CROSS JOIN', 'FLATTEN', 'ARRAY_AGG', 'DISTINCT',
      'PARSE_JSON', 'GET_PATH', 'DATEDIFF', 'DATEADD'
    ];

    let formatted = sql.trim();
    
    // Capitalize keywords
    keywords.forEach(keyword => {
      formatted = formatted.replace(
        new RegExp(`\\b${keyword}\\b`, 'gi'),
        keyword
      );
    });

    // Add newlines and proper indentation
    formatted = formatted
      .replace(/\b(FROM|WHERE|GROUP BY|HAVING|ORDER BY|LIMIT)\b/gi, '\n$1')
      .replace(/\b(LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN\b/gi, '\n$&')
      .replace(/,\s*([\w\d_]+)/g, ',\n  $1');

    // Indent subqueries
    let depth = 0;
    formatted = formatted
      .split('\n')
      .map(line => {
        const openCount = (line.match(/\(/g) || []).length;
        const closeCount = (line.match(/\)/g) || []).length;
        const indent = '  '.repeat(depth);
        depth += openCount - closeCount;
        return indent + line.trim();
      })
      .join('\n');

    return formatted;
  }
}