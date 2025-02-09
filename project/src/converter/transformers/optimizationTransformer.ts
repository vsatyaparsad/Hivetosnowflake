export class OptimizationTransformer {
  transformDistributeBy(sql: string): string {
    // Convert DISTRIBUTE BY to ORDER BY
    return sql.replace(/DISTRIBUTE\s+BY\s+([^;\s]+)/gi, 'ORDER BY $1');
  }

  transformClusterBy(sql: string): string {
    // Convert CLUSTER BY to combination of PARTITION BY and ORDER BY
    return sql.replace(
      /CLUSTER\s+BY\s+([^;\s]+)/gi,
      'PARTITION BY $1 ORDER BY $1'
    );
  }

  transformSortBy(sql: string): string {
    // Convert SORT BY to ORDER BY
    return sql.replace(/SORT\s+BY\s+([^;\s]+)/gi, 'ORDER BY $1');
  }

  removeHints(sql: string): string {
    // Remove Hive-specific hints
    return sql
      .replace(/\/\*\+\s*MAPJOIN\([^)]*\)\s*\*\//gi, '')
      .replace(/\/\*\+\s*STREAMTABLE\([^)]*\)\s*\*\//gi, '')
      .replace(/\/\*\+\s*BROADCAST\([^)]*\)\s*\*\//gi, '');
  }

  transformTableSample(sql: string): string {
    // Convert TABLESAMPLE to Snowflake's SAMPLE
    return sql.replace(
      /TABLESAMPLE\s*\(\s*(\d+)\s*PERCENT\s*\)/gi,
      'SAMPLE ($1)'
    );
  }
}