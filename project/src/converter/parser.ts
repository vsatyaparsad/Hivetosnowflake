import { Parser } from 'node-sql-parser';
import { format } from 'sql-formatter';
import { DDLTransformer } from './transformers/ddlTransformer';
import { DMLTransformer } from './transformers/dmlTransformer';
import { FunctionTransformer } from './transformers/functionTransformer';
import { OptimizationTransformer } from './transformers/optimizationTransformer';
import { HIVE_PATTERNS } from './hivePatterns';
import { ConversionOptions, ConversionResult } from '../types/sql';

export class SQLParser {
  private sql: string;
  private parser: Parser;
  private ddlTransformer: DDLTransformer;
  private dmlTransformer: DMLTransformer;
  private functionTransformer: FunctionTransformer;
  private optimizationTransformer: OptimizationTransformer;
  private options: ConversionOptions;
  private warnings: string[] = [];
  private errors: string[] = [];
  
  constructor(sql: string, options: ConversionOptions = {}) {
    this.sql = sql.trim();
    this.parser = new Parser();
    this.ddlTransformer = new DDLTransformer();
    this.dmlTransformer = new DMLTransformer();
    this.functionTransformer = new FunctionTransformer();
    this.optimizationTransformer = new OptimizationTransformer();
    this.options = {
      preserveComments: true,
      handleErrors: 'warn',
      validateSyntax: true,
      ...options
    };
  }

  public convert(): ConversionResult {
    try {
      // Preprocess SQL
      this.sql = this.preprocessSQL(this.sql);

      // Split into individual statements
      const statements = this.splitStatements(this.sql);
      
      // Convert each statement
      const convertedStatements = statements.map(stmt => {
        try {
          return this.convertStatement(stmt);
        } catch (error) {
          const errorMsg = error instanceof Error ? error.message : String(error);
          if (this.options.handleErrors === 'throw') {
            throw error;
          }
          this.warnings.push(`Warning: Using fallback conversion for statement: ${errorMsg}`);
          return this.fallbackConversion(stmt);
        }
      });

      // Join statements and format
      const convertedSQL = this.formatSQL(convertedStatements.join('\n\n'));

      return {
        success: true,
        sql: convertedSQL,
        warnings: this.warnings,
        errors: this.errors,
        metadata: this.generateMetadata(convertedSQL)
      };
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.errors.push(errorMsg);
      
      return {
        success: false,
        errors: this.errors,
        warnings: this.warnings
      };
    }
  }

  private preprocessSQL(sql: string): string {
    // Remove Hive SET statements
    sql = sql.replace(/^SET\s+[^;]+;/gm, '');
    
    // Remove inline Hive hints
    sql = sql.replace(/\/\*\+[^*]*\*\//g, '');
    
    // Convert Hive-specific comments to standard SQL comments
    sql = sql.replace(/^--\s*#####/gm, '-- -----');
    
    // Handle ARRAY<TYPE> syntax
    sql = sql.replace(/ARRAY\s*<\s*([^>]+)\s*>/gi, 'ARRAY');
    
    // Handle MAP<TYPE, TYPE> syntax
    sql = sql.replace(/MAP\s*<\s*([^>]+)\s*,\s*([^>]+)\s*>/gi, 'OBJECT');
    
    // Handle STRUCT<...> syntax
    sql = sql.replace(/STRUCT\s*<[^>]+>/gi, 'OBJECT');
    
    // Clean up whitespace
    sql = sql.replace(/\s+/g, ' ').trim();
    
    return sql;
  }

  private splitStatements(sql: string): string[] {
    const statements: string[] = [];
    let currentStatement = '';
    let inString = false;
    let stringChar = '';
    let inComment = false;
    let depth = 0;

    for (let i = 0; i < sql.length; i++) {
      const char = sql[i];
      const nextChar = sql[i + 1] || '';

      // Handle string literals
      if ((char === "'" || char === '"') && !inComment) {
        if (!inString) {
          inString = true;
          stringChar = char;
        } else if (char === stringChar) {
          inString = false;
        }
      }

      // Handle comments
      if (!inString) {
        if (char === '-' && nextChar === '-') {
          inComment = true;
        } else if (char === '\n' && inComment) {
          inComment = false;
        }
      }

      // Handle parentheses
      if (!inString && !inComment) {
        if (char === '(') depth++;
        if (char === ')') depth--;
      }

      // Handle statement separation
      if (char === ';' && !inString && !inComment && depth === 0) {
        statements.push(currentStatement.trim());
        currentStatement = '';
        continue;
      }

      currentStatement += char;
    }

    if (currentStatement.trim()) {
      statements.push(currentStatement.trim());
    }

    return statements.filter(stmt => stmt.length > 0);
  }

  private convertStatement(sql: string): string {
    // Handle different statement types
    if (HIVE_PATTERNS.CREATE_TABLE.test(sql)) {
      return this.ddlTransformer.transformCreateTable(sql);
    }
    
    if (HIVE_PATTERNS.ALTER_TABLE.test(sql)) {
      return this.ddlTransformer.transformAlterTable(sql);
    }
    
    if (HIVE_PATTERNS.INSERT_OVERWRITE.test(sql) || HIVE_PATTERNS.INSERT_INTO.test(sql)) {
      return this.dmlTransformer.transformInsert(sql);
    }

    // Parse and transform the SQL
    const ast = this.parser.parse(sql);
    this.transformAst(ast);
    let convertedSql = this.parser.astToSQL(ast);

    // Apply additional transformations
    convertedSql = this.optimizationTransformer.removeHints(convertedSql);
    convertedSql = this.optimizationTransformer.transformDistributeBy(convertedSql);
    convertedSql = this.optimizationTransformer.transformClusterBy(convertedSql);
    convertedSql = this.optimizationTransformer.transformSortBy(convertedSql);

    return convertedSql;
  }

  private fallbackConversion(sql: string): string {
    // Handle specific Hive constructs that might cause parsing issues
    
    // Convert DISTRIBUTE BY to ORDER BY
    sql = sql.replace(/DISTRIBUTE\s+BY/gi, 'ORDER BY');

    // Convert LATERAL VIEW EXPLODE
    sql = sql.replace(
      /LATERAL\s+VIEW\s+EXPLODE\s*\(([^)]+)\)\s+([a-zA-Z0-9_]+)\s+AS\s+([a-zA-Z0-9_]+)/gi,
      'CROSS JOIN TABLE(FLATTEN(input => $1)) AS $2($3)'
    );

    // Convert COLLECT_SET
    sql = sql.replace(
      /collect_set\(([^)]+)\)/gi,
      'array_agg(DISTINCT $1)'
    );

    // Convert unix_timestamp
    sql = sql.replace(
      /unix_timestamp\(([^)]*)\)/gi,
      (match, args) => args ? `date_part(epoch_seconds, ${args})` : 'date_part(epoch_seconds, current_timestamp())'
    );

    // Convert date functions
    sql = sql.replace(
      /date_add\(([^,]+),\s*(\d+)\)/gi,
      'dateadd(day, $2, $1)'
    );

    sql = sql.replace(
      /date_sub\(([^,]+),\s*(\d+)\)/gi,
      'dateadd(day, -$2, $1)'
    );

    return this.formatSQL(sql);
  }

  private transformAst(ast: any): void {
    if (Array.isArray(ast)) {
      ast.forEach(node => this.transformAst(node));
      return;
    }

    if (!ast || typeof ast !== 'object') return;

    // Transform functions
    if (ast.type === 'function') {
      const transformed = this.functionTransformer.transformFunction(ast.name, ast.args);
      ast.raw = transformed;
    }

    // Recursively transform child nodes
    Object.values(ast).forEach(value => {
      if (value && typeof value === 'object') {
        this.transformAst(value);
      }
    });
  }

  private generateMetadata(sql: string): any {
    return {
      tablesReferenced: this.extractTablesReferenced(sql),
      functionsUsed: this.extractFunctionsUsed(sql),
      dataTypesUsed: this.extractDataTypesUsed(sql),
      unsupportedFeatures: this.warnings.map(w => w.replace(/^Warning: /, ''))
    };
  }

  private extractTablesReferenced(sql: string): string[] {
    const tableMatches = sql.match(/(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+([a-zA-Z_][a-zA-Z0-9_]*)/gi) || [];
    return [...new Set(tableMatches.map(m => m.split(/\s+/).pop()!))];
  }

  private extractFunctionsUsed(sql: string): string[] {
    const functionMatches = sql.match(/[a-zA-Z_][a-zA-Z0-9_]*\s*\(/g) || [];
    return [...new Set(functionMatches.map(m => m.replace(/[\s(]/g, '')))];
  }

  private extractDataTypesUsed(sql: string): string[] {
    const typeMatches = sql.match(/\b(?:VARCHAR|NUMBER|TIMESTAMP|DATE|BOOLEAN|ARRAY|OBJECT)\b/gi) || [];
    return [...new Set(typeMatches)];
  }

  private formatSQL(sql: string): string {
    return format(sql, {
      language: 'snowflake',
      keywordCase: 'upper',
      indentStyle: 'standard',
      linesBetweenQueries: 2
    });
  }
}