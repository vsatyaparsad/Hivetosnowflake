import logging
from pathlib import Path
from typing import Dict, List, Tuple
import re
import sqlparse
from dataclasses import dataclass
import pandas as pd

@dataclass
class ValidationResult:
    is_valid: bool
    issues: List[str]
    line_numbers: List[int]
    suggestions: List[str]

class SnowflakeValidator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Hive to Snowflake syntax mappings
        self.hive_patterns = {
            # Data Types
            r'\bSTRING\b': 'VARCHAR',
            r'\bBINARY\b': 'BINARY',
            r'\bTINYINT\b': 'SMALLINT',
            r'\bINT\b': 'INTEGER',
            r'\bBIGINT\b': 'BIGINT',
            r'\bDOUBLE\b': 'DOUBLE',
            r'\bFLOAT\b': 'FLOAT',
            r'\bBOOLEAN\b': 'BOOLEAN',
            
            # Functions
            r'\bCONCAT_WS\s*\(': 'LISTAGG(',
            r'\bCOLLECT_LIST\s*\(': 'ARRAY_AGG(',
            r'\bNVL\s*\(': 'COALESCE(',
            r'\bGET_JSON_OBJECT\s*\(': 'GET_PATH(',
            r'\bPARSE_URL\s*\(': 'PARSE_URL(',
            r'\bUNIX_TIMESTAMP\s*\(': 'UNIX_TIMESTAMP(',
            
            # Syntax
            r'DISTRIBUTE\s+BY': 'CLUSTER BY',
            r'SORT\s+BY': 'ORDER BY',
            r'LATERAL\s+VIEW\s+EXPLODE': 'FLATTEN',
            r'ROW\s+FORMAT\s+DELIMITED': '',
            r'STORED\s+AS\s+(ORC|PARQUET|AVRO)': '',
        }
        
        # Snowflake-specific keywords that should be present
        self.snowflake_keywords = [
            'WAREHOUSE',
            'COPY INTO',
            'CLONE',
            'MERGE INTO',
            'QUALIFY',
            'SAMPLE',
            'MATCH_RECOGNIZE'
        ]
        
        # Hive-specific patterns that should not be present
        self.hive_specific_patterns = [
            r'ADD\s+JAR',
            r'LOAD\s+DATA\s+INPATH',
            r'INPUTFORMAT',
            r'OUTPUTFORMAT',
            r'SERDE',
            r'PARTITIONED\s+BY',
            r'CLUSTERED\s+BY',
            r'BUCKETS',
            r'SKEWED\s+BY',
            r'STORED\s+AS\s+DIRECTORIES',
            r'LOCATION\s+\'hdfs://'
        ]

    def validate_conversion(self, sql_content: str) -> ValidationResult:
        """Validate if SQL has been properly converted to Snowflake format"""
        issues = []
        line_numbers = []
        suggestions = []
        
        # Parse SQL
        statements = sqlparse.parse(sql_content)
        
        for statement in statements:
            # Check for Hive-specific patterns
            for pattern in self.hive_specific_patterns:
                matches = re.finditer(pattern, str(statement), re.IGNORECASE)
                for match in matches:
                    line_no = sql_content[:match.start()].count('\n') + 1
                    issues.append(f"Found Hive-specific pattern '{pattern}' at line {line_no}")
                    line_numbers.append(line_no)
                    suggestions.append(f"Remove or replace Hive-specific syntax '{pattern}'")
            
            # Check for unconverted Hive patterns
            for hive_pattern, snowflake_equiv in self.hive_patterns.items():
                matches = re.finditer(hive_pattern, str(statement), re.IGNORECASE)
                for match in matches:
                    line_no = sql_content[:match.start()].count('\n') + 1
                    issues.append(f"Found Hive pattern '{match.group()}' at line {line_no}")
                    line_numbers.append(line_no)
                    suggestions.append(f"Replace with Snowflake equivalent '{snowflake_equiv}'")
            
            # Check for specific syntax issues
            self._check_syntax_issues(statement, issues, line_numbers, suggestions)
        
        return ValidationResult(
            is_valid=len(issues) == 0,
            issues=issues,
            line_numbers=line_numbers,
            suggestions=suggestions
        )

    def _check_syntax_issues(self, statement: sqlparse.sql.Statement, 
                           issues: List[str], line_numbers: List[int], 
                           suggestions: List[str]):
        """Check for specific syntax issues"""
        sql_str = str(statement)
        
        # Check JOIN syntax
        if re.search(r'JOIN.*?(?:ON|USING)', sql_str, re.IGNORECASE):
            if not re.search(r'(INNER|LEFT|RIGHT|FULL)\s+JOIN', sql_str, re.IGNORECASE):
                issues.append("JOIN type not explicitly specified")
                suggestions.append("Specify JOIN type (INNER, LEFT, RIGHT, FULL)")
        
        # Check for proper table aliases
        if re.search(r'FROM\s+\w+\s+\w+\s+(?!AS\b)', sql_str, re.IGNORECASE):
            issues.append("Table alias missing AS keyword")
            suggestions.append("Add AS keyword before table aliases")
        
        # Check for proper date/timestamp literals
        if re.search(r"'[\d-]+'::timestamp", sql_str, re.IGNORECASE):
            issues.append("Found Hive-style timestamp casting")
            suggestions.append("Use Snowflake TIMESTAMP_* functions")
        
        # Check for proper NULL handling
        if re.search(r'(\w+)\s*=\s*NULL', sql_str, re.IGNORECASE):
            issues.append("Found incorrect NULL comparison")
            suggestions.append("Use IS NULL instead of = NULL")

def analyze_statement_types(sql_content: str) -> Dict[str, int]:
    """Analyze types of SQL statements in a file"""
    statements = sqlparse.parse(sql_content)
    statement_types = {
        'CREATE': 0,
        'DROP': 0,
        'DELETE': 0,
        'INSERT': 0,
        'SELECT': 0,
        'UPDATE': 0,
        'MERGE': 0,
        'ALTER': 0,
        'TRUNCATE': 0,
        'OTHER': 0
    }
    
    for stmt in statements:
        first_token = stmt.token_first()
        if first_token:
            token_type = first_token.value.upper()
            if token_type.startswith('CREATE'):
                statement_types['CREATE'] += 1
            elif token_type.startswith('DROP'):
                statement_types['DROP'] += 1
            elif token_type.startswith('DELETE'):
                statement_types['DELETE'] += 1
            elif token_type.startswith('INSERT'):
                statement_types['INSERT'] += 1
            elif token_type.startswith('SELECT'):
                statement_types['SELECT'] += 1
            elif token_type.startswith('UPDATE'):
                statement_types['UPDATE'] += 1
            elif token_type.startswith('MERGE'):
                statement_types['MERGE'] += 1
            elif token_type.startswith('ALTER'):
                statement_types['ALTER'] += 1
            elif token_type.startswith('TRUNCATE'):
                statement_types['TRUNCATE'] += 1
            else:
                statement_types['OTHER'] += 1
    
    return statement_types

def analyze_files(hive_dir: Path, snowflake_dir: Path) -> Dict:
    """Analyze SQL files in both directories"""
    analysis = {
        'hive_files': [],
        'snowflake_files': [],
        'missing_conversions': [],
        'extra_snowflake_files': [],
        'stats': {
            'total_hive': 0,
            'total_snowflake': 0,
            'converted': 0,
            'not_converted': 0,
            'extra_files': 0
        },
        'statement_analysis': {
            'hive': {
                'CREATE': 0,
                'DROP': 0,
                'DELETE': 0,
                'INSERT': 0,
                'SELECT': 0,
                'UPDATE': 0,
                'MERGE': 0,
                'ALTER': 0,
                'TRUNCATE': 0,
                'OTHER': 0
            },
            'snowflake': {
                'CREATE': 0,
                'DROP': 0,
                'DELETE': 0,
                'INSERT': 0,
                'SELECT': 0,
                'UPDATE': 0,
                'MERGE': 0,
                'ALTER': 0,
                'TRUNCATE': 0,
                'OTHER': 0
            }
        }
    }
    
    # Get all files
    hive_files = list(hive_dir.glob('*.hql'))
    snowflake_files = list(snowflake_dir.glob('*.sql'))
    
    # Analyze Hive files
    for file in hive_files:
        try:
            content = file.read_text(encoding='utf-8')
            stmt_types = analyze_statement_types(content)
            for stmt_type, count in stmt_types.items():
                analysis['statement_analysis']['hive'][stmt_type] += count
        except Exception as e:
            logging.warning(f"Error analyzing Hive file {file}: {e}")
    
    # Analyze Snowflake files
    for file in snowflake_files:
        try:
            content = file.read_text(encoding='utf-8')
            stmt_types = analyze_statement_types(content)
            for stmt_type, count in stmt_types.items():
                analysis['statement_analysis']['snowflake'][stmt_type] += count
        except Exception as e:
            logging.warning(f"Error analyzing Snowflake file {file}: {e}")
    
    # Track all files
    analysis['hive_files'] = [f.name for f in hive_files]
    analysis['snowflake_files'] = [f.name for f in snowflake_files]
    
    # Find missing conversions
    for hive_file in hive_files:
        expected_snowflake = snowflake_dir / (hive_file.stem + '.sql')
        if not expected_snowflake.exists():
            analysis['missing_conversions'].append(hive_file.name)
    
    # Find extra Snowflake files
    hive_stems = {f.stem for f in hive_files}
    for snowflake_file in snowflake_files:
        if snowflake_file.stem not in hive_stems:
            analysis['extra_snowflake_files'].append(snowflake_file.name)
    
    # Calculate statistics
    analysis['stats']['total_hive'] = len(hive_files)
    analysis['stats']['total_snowflake'] = len(snowflake_files)
    analysis['stats']['converted'] = len(hive_files) - len(analysis['missing_conversions'])
    analysis['stats']['not_converted'] = len(analysis['missing_conversions'])
    analysis['stats']['extra_files'] = len(analysis['extra_snowflake_files'])
    
    return analysis

def print_analysis(analysis: Dict):
    """Print analysis results"""
    print("\nFile Analysis Summary:")
    print("=" * 80)
    
    # Print statistics
    stats = analysis['stats']
    print(f"\nStatistics:")
    print(f"Total Hive SQL Files (.hql):     {stats['total_hive']}")
    print(f"Total Snowflake SQL Files (.sql): {stats['total_snowflake']}")
    print(f"Successfully Converted:           {stats['converted']}")
    print(f"Not Yet Converted:               {stats['not_converted']}")
    print(f"Extra Snowflake Files:           {stats['extra_files']}")
    
    # Print statement type analysis
    print("\nStatement Type Analysis:")
    print("-" * 40)
    print(f"{'Statement Type':<15} {'Hive':<10} {'Snowflake':<10}")
    print("-" * 40)
    
    hive_stats = analysis['statement_analysis']['hive']
    sf_stats = analysis['statement_analysis']['snowflake']
    
    for stmt_type in hive_stats.keys():
        print(f"{stmt_type:<15} {hive_stats[stmt_type]:<10} {sf_stats[stmt_type]:<10}")
    
    # Calculate DDL statements
    hive_ddl = hive_stats['CREATE'] + hive_stats['DROP'] + hive_stats['ALTER']
    sf_ddl = sf_stats['CREATE'] + sf_stats['DROP'] + sf_stats['ALTER']
    
    print("\nDDL Statement Summary:")
    print(f"Hive DDL Statements:     {hive_ddl}")
    print(f"Snowflake DDL Statements: {sf_ddl}")
    
    # Print file lists
    if analysis['missing_conversions']:
        print("\nHive Files Not Yet Converted:")
        for file in analysis['missing_conversions']:
            print(f"  • {file}")
    
    if analysis['extra_snowflake_files']:
        print("\nExtra Snowflake Files (no matching Hive file):")
        for file in analysis['extra_snowflake_files']:
            print(f"  • {file}")
    
    print("\nConversion Progress:")
    if stats['total_hive'] > 0:
        progress = (stats['converted'] / stats['total_hive']) * 100
        print(f"[{'=' * int(progress/2)}{' ' * (50-int(progress/2))}] {progress:.1f}%")
    print("=" * 80)

def analyze_statement_details(sql_content: str) -> Dict[str, List[str]]:
    """Analyze detailed statement information"""
    statements = sqlparse.parse(sql_content)
    details = {
        'CREATE': [],
        'DROP': [],
        'DELETE': [],
        'CREATE_TABLE': [],
        'CREATE_VIEW': [],
        'DROP_TABLE': [],
        'DROP_VIEW': [],
        'ALTER_TABLE': [],
        'TRUNCATE_TABLE': []
    }
    
    for stmt in statements:
        sql = str(stmt).strip()
        
        # Analyze CREATE statements
        if sql.upper().startswith('CREATE'):
            details['CREATE'].append(sql)
            if 'CREATE TABLE' in sql.upper():
                details['CREATE_TABLE'].append(sql)
            elif 'CREATE VIEW' in sql.upper():
                details['CREATE_VIEW'].append(sql)
        
        # Analyze DROP statements
        elif sql.upper().startswith('DROP'):
            details['DROP'].append(sql)
            if 'DROP TABLE' in sql.upper():
                details['DROP_TABLE'].append(sql)
            elif 'DROP VIEW' in sql.upper():
                details['DROP_VIEW'].append(sql)
        
        # Analyze DELETE statements
        elif sql.upper().startswith('DELETE'):
            details['DELETE'].append(sql)
        
        # Analyze ALTER TABLE statements
        elif sql.upper().startswith('ALTER TABLE'):
            details['ALTER_TABLE'].append(sql)
        
        # Analyze TRUNCATE statements
        elif sql.upper().startswith('TRUNCATE'):
            details['TRUNCATE_TABLE'].append(sql)
    
    return details

def create_excel_report(analysis: Dict, output_file: str = "sql_analysis_report.xlsx"):
    """Create detailed Excel report of SQL analysis"""
    try:
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Create formats
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#D3D3D3',
                'border': 1
            })
            
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Total Hive Files',
                    'Total Snowflake Files',
                    'Converted Files',
                    'Not Converted',
                    'Extra Files'
                ],
                'Count': [
                    analysis['stats']['total_hive'],
                    analysis['stats']['total_snowflake'],
                    analysis['stats']['converted'],
                    analysis['stats']['not_converted'],
                    analysis['stats']['extra_files']
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            # Statement Analysis sheet
            stmt_data = []
            hive_stats = analysis['statement_analysis']['hive']
            sf_stats = analysis['statement_analysis']['snowflake']
            
            for stmt_type in hive_stats.keys():
                stmt_data.append({
                    'Statement Type': stmt_type,
                    'Hive Count': hive_stats[stmt_type],
                    'Snowflake Count': sf_stats[stmt_type],
                    'Difference': sf_stats[stmt_type] - hive_stats[stmt_type]
                })
            
            pd.DataFrame(stmt_data).to_excel(writer, sheet_name='Statement Analysis', index=False)
            
            # DDL Analysis sheet
            hive_details = {}
            snowflake_details = {}
            
            for file in analysis.get('hive_files', []):
                try:
                    content = Path("hive_queries") / file
                    details = analyze_statement_details(content.read_text(encoding='utf-8'))
                    hive_details[file] = details
                except Exception as e:
                    logging.warning(f"Error analyzing Hive file {file}: {e}")
            
            for file in analysis.get('snowflake_files', []):
                try:
                    content = Path("snowflake_queries") / file
                    details = analyze_statement_details(content.read_text(encoding='utf-8'))
                    snowflake_details[file] = details
                except Exception as e:
                    logging.warning(f"Error analyzing Snowflake file {file}: {e}")
            
            # Create DDL sheet
            ddl_data = []
            for file, details in hive_details.items():
                sf_file = file.replace('.hql', '.sql')
                sf_details = snowflake_details.get(sf_file, {})
                
                ddl_data.append({
                    'File Name': file,
                    'Hive CREATE TABLE': len(details['CREATE_TABLE']),
                    'Snowflake CREATE TABLE': len(sf_details.get('CREATE_TABLE', [])),
                    'Hive DROP': len(details['DROP']),
                    'Snowflake DROP': len(sf_details.get('DROP', [])),
                    'Hive DELETE': len(details['DELETE']),
                    'Snowflake DELETE': len(sf_details.get('DELETE', []))
                })
            
            pd.DataFrame(ddl_data).to_excel(writer, sheet_name='DDL Analysis', index=False)
            
            # Format all sheets
            for sheet in writer.sheets.values():
                for col, value in enumerate(pd.DataFrame(summary_data).columns.values):
                    sheet.write(0, col, value, header_format)
                sheet.set_column('A:Z', 20)  # Set column width
            
        return True
    except Exception as e:
        logging.error(f"Error creating Excel report: {e}")
        return False

def main():
    """Process and validate SQL files"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize validator
        validator = SnowflakeValidator()
        
        # Get SQL files
        hive_dir = Path("hive_queries")
        snowflake_dir = Path("snowflake_queries")
        
        # Analyze files first
        analysis = analyze_files(hive_dir, snowflake_dir)
        print_analysis(analysis)
        
        if not analysis['stats']['total_hive'] or not analysis['stats']['total_snowflake']:
            logger.error("No SQL files found")
            return 1
        
        # Process each pair of files
        validation_results = []
        for hive_file in hive_dir.glob('*.hql'):
            snowflake_file = snowflake_dir / (hive_file.stem + '.sql')
            if not snowflake_file.exists():
                continue
            
            logger.info(f"Validating conversion: {hive_file.name} -> {snowflake_file.name}")
            
            # Read files
            snowflake_sql = snowflake_file.read_text(encoding='utf-8')
            
            # Validate conversion
            result = validator.validate_conversion(snowflake_sql)
            validation_results.append((snowflake_file.name, result))
        
        # Print validation results
        print("\nValidation Results:")
        print("=" * 80)
        for file_name, result in validation_results:
            print(f"\nFile: {file_name}")
            print("-" * 80)
            if result.is_valid:
                print("✅ Valid Snowflake SQL - No issues found")
            else:
                print("⚠️ Issues Found:")
                for i, (issue, line_no, suggestion) in enumerate(
                    zip(result.issues, result.line_numbers, result.suggestions), 1):
                    print(f"\n{i}. Issue at line {line_no}:")
                    print(f"   {issue}")
                    print(f"   Suggestion: {suggestion}")
            print("-" * 80)
        
        # Print final summary
        total_valid = sum(1 for _, r in validation_results if r.is_valid)
        total_files = len(validation_results)
        print("\nFinal Summary:")
        print(f"Total Files Validated: {total_files}")
        print(f"Valid Conversions: {total_valid}")
        print(f"Files with Issues: {total_files - total_valid}")
        
        # After analysis
        print("\nGenerating Excel report...")
        if create_excel_report(analysis, "sql_analysis_report.xlsx"):
            print("Excel report generated successfully: sql_analysis_report.xlsx")
        else:
            print("Failed to generate Excel report. See log for details.")
    
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    main() 
