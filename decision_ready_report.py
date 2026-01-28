"""
AWS Lambda entry point for Decision-Ready Restaurant Reports
Processes CSV files and ZIP files with multiple CSVs, returns Excel files with AI-powered insights
"""
import json
import logging
import base64
import boto3
import zipfile
from io import BytesIO, StringIO
from datetime import datetime
from src.processors.csv_processor import CSVProcessor
from src.processors.zip_processor import ZipProcessor
from src.models.file_info import FileInfo, FileType
from src.utils.metadata_extractor import extract_csvs_metadata

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')


def lambda_handler(event, context):
    """
    AWS Lambda handler function for CSV to Excel processing
    
    Args:
        event: AWS Lambda event object (can contain CSV data or S3 reference)
        context: AWS Lambda context object
        
    Returns:
        dict: Response object with statusCode and Excel file (base64 encoded)
    """
    try:
        logger.info(f"Processing event: {json.dumps(event, default=str)}")
        
        file_data = None
        file_name = "restaurant_report"
        file_type = "csv"  # default to CSV
        
        # Handle different event sources
        if 'Records' in event:
            # S3 event - file uploaded to S3
            file_data, file_name, file_type = _handle_s3_event(event)
        elif 'body' in event:
            # API Gateway event - CSV or ZIP data in request body
            file_data, file_name, file_type = _handle_api_gateway_event(event)
        else:
            # Direct invocation
            file_data, file_name, file_type = _handle_direct_invocation(event)
        
        if not file_data:
            raise ValueError("No file data found in the event")
        
        # Process based on file type
        if file_type == "zip":
            logger.info(f"Processing ZIP file: {file_name}")
            excel_data, insights = _process_zip_file(file_data, file_name)
        else:
            logger.info(f"Processing CSV data for file: {file_name}")
            excel_data, insights = _process_csv_file(file_data, file_name)
        
        # Log file size before encoding
        logger.info(f"Excel file size: {len(excel_data)} bytes ({len(excel_data)/1024/1024:.2f} MB)")
        
        # Encode Excel file as base64 for response
        # Always prefer S3 links (new approach - no Excel files)
        if 'shareable_links' in insights:
            s3_links = insights['shareable_links']
            logger.info("✅ Returning S3 links (markdown + HTML dashboard)")
            
            response = {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(to_serializable({
                    'message': 'Restaurant report generated successfully',
                    'file_name': f"{file_name}_report",
                    'delivery_method': 'S3_dashboard',
                    'reports': {
                        'business_analysis': s3_links.get('markdown_report'),
                        'interactive_dashboard': s3_links.get('html_dashboard'),
                        'data_sources': s3_links.get('csv_sources', {}),
                        'metadata': s3_links.get('metadata')
                    },
                    'ai_summary': insights.get('ai_insights', {}).get('summary', 'Analysis completed'),
                    'request_id': insights.get('s3_upload', {}).get('request_id'),
                    'instructions': {
                        'business_analysis': 'Open markdown URL for detailed business insights',
                        'interactive_dashboard': 'Open HTML URL for charts and visualizations',
                        'data_sources': 'CSV files used in chart generation'
                    }
                }))
            }
            
            logger.info(f"Successfully processed with S3 dashboard delivery: {file_name}")
            return response
        
        # Fallback: If S3 failed, return basic insights
        else:
            logger.warning("S3 upload failed, returning basic insights")
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(to_serializable({
                    'message': 'Report processed (S3 upload failed)',
                    'file_name': file_name,
                    'delivery_method': 'basic_insights',
                    'ai_insights': insights.get('ai_insights'),
                    'aggregation_results': insights.get('aggregation_results', {}),
                    'warning': 'S3 upload failed - limited response format'
                }))
            }
        
        # This fallback should not be reached with S3 integration
        logger.error("Unexpected fallback - should always have S3 or error above")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Internal processing error',
                'message': 'Neither S3 upload nor fallback handling worked'
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing CSV file: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Internal server error',
                'message': str(e)
            })
        }


def _handle_s3_event(event):
    """Handle S3 event - download file from S3"""
    try:
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        
        logger.info(f"Processing S3 object: s3://{bucket_name}/{object_key}")
        
        # Download file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_data = response['Body'].read()
        
        # Extract filename and determine type
        file_name = object_key.split('/')[-1].rsplit('.', 1)[0]
        file_extension = object_key.split('/')[-1].rsplit('.', 1)[-1].lower()
        
        if file_extension == 'zip':
            file_type = "zip"
        elif file_extension == 'csv':
            file_type = "csv"
            file_data = file_data.decode('utf-8')  # Decode CSV to string
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")
        
        return file_data, file_name, file_type
    except Exception as e:
        logger.error(f"Error handling S3 event: {str(e)}")
        raise


def _handle_api_gateway_event(event):
    """Handle API Gateway event - CSV or ZIP data in request body"""
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        
        file_name = body.get('file_name', 'api_upload')
        
        # Check for CSV data
        if 'csv_data' in body:
            file_data = body['csv_data']
            file_type = "csv"
        elif 'csv_base64' in body:
            # Handle base64 encoded CSV
            file_data = base64.b64decode(body['csv_base64']).decode('utf-8')
            file_type = "csv"
        # Check for ZIP data
        elif 'zip_data' in body:
            file_data = body['zip_data']
            file_type = "zip"
            if isinstance(file_data, str):
                file_data = file_data.encode('latin-1')  # Convert string to bytes
        elif 'zip_base64' in body:
            # Handle base64 encoded ZIP
            file_data = base64.b64decode(body['zip_base64'])
            file_type = "zip"
        else:
            raise ValueError("No file data found in request body")
        
        return file_data, file_name, file_type
    except Exception as e:
        logger.error(f"Error handling API Gateway event: {str(e)}")
        raise


def _handle_direct_invocation(event):
    """Handle direct Lambda invocation"""
    try:
        file_name = event.get('file_name', 'direct_invocation')
        
        # Check for CSV data
        if 'csv_data' in event:
            file_data = event['csv_data']
            file_type = "csv"
        elif 'csv_base64' in event:
            file_data = base64.b64decode(event['csv_base64']).decode('utf-8')
            file_type = "csv"
        # Check for ZIP data
        elif 'zip_data' in event:
            file_data = event['zip_data']
            file_type = "zip"
            if isinstance(file_data, str):
                file_data = file_data.encode('latin-1')  # Convert string to bytes
        elif 'zip_base64' in event:
            file_data = base64.b64decode(event['zip_base64'])
            file_type = "zip"
        else:
            raise ValueError("No file data found in direct invocation")
        
        return file_data, file_name, file_type
    except Exception as e:
        logger.error(f"Error handling direct invocation: {str(e)}")
        raise


def _process_csv_file(csv_data, file_name):
    """Process single CSV file using new AI-powered processor"""
    try:
        # Create file info
        file_info = FileInfo(
            name=file_name,
            original_name=file_name,
            file_type=FileType.CSV,
            size_bytes=len(csv_data.encode('utf-8')),
            uploaded_at=datetime.now()
        )
        
        # Process with CSVProcessor (with AI enabled)
        csv_processor = CSVProcessor(enable_ai_analysis=True)
        result = csv_processor.process_csv_string(csv_data, file_info)
        
        if not result.success:
            raise Exception(f"CSV processing failed: {', '.join(result.errors)}")
        
        # Generate Excel from the report
        if result.reports:
            report = result.reports[0]
            excel_data = _generate_excel_from_dataframe(report.dataframe, file_name)
        else:
            raise Exception("No valid data found in CSV")
        
        # Extract insights
        insights = {
            'file_info': {
                'file_name': file_name,
                'processed_at': datetime.now().isoformat(),
                'total_rows': len(result.reports[0].dataframe) if result.reports else 0,
                'total_columns': len(result.reports[0].dataframe.columns) if result.reports else 0,
                'columns': list(result.reports[0].dataframe.columns) if result.reports else []
            },
            'data_quality': {
                'missing_values': {col: result.reports[0].dataframe[col].isnull().sum() for col in result.reports[0].dataframe.columns} if result.reports else {},
                'duplicate_rows': result.reports[0].dataframe.duplicated().sum() if result.reports else 0,
                'data_types': {col: str(dtype) for col, dtype in result.reports[0].dataframe.dtypes.items()} if result.reports else {}
            },
            'key_insights': _generate_basic_insights(result.reports[0].dataframe) if result.reports else [],
            'recommendations': [],
            'alerts': result.warnings
        }
        
        # Add AI insights if available
        if hasattr(result, 'insights') and result.insights:
            insights.update(result.insights)
        
        return excel_data, insights
        
    except Exception as e:
        logger.error(f"Error processing CSV file: {str(e)}")
        raise


def _process_zip_file(zip_data, file_name):
    """Process ZIP file containing multiple CSV files"""
    try:
        # Convert string to bytes if needed
        if isinstance(zip_data, str):
            zip_data = zip_data.encode('latin-1')
        
        # Open ZIP file
        with zipfile.ZipFile(BytesIO(zip_data), 'r') as zip_file:
            csv_files = []
            file_dfs = {}
            skipped_files = []

            # Find all CSV files in the ZIP
            for file_info in zip_file.infolist():
                if file_info.filename.lower().endswith('.csv') and not file_info.is_dir():
                    # Skip hidden files and __MACOSX folders
                    if not file_info.filename.startswith('__MACOSX/') and not file_info.filename.startswith('.'):
                        csv_files.append(file_info.filename)
            
            if not csv_files:
                raise ValueError("No CSV files found in ZIP archive")
            
            logger.info(f"Found {len(csv_files)} CSV files in ZIP: {csv_files}")
            
            # Read all CSVs into DataFrames for metadata extraction
            for csv_filename in csv_files:
                with zip_file.open(csv_filename) as csv_file_obj:
                    csv_data = csv_file_obj.read().decode('utf-8')
                    import pandas as pd
                    try:
                        df = pd.read_csv(StringIO(csv_data))
                        if df.empty and not df.columns.any():
                            raise pd.errors.EmptyDataError('No columns to parse from file')
                        file_dfs[csv_filename] = df
                    except Exception as e:
                        logger.warning(f"Skipping {csv_filename}: {str(e)}")
                        skipped_files.append({'filename': csv_filename, 'error': str(e)})
            if not file_dfs:
                raise ValueError(f"No valid CSV files found in ZIP. Skipped files: {skipped_files}")
            metadata = extract_csvs_metadata(file_dfs)

            # Process each CSV file
            if len(file_dfs) == 1:
                csv_filename = list(file_dfs.keys())[0]
                with zip_file.open(csv_filename) as csv_file:
                    csv_data = csv_file.read().decode('utf-8')
                csv_name = csv_filename.rsplit('.', 1)[0]  # Remove extension
                excel_data, insights = _process_csv_file(csv_data, csv_name)
                
                # Create ZIP response structure but propagate important keys to top level
                zip_insights = {
                    'zip_file': file_name,
                    'processed_files': [csv_filename],
                    'single_file_insights': insights,
                    'metadata': metadata,
                    'skipped_files': skipped_files
                }
                
                # Propagate critical keys to top level for consistent response format
                if 'shareable_links' in insights:
                    zip_insights['shareable_links'] = insights['shareable_links']
                if 's3_upload' in insights:
                    zip_insights['s3_upload'] = insights['s3_upload']
                if 'ai_insights' in insights:
                    zip_insights['ai_insights'] = insights['ai_insights']
                if 'aggregation_results' in insights:
                    zip_insights['aggregation_results'] = insights['aggregation_results']
                
                logger.info(f"✅ ZIP processing: propagated {list(insights.keys())} to top level")
                return excel_data, zip_insights
            else:
                combined_insights = {
                    'zip_file': file_name,
                    'processed_files': list(file_dfs.keys()),
                    'individual_file_insights': {},
                    'combined_summary': {
                        'total_files': len(file_dfs),
                        'total_rows': 0,
                        'file_summaries': []
                    },
                    'metadata': metadata,
                    'skipped_files': skipped_files
                }
                main_csv = list(file_dfs.keys())[0]
                with zip_file.open(main_csv) as csv_file:
                    csv_data = csv_file.read().decode('utf-8')
                csv_name = main_csv.rsplit('.', 1)[0]
                excel_data, main_insights = _process_csv_file(csv_data, csv_name)
                combined_insights['primary_file'] = main_csv
                combined_insights['primary_insights'] = main_insights
                for csv_filename in list(file_dfs.keys())[1:]:
                    try:
                        with zip_file.open(csv_filename) as csv_file:
                            csv_content = csv_file.read().decode('utf-8')
                            lines = csv_content.strip().split('\n')
                            if lines:
                                header_cols = len(lines[0].split(',')) if lines[0] else 0
                                data_rows = len(lines) - 1  # Subtract header
                                combined_insights['combined_summary']['file_summaries'].append({
                                    'filename': csv_filename,
                                    'rows': data_rows,
                                    'columns': header_cols
                                })
                                combined_insights['combined_summary']['total_rows'] += data_rows
                    except Exception as e:
                        logger.warning(f"Could not analyze {csv_filename}: {str(e)}")
                        combined_insights['combined_summary']['file_summaries'].append({
                            'filename': csv_filename,
                            'error': str(e)
                        })
                
                # Propagate critical keys from primary insights to top level for consistent response format
                if 'shareable_links' in main_insights:
                    combined_insights['shareable_links'] = main_insights['shareable_links']
                if 's3_upload' in main_insights:
                    combined_insights['s3_upload'] = main_insights['s3_upload']
                if 'ai_insights' in main_insights:
                    combined_insights['ai_insights'] = main_insights['ai_insights']
                if 'aggregation_results' in main_insights:
                    combined_insights['aggregation_results'] = main_insights['aggregation_results']
                
                logger.info(f"✅ ZIP multi-file processing: propagated {list(main_insights.keys())} to top level")
                return excel_data, combined_insights
                
    except zipfile.BadZipFile:
        raise ValueError("Invalid or corrupted ZIP file")
    except Exception as e:
        logger.error(f"Error processing ZIP file: {str(e)}")
        raise ValueError(f"Failed to process ZIP file: {str(e)}")


def _get_s3_bucket_name():
    """Get S3 bucket name from environment or CloudFormation"""
    import os
    import boto3
    
    # Try environment variable first
    bucket_name = os.getenv('S3_BUCKET_NAME')
    if bucket_name:
        return bucket_name
    
    # Try to find bucket with our stack name pattern
    try:
        cf_client = boto3.client('cloudformation')
        s3_client = boto3.client('s3')
        
        # List buckets and find one that matches our pattern
        buckets = s3_client.list_buckets()['Buckets']
        for bucket in buckets:
            bucket_name = bucket['Name']
            if 'restaurant-reports' in bucket_name and '-reports-' in bucket_name:
                return bucket_name
    except Exception as e:
        logger.warning(f"Could not auto-discover S3 bucket: {e}")
    
    return None


def _generate_excel_from_dataframe(df, file_name):
    """Generate Excel file from DataFrame"""
    import pandas as pd
    
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        # Original data sheet
        df.to_excel(writer, sheet_name='Original Data', index=False)
        
        # Summary statistics sheet
        if df.select_dtypes(include=[int, float]).shape[1] > 0:
            summary_stats = df.describe()
            summary_stats.to_excel(writer, sheet_name='Summary Statistics')
    
    return excel_buffer.getvalue()


def _generate_basic_insights(df):
    """Generate basic insights from DataFrame"""
    import pandas as pd
    
    insights = []
    
    # Sales analysis if Sales column exists
    sales_cols = [col for col in df.columns if 'sales' in col.lower()]
    if sales_cols:
        sales_col = sales_cols[0]
        if pd.api.types.is_numeric_dtype(df[sales_col]):
            total = df[sales_col].sum()
            avg = df[sales_col].mean()
            median = df[sales_col].median()
            insights.append({
                "metric": "Sales Analysis",
                "total": f"{total:.2f}",
                "average": f"{avg:.2f}",
                "median": f"{median:.2f}"
            })
    
    # Date range if date column exists
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    if date_cols:
        date_col = date_cols[0]
        try:
            df[date_col] = pd.to_datetime(df[date_col])
            min_date = df[date_col].min()
            max_date = df[date_col].max()
            insights.append({
                "metric": "Date Range",
                "value": f"{min_date} to {max_date}"
            })
        except:
            pass
    
    return insights


def to_serializable(obj):
    """Recursively convert numpy types to native Python types for JSON serialization."""
    import numpy as np
    if isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_serializable(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(to_serializable(v) for v in obj)
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif obj is None:
        return None
    else:
        return obj

