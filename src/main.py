"""
Main processing module for restaurant reports
"""
import logging
import pandas as pd
from typing import Dict, Any, Tuple
from io import BytesIO, StringIO
from datetime import datetime
from src.utils.metadata_extractor import    extract_csvs_metadata

logger = logging.getLogger(__name__)


def process_restaurant_report(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process restaurant report data and generate AI-powered insights
    
    Args:
        data: Restaurant report data from Toast
        
    Returns:
        dict: Processed report with visualizations and actionable summary
    """
    try:
        logger.info("Starting restaurant report processing")
        
        # Placeholder for actual processing logic
        # This is where you'll implement:
        # 1. Data validation and parsing
        # 2. AI-powered analysis
        # 3. Visualization generation
        # 4. Actionable summary creation
        
        report_type = data.get('report_type', 'unknown')
        restaurant_id = data.get('restaurant_id', 'unknown')
        
        logger.info(f"Processing {report_type} report for restaurant {restaurant_id}")
        
        # Mock response structure
        result = {
            'restaurant_id': restaurant_id,
            'report_type': report_type,
            'processed_at': None,  # Add timestamp in actual implementation
            'visualizations': {
                'sales_trends': {},
                'performance_metrics': {},
                'customer_insights': {}
            },
            'actionable_summary': {
                'key_insights': [],
                'recommendations': [],
                'alerts': []
            },
            'status': 'processed'
        }
        
        logger.info("Restaurant report processing completed successfully")
        return result
        
    except Exception as e:
        logger.error(f"Error processing restaurant report: {str(e)}")
        raise


def process_csv_to_excel(csv_data: str, file_name: str = "restaurant_report") -> Tuple[bytes, Dict[str, Any]]:
    """
    Process CSV data and generate Excel file with AI-powered insights
    
    Args:
        csv_data: Raw CSV data as string
        file_name: Name of the file for processing context
        
    Returns:
        tuple: (Excel file as bytes, insights dictionary)
    """
    try:
        logger.info(f"Starting CSV to Excel processing for file: {file_name}")
        # Parse CSV data
        df = pd.read_csv(StringIO(csv_data))
        logger.info(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
        # Extract metadata
        metadata = extract_csvs_metadata({file_name: df})
        # Generate insights from the data
        insights = _generate_insights(df, file_name)
        # Create Excel file with multiple sheets
        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            # Original data sheet
            df.to_excel(writer, sheet_name='Original Data', index=False)
            
            # Summary statistics sheet
            summary_stats = _generate_summary_statistics(df)
            summary_stats.to_excel(writer, sheet_name='Summary Statistics')
            
            # Insights sheet
            insights_df = _create_insights_dataframe(insights)
            insights_df.to_excel(writer, sheet_name='Insights', index=False)
        excel_data = excel_buffer.getvalue()
        
        # Return Excel bytes and info dict with insights and metadata
        return excel_data, {'insights': insights, 'metadata': metadata}

    except Exception as e:
        logger.error(f"Error processing CSV to Excel: {str(e)}")
        raise


def _generate_insights(df: pd.DataFrame, file_name: str) -> Dict[str, Any]:
    """Generate AI-powered insights from the dataframe"""
    try:
        insights = {
            'file_info': {
                'file_name': file_name,
                'processed_at': datetime.now().isoformat(),
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'columns': list(df.columns)
            },
            'data_quality': {
                'missing_values': {k: int(v) for k, v in df.isnull().sum().to_dict().items()},
                'duplicate_rows': int(df.duplicated().sum()),
                'data_types': {k: str(v) for k, v in df.dtypes.to_dict().items()}
            },
            'key_insights': [],
            'recommendations': [],
            'alerts': []
        }
        
        # Detect numeric columns for analysis
        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
        
        if numeric_columns:
            # Revenue/Sales analysis (look for common column names)
            revenue_columns = [col for col in numeric_columns 
                             if any(keyword in col.lower() for keyword in 
                                  ['revenue', 'sales', 'total', 'amount', 'price'])]
            
            if revenue_columns:
                for col in revenue_columns[:3]:  # Analyze top 3 revenue columns
                    total = df[col].sum()
                    mean = df[col].mean()
                    median = df[col].median()
                    
                    insights['key_insights'].append({
                        'metric': f'{col} Analysis',
                        'total': f'${total:,.2f}' if total > 1000 else f'{total:.2f}',
                        'average': f'${mean:,.2f}' if mean > 1000 else f'{mean:.2f}',
                        'median': f'${median:,.2f}' if median > 1000 else f'{median:.2f}'
                    })
                    
                    # Generate recommendations based on data patterns
                    if mean > median * 1.5:
                        insights['recommendations'].append({
                            'category': 'Revenue Distribution',
                            'recommendation': f'High variation in {col} detected. Consider analyzing top performers to identify success patterns.',
                            'priority': 'Medium'
                        })
        
        # Date/time analysis
        date_columns = df.select_dtypes(include=['datetime', 'object']).columns
        for col in date_columns:
            if any(keyword in col.lower() for keyword in ['date', 'time', 'created', 'updated']):
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    if not df[col].isnull().all():
                        date_range = f"{df[col].min()} to {df[col].max()}"
                        insights['key_insights'].append({
                            'metric': f'{col} Range',
                            'value': date_range
                        })
                except:
                    continue
        
        # Data quality alerts
        missing_percentage = (df.isnull().sum() / len(df) * 100)
        high_missing = missing_percentage[missing_percentage > 20]
        
        if not high_missing.empty:
            insights['alerts'].append({
                'type': 'Data Quality',
                'message': f'High missing values detected in: {", ".join(high_missing.index)}',
                'severity': 'Warning'
            })
        
        if insights['data_quality']['duplicate_rows'] > 0:
            insights['alerts'].append({
                'type': 'Data Quality',
                'message': f'{insights["data_quality"]["duplicate_rows"]} duplicate rows found',
                'severity': 'Info'
            })
        
        return insights
        
    except Exception as e:
        logger.error(f"Error generating insights: {str(e)}")
        return {
            'error': 'Failed to generate insights',
            'message': str(e),
            'file_info': {
                'file_name': file_name,
                'processed_at': datetime.now().isoformat()
            }
        }


def _generate_summary_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """Generate summary statistics for numeric columns"""
    try:
        numeric_df = df.select_dtypes(include=['number'])
        if numeric_df.empty:
            return pd.DataFrame({'Message': ['No numeric columns found for statistics']})
        
        stats = numeric_df.describe()
        return stats
    except Exception as e:
        logger.error(f"Error generating summary statistics: {str(e)}")
        return pd.DataFrame({'Error': [str(e)]})


def _create_insights_dataframe(insights: Dict[str, Any]) -> pd.DataFrame:
    """Convert insights dictionary to DataFrame for Excel export"""
    try:
        insights_list = []
        
        # File info
        file_info = insights.get('file_info', {})
        insights_list.append({
            'Category': 'File Information',
            'Metric': 'File Name',
            'Value': file_info.get('file_name', 'Unknown')
        })
        insights_list.append({
            'Category': 'File Information',
            'Metric': 'Processed At',
            'Value': file_info.get('processed_at', 'Unknown')
        })
        insights_list.append({
            'Category': 'File Information',
            'Metric': 'Total Rows',
            'Value': file_info.get('total_rows', 0)
        })
        insights_list.append({
            'Category': 'File Information',
            'Metric': 'Total Columns',
            'Value': file_info.get('total_columns', 0)
        })
        
        # Key insights
        for insight in insights.get('key_insights', []):
            insights_list.append({
                'Category': 'Key Insights',
                'Metric': insight.get('metric', 'Unknown'),
                'Value': insight.get('value', insight.get('total', 'Unknown'))
            })
        
        # Alerts
        for alert in insights.get('alerts', []):
            insights_list.append({
                'Category': f'Alert ({alert.get("severity", "Unknown")})',
                'Metric': alert.get('type', 'Unknown'),
                'Value': alert.get('message', 'Unknown')
            })
        
        return pd.DataFrame(insights_list)
    except Exception as e:
        logger.error(f"Error creating insights dataframe: {str(e)}")
        return pd.DataFrame({'Error': [str(e)]})


def _create_recommendations_dataframe(insights: Dict[str, Any]) -> pd.DataFrame:
    """Convert recommendations to DataFrame for Excel export"""
    try:
        recommendations = insights.get('recommendations', [])
        if not recommendations:
            return pd.DataFrame({'Message': ['No specific recommendations generated']})
        
        recommendations_list = []
        for rec in recommendations:
            recommendations_list.append({
                'Category': rec.get('category', 'General'),
                'Recommendation': rec.get('recommendation', 'Unknown'),
                'Priority': rec.get('priority', 'Low')
            })
        
        return pd.DataFrame(recommendations_list)
    except Exception as e:
        logger.error(f"Error creating recommendations dataframe: {str(e)}")
        return pd.DataFrame({'Error': [str(e)]})
