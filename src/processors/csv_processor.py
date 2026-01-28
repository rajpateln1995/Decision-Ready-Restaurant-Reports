"""
CSV file processor
"""
import logging
import pandas as pd
from io import StringIO
from typing import Dict, Any

from ..models import FileInfo, ReportData, ProcessingResult
from ..utils.metadata_extractor import extract_csvs_metadata
from ..analyzers.ai_analyzer import AIAnalyzer

logger = logging.getLogger(__name__)


class CSVProcessor:
    """Processor for individual CSV files"""
    
    def __init__(self, enable_ai_analysis: bool = True):
        """
        Initialize CSV processor
        
        Args:
            enable_ai_analysis: Whether to enable AI-powered analysis
        """
        self.enable_ai_analysis = enable_ai_analysis
        if enable_ai_analysis:
            try:
                self.ai_analyzer = AIAnalyzer()
            except Exception as e:
                logger.warning(f"Could not initialize AI analyzer: {e}")
                self.enable_ai_analysis = False
    
    def process_csv_string(self, csv_data: str, file_info: FileInfo) -> ProcessingResult:
        """
        Process CSV data from string
        
        Args:
            csv_data: CSV data as string
            file_info: Information about the file
            
        Returns:
            ProcessingResult: Processing results
        """
        result = ProcessingResult(success=True)
        
        try:
            # Parse CSV data
            df = pd.read_csv(StringIO(csv_data))
            logger.info(f"Loaded CSV '{file_info.name}' with {len(df)} rows and {len(df.columns)} columns")
            
            # Create report data
            report_data = ReportData(
                name=file_info.name,
                dataframe=df,
                file_info={
                    'original_name': file_info.original_name,
                    'size_bytes': file_info.size_bytes,
                    'uploaded_at': file_info.uploaded_at.isoformat()
                }
            )
            
            # Add to results
            result.reports.append(report_data)
            
            # Extract and attach metadata
            result.metadata = extract_csvs_metadata({file_info.name: df})

            # Validate data quality
            self._validate_data_quality(df, result)
            
            # Perform AI analysis if enabled
            if self.enable_ai_analysis and hasattr(self, 'ai_analyzer'):
                try:
                    result = self.ai_analyzer.analyze_processing_result(result, "quick_summary")
                except Exception as e:
                    logger.warning(f"AI analysis failed: {e}")
                    result.add_warning(f"AI analysis unavailable: {str(e)}")
            
            logger.info(f"Successfully processed CSV: {file_info.name}")
            
        except pd.errors.EmptyDataError:
            result.add_error(f"CSV file '{file_info.name}' is empty")
        except pd.errors.ParserError as e:
            result.add_error(f"Failed to parse CSV file '{file_info.name}': {str(e)}")
        except Exception as e:
            result.add_error(f"Error processing CSV '{file_info.name}': {str(e)}")
            
        return result
    
    def _validate_data_quality(self, df: pd.DataFrame, result: ProcessingResult):
        """Validate data quality and add warnings if needed"""
        
        # Check for empty dataframe
        if df.empty:
            result.add_warning("CSV file contains no data rows")
            return
        
        # Check for missing values
        missing_counts = df.isnull().sum()
        high_missing = missing_counts[missing_counts > len(df) * 0.5]  # More than 50% missing
        
        if not high_missing.empty:
            result.add_warning(f"High missing values (>50%) in columns: {', '.join(high_missing.index)}")
        
        # Check for duplicate rows
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            result.add_warning(f"Found {duplicate_count} duplicate rows")
        
        # Check for very wide datasets (too many columns might indicate parsing issues)
        if len(df.columns) > 50:
            result.add_warning(f"Large number of columns ({len(df.columns)}) - verify CSV format is correct")
        
        # Check for very narrow datasets
        if len(df.columns) < 2:
            result.add_warning("CSV has very few columns - verify data is structured correctly")
