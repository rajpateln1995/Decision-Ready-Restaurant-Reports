"""
ZIP file processor for handling multiple CSV files
"""
import logging
import zipfile
from io import BytesIO
from typing import Dict, Any, List

from ..models import FileInfo, FileType, ProcessingResult
from .csv_processor import CSVProcessor
from ..analyzers.ai_analyzer import AIAnalyzer

logger = logging.getLogger(__name__)


class ZipProcessor:
    """Processor for ZIP files containing multiple CSV files"""
    
    def __init__(self, enable_ai_analysis: bool = True):
        """
        Initialize ZIP processor
        
        Args:
            enable_ai_analysis: Whether to enable AI-powered analysis
        """
        self.csv_processor = CSVProcessor(enable_ai_analysis=False)  # We'll do AI analysis at ZIP level
        self.enable_ai_analysis = enable_ai_analysis
        if enable_ai_analysis:
            try:
                self.ai_analyzer = AIAnalyzer()
            except Exception as e:
                logger.warning(f"Could not initialize AI analyzer: {e}")
                self.enable_ai_analysis = False
        
        self.max_file_size = 50 * 1024 * 1024  # 50MB per file
        self.max_files = 20  # Maximum number of CSV files to process
    
    def process_zip_bytes(self, zip_bytes: bytes, file_info: FileInfo) -> ProcessingResult:
        """
        Process ZIP file containing CSV files
        
        Args:
            zip_bytes: ZIP file as bytes
            file_info: Information about the ZIP file
            
        Returns:
            ProcessingResult: Processing results
        """
        result = ProcessingResult(success=True)
        
        try:
            # Open ZIP file
            with zipfile.ZipFile(BytesIO(zip_bytes), 'r') as zip_file:
                csv_files = self._get_csv_files_from_zip(zip_file)
                
                if not csv_files:
                    result.add_error("No CSV files found in ZIP archive")
                    return result
                
                if len(csv_files) > self.max_files:
                    result.add_warning(f"ZIP contains {len(csv_files)} CSV files, processing only first {self.max_files}")
                    csv_files = csv_files[:self.max_files]
                
                logger.info(f"Processing {len(csv_files)} CSV files from ZIP: {file_info.name}")
                
                # Process each CSV file
                for csv_filename in csv_files:
                    try:
                        csv_result = self._process_csv_from_zip(zip_file, csv_filename, file_info)
                        
                        # Merge results
                        result.reports.extend(csv_result.reports)
                        result.errors.extend(csv_result.errors)
                        result.warnings.extend(csv_result.warnings)
                        
                        if not csv_result.success:
                            result.success = False
                            
                    except Exception as e:
                        result.add_error(f"Failed to process '{csv_filename}': {str(e)}")
                
                if result.reports:
                    logger.info(f"Successfully processed {len(result.reports)} CSV files from ZIP")
                    
                    # Perform comprehensive AI analysis on the combined ZIP results
                    if self.enable_ai_analysis and hasattr(self, 'ai_analyzer'):
                        try:
                            result = self.ai_analyzer.analyze_processing_result(result, "comprehensive")
                        except Exception as e:
                            logger.warning(f"AI analysis failed: {e}")
                            result.add_warning(f"AI analysis unavailable: {str(e)}")
                else:
                    result.add_error("No CSV files could be processed from ZIP")
                    
        except zipfile.BadZipFile:
            result.add_error("Invalid or corrupted ZIP file")
        except Exception as e:
            result.add_error(f"Error processing ZIP file: {str(e)}")
            
        return result
    
    def _get_csv_files_from_zip(self, zip_file: zipfile.ZipFile) -> List[str]:
        """Get list of CSV files from ZIP archive"""
        csv_files = []
        
        for file_info in zip_file.infolist():
            # Skip directories
            if file_info.is_dir():
                continue
            
            # Check if it's a CSV file
            if file_info.filename.lower().endswith('.csv'):
                # Skip hidden files and files in __MACOSX folders
                if not file_info.filename.startswith('__MACOSX/') and not file_info.filename.startswith('.'):
                    csv_files.append(file_info.filename)
        
        return sorted(csv_files)  # Sort for consistent processing order
    
    def _process_csv_from_zip(self, zip_file: zipfile.ZipFile, csv_filename: str, 
                             zip_file_info: FileInfo) -> ProcessingResult:
        """Process a single CSV file from within the ZIP"""
        
        try:
            # Get file info from ZIP
            csv_info = zip_file.getinfo(csv_filename)
            
            # Check file size
            if csv_info.file_size > self.max_file_size:
                result = ProcessingResult(success=False)
                result.add_error(f"CSV file '{csv_filename}' is too large ({csv_info.file_size} bytes)")
                return result
            
            # Read CSV data from ZIP
            with zip_file.open(csv_filename) as csv_file:
                csv_data = csv_file.read().decode('utf-8')
            
            # Create file info for the CSV
            csv_file_info = FileInfo(
                name=csv_filename,
                original_name=f"{zip_file_info.original_name}#{csv_filename}",  # Include ZIP name
                file_type=FileType.CSV,
                size_bytes=csv_info.file_size,
                uploaded_at=zip_file_info.uploaded_at,
                metadata={
                    'from_zip': True,
                    'zip_file': zip_file_info.name,
                    'compressed_size': csv_info.compress_size
                }
            )
            
            # Process the CSV
            return self.csv_processor.process_csv_string(csv_data, csv_file_info)
            
        except UnicodeDecodeError:
            result = ProcessingResult(success=False)
            result.add_error(f"Cannot decode CSV file '{csv_filename}' - invalid encoding")
            return result
        except Exception as e:
            result = ProcessingResult(success=False)
            result.add_error(f"Error reading '{csv_filename}' from ZIP: {str(e)}")
            return result
