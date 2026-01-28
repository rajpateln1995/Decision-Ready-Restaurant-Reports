"""
Main file processor that handles different file types
"""
import logging
import base64
from typing import Dict, Any, List
from io import BytesIO, StringIO

from ..models import FileInfo, FileType, ProcessingResult
from .csv_processor import CSVProcessor
from .zip_processor import ZipProcessor

logger = logging.getLogger(__name__)


class FileProcessor:
    """Main processor that routes files to appropriate handlers"""
    
    def __init__(self):
        self.csv_processor = CSVProcessor()
        self.zip_processor = ZipProcessor()
    
    def process_request(self, event_data: Dict[str, Any]) -> ProcessingResult:
        """
        Process incoming request with file data
        
        Args:
            event_data: Request data containing file information
            
        Returns:
            ProcessingResult: Processing results
        """
        try:
            # Determine input type and extract file data
            if 'csv_data' in event_data:
                # Single CSV data
                return self._process_csv_data(event_data)
            elif 'zip_data' in event_data:
                # ZIP file with multiple CSVs
                return self._process_zip_data(event_data)
            elif 'zip_base64' in event_data:
                # Base64 encoded ZIP file
                return self._process_zip_base64(event_data)
            else:
                result = ProcessingResult(success=False)
                result.add_error("No supported file data found in request")
                return result
                
        except Exception as e:
            logger.error(f"Error processing request: {str(e)}")
            result = ProcessingResult(success=False)
            result.add_error(f"Processing failed: {str(e)}")
            return result
    
    def _process_csv_data(self, event_data: Dict[str, Any]) -> ProcessingResult:
        """Process single CSV data"""
        csv_data = event_data.get('csv_data', '')
        file_name = event_data.get('file_name', 'data.csv')
        
        file_info = FileInfo.from_filename(file_name)
        return self.csv_processor.process_csv_string(csv_data, file_info)
    
    def _process_zip_data(self, event_data: Dict[str, Any]) -> ProcessingResult:
        """Process ZIP data"""
        zip_data = event_data.get('zip_data', '')
        file_name = event_data.get('file_name', 'data.zip')
        
        file_info = FileInfo.from_filename(file_name)
        # Convert string to bytes if needed
        if isinstance(zip_data, str):
            zip_bytes = zip_data.encode('utf-8')
        else:
            zip_bytes = zip_data
            
        return self.zip_processor.process_zip_bytes(zip_bytes, file_info)
    
    def _process_zip_base64(self, event_data: Dict[str, Any]) -> ProcessingResult:
        """Process base64 encoded ZIP data"""
        zip_base64 = event_data.get('zip_base64', '')
        file_name = event_data.get('file_name', 'data.zip')
        
        try:
            zip_bytes = base64.b64decode(zip_base64)
            file_info = FileInfo.from_filename(file_name, size_bytes=len(zip_bytes))
            return self.zip_processor.process_zip_bytes(zip_bytes, file_info)
        except Exception as e:
            result = ProcessingResult(success=False)
            result.add_error(f"Failed to decode base64 ZIP data: {str(e)}")
            return result
