"""
Data models and structures for restaurant reports
"""
from .report_data import ReportData, ProcessingResult
from .file_info import FileInfo, FileType

__all__ = ['ReportData', 'ProcessingResult', 'FileInfo', 'FileType']
