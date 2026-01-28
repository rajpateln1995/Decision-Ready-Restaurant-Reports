"""
Report data structures and processing results
"""
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd


@dataclass
class ReportData:
    """Structured data for a single report/CSV"""
    name: str
    dataframe: pd.DataFrame
    file_info: Dict[str, Any]
    processed_at: datetime = field(default_factory=datetime.now)
    
    @property
    def row_count(self) -> int:
        return len(self.dataframe)
    
    @property
    def column_count(self) -> int:
        return len(self.dataframe.columns)
    
    @property
    def columns(self) -> List[str]:
        return list(self.dataframe.columns)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get basic summary of the report data"""
        return {
            'name': self.name,
            'rows': self.row_count,
            'columns': self.column_count,
            'column_names': self.columns,
            'processed_at': self.processed_at.isoformat(),
            'data_types': {col: str(dtype) for col, dtype in self.dataframe.dtypes.items()},
            'missing_values': {col: int(self.dataframe[col].isnull().sum()) for col in self.columns}
        }


@dataclass
class ProcessingResult:
    """Result of processing one or more files"""
    success: bool
    reports: List[ReportData] = field(default_factory=list)
    insights: Dict[str, Any] = field(default_factory=dict)
    visualizations: Dict[str, Any] = field(default_factory=dict)
    excel_files: Dict[str, bytes] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)  # <-- Add this line
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    processing_time_seconds: float = 0.0
    
    @property
    def total_reports(self) -> int:
        return len(self.reports)
    
    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0
    
    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0
    
    def add_error(self, error: str):
        """Add an error message"""
        self.errors.append(error)
        self.success = False
    
    def add_warning(self, warning: str):
        """Add a warning message"""
        self.warnings.append(warning)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of processing results"""
        return {
            'success': self.success,
            'total_reports': self.total_reports,
            'processing_time_seconds': self.processing_time_seconds,
            'reports_summary': [report.get_summary() for report in self.reports],
            'has_errors': self.has_errors,
            'has_warnings': self.has_warnings,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings)
        }
