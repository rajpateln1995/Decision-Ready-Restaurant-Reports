"""
File information and type definitions
"""
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime


class FileType(Enum):
    CSV = "csv"
    ZIP = "zip"
    UNKNOWN = "unknown"


@dataclass
class FileInfo:
    """Information about a file being processed"""
    name: str
    original_name: str
    file_type: FileType
    size_bytes: int
    uploaded_at: datetime
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    @property
    def extension(self) -> str:
        """Get file extension"""
        return self.name.split('.')[-1].lower() if '.' in self.name else ''
    
    @classmethod
    def from_filename(cls, filename: str, size_bytes: int = 0) -> 'FileInfo':
        """Create FileInfo from filename"""
        ext = filename.split('.')[-1].lower() if '.' in filename else ''
        
        if ext == 'csv':
            file_type = FileType.CSV
        elif ext == 'zip':
            file_type = FileType.ZIP
        else:
            file_type = FileType.UNKNOWN
        
        return cls(
            name=filename,
            original_name=filename,
            file_type=file_type,
            size_bytes=size_bytes,
            uploaded_at=datetime.now()
        )
