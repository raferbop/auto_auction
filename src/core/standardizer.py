# standardizer.py

import re
from typing import Any, Union, Optional

class DataStandardizer:
    """Standardizes extracted auction data for consistent formatting"""
    
    def __init__(self):
        self.color_mappings = {
            'white': 'White',
            'black': 'Black',
            'silver': 'Silver',
            'gray': 'Gray',
            'red': 'Red',
            'blue': 'Blue',
            'green': 'Green',
            'yellow': 'Yellow',
            'orange': 'Orange',
            'purple': 'Purple',
            'brown': 'Brown',
            'beige': 'Beige',
            'gold': 'Gold',
            'pink': 'Pink',
            'navy': 'Navy',
            'maroon': 'Maroon',
            'teal': 'Teal',
            'lime': 'Lime',
            'cyan': 'Cyan',
            'magenta': 'Magenta'
        }
        
        self.transmission_mappings = {
            'automatic': 'Automatic',
            'manual': 'Manual',
            'cvt': 'CVT',
            'semi-auto': 'Semi-Automatic',
            'auto': 'Automatic',
            'mt': 'Manual',
            'at': 'Automatic'
        }
    
    @staticmethod
    def standardize_color(color_str: Optional[str]) -> Optional[str]:
        if not color_str:
            return None
        color_str = color_str.upper()
        mappings = {
            'WHITE': ['PEARL', 'CRYSTAL WHITE'],
            'BLACK': ['OBSIDIAN', 'CRYSTAL BLACK'],
            'SILVER': ['METALLIC SILVER', 'LIGHT SILVER']
        }
        for standard, variants in mappings.items():
            if any(variant in color_str for variant in variants):
                return standard
        return color_str

    @staticmethod
    def parse_numeric(value_str: Optional[str]) -> int:
        """Parse numeric values from strings, handling various formats"""
        if not value_str:
            return 0
        try:
            # Remove common non-numeric characters but keep decimal points and commas
            cleaned = ''.join(c for c in str(value_str) if c.isdigit() or c in ['.', ','])
            
            # Remove commas (thousand separators)
            cleaned = cleaned.replace(',', '')
            
            # Handle decimal points
            if '.' in cleaned:
                cleaned = cleaned.split('.')[0]  # Take only the integer part
            
            # Convert to int
            return int(cleaned) if cleaned else 0
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def parse_price(price_str: Optional[str]) -> int:
        """Parse price strings to integers"""
        if not price_str:
            return 0
        
        # Remove currency symbols and common price indicators
        cleaned = re.sub(r'[^\d.,]', '', str(price_str))
        
        # Handle commas (thousand separators)
        cleaned = cleaned.replace(',', '')
        
        # Handle decimal points
        if '.' in cleaned:
            cleaned = cleaned.split('.')[0]
        
        try:
            return int(cleaned) if cleaned else 0
        except ValueError:
            return 0

    @staticmethod
    def parse_mileage(mileage_str: Optional[str]) -> int:
        """Parse mileage strings to integers"""
        if not mileage_str:
            return 0
        
        # Remove common mileage indicators
        cleaned = re.sub(r'[^\d.,]', '', str(mileage_str))
        
        # Handle commas (thousand separators)
        cleaned = cleaned.replace(',', '')
        
        # Handle decimal points
        if '.' in cleaned:
            cleaned = cleaned.split('.')[0]
        
        try:
            return int(cleaned) if cleaned else 0
        except ValueError:
            return 0

    @staticmethod
    def parse_year(year_str: Optional[str]) -> int:
        """Parse year strings to integers"""
        if not year_str:
            return 0
        
        # Extract 4-digit year
        year_match = re.search(r'\b(19|20)\d{2}\b', str(year_str))
        if year_match:
            return int(year_match.group())
        
        # Try to parse as integer
        try:
            year_int = int(str(year_str).strip())
            if 1900 <= year_int <= 2100:
                return year_int
        except ValueError:
            pass
        
        return 0

    @staticmethod
    def standardize_text(text: Optional[str]) -> str:
        """Standardize general text fields"""
        if not text:
            return ''
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', text.strip())
        
        # Capitalize first letter of each word
        return ' '.join(word.capitalize() for word in cleaned.split()) 