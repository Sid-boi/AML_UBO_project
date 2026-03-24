# src/semi/json_normalizer.py

import json
from typing import List, Dict, Any
import logging

from .base_normalizer import BaseNormalizer

logger = logging.getLogger(__name__)


class JSONNormalizer(BaseNormalizer):
    """
    JSON file normalizer with ARRAY EXPLOSION.
    
    Philosophy:
    - One entity per record (1NF normalization)
    - Arrays (directorships, ownerships) → Multiple records
    - Clean, simple DINT ingestion
    """
    
    def parse(self, filepath: str) -> List[Dict]:
        """
        Parse JSON with array explosion.
        
        Returns: Flat list of records (one entity per record)
        """
        
        logger.info(f"Parsing JSON: {filepath}")
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # Handle different JSON structures
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            if 'data' in data:
                records = data['data']
            elif 'records' in data:
                records = data['records']
            elif 'results' in data:
                records = data['results']
            else:
                records = [data]
        else:
            raise ValueError(f"Unexpected JSON structure: {type(data)}")
        
        # Flatten nested objects
        flattened = [self._flatten_record(r) for r in records]
        
        # ✨ EXPLODE ARRAYS (NEW!)
        exploded = []
        for record in flattened:
            exploded_records = self._explode_arrays(record)
            exploded.extend(exploded_records)
        
        logger.info(
            f"Parsed {len(records)} JSON objects → "
            f"{len(flattened)} flattened → "
            f"{len(exploded)} exploded records"
        )
        
        return exploded
    
    def _flatten_record(self, record: Dict) -> Dict:
        """Flatten nested objects (keep arrays for now)"""
        
        flat = {}
        
        for key, value in record.items():
            if isinstance(value, dict):
                # Handle nested objects based on key
                if key.lower() in ['address', 'registeredaddress', 'registered_address']:
                    # Flatten address
                    for nested_key, nested_value in value.items():
                        if nested_key.lower() in ['line1', 'addressline1']:
                            flat['address_line1'] = nested_value
                        elif nested_key.lower() in ['line2', 'addressline2']:
                            flat['address_line2'] = nested_value
                        elif nested_key.lower() in ['city', 'town', 'posttown']:
                            flat['city'] = nested_value
                        elif nested_key.lower() in ['country', 'nation']:
                            flat['country'] = nested_value
                        else:
                            flat[nested_key] = nested_value
                
                elif key.lower() in ['ownership', 'ownershipdetails', 'owned_by']:
                    # Flatten ownership
                    for nested_key, nested_value in value.items():
                        if nested_key.lower() in ['entityid', 'entity_id', 'owner_id']:
                            flat['owned_by_entity_id'] = nested_value
                        elif nested_key.lower() in ['percentage', 'ownership_percentage']:
                            flat['ownership_percentage'] = nested_value
                        else:
                            flat[nested_key] = nested_value
                
                elif key.lower() in ['directorship', 'director_of', 'directorof']:
                    # Keep directorship array for explosion later
                    flat['_directorship_array'] = value
                
                else:
                    # Generic nested object - flatten keys
                    for nested_key, nested_value in value.items():
                        flat[nested_key] = nested_value
            
            elif isinstance(value, list):
                # Keep arrays for explosion later
                if key.lower() in ['director_of', 'director_of_entity_ids', 'company_ids', 'companies']:
                    flat['_directorship_array'] = value
                else:
                    # Non-directorship arrays - just join
                    flat[key] = '; '.join(str(v) for v in value)
            
            else:
                # Scalar value
                flat[key] = value
        
        # Build full address if components exist
        if not flat.get('address') and flat.get('city'):
            address_parts = [
                flat.get('address_line1'),
                flat.get('address_line2'),
                flat.get('city'),
                flat.get('country')
            ]
            flat['address'] = ', '.join(filter(None, address_parts))
        
        return flat
    
    def _explode_arrays(self, record: Dict) -> List[Dict]:
        """
        Explode array fields into multiple records.
        
        Example:
        Input:  {"name": "Alice", "_directorship_array": ["CO_123", "CO_456"]}
        Output: [
            {"name": "Alice", "director_of_entity_ids": "CO_123"},
            {"name": "Alice", "director_of_entity_ids": "CO_456"}
        ]
        """
        
        # Check for directorship array
        directorship_array = record.pop('_directorship_array', None)
        
        if not directorship_array:
            # No arrays to explode, return as-is
            return [record]
        
        # Parse directorship array
        company_ids = self._parse_directorship_array(directorship_array)
        
        if not company_ids:
            # Empty array, return record without directorship
            return [record]
        
        # ✨ EXPLODE: Create one record per company
        exploded_records = []
        
        for company_id in company_ids:
            # Clone the base record
            exploded_record = record.copy()
            
            # Add single directorship
            exploded_record['director_of_entity_ids'] = company_id
            
            exploded_records.append(exploded_record)
        
        logger.debug(
            f"Exploded {record.get('full_name', 'Unknown')} into "
            f"{len(exploded_records)} directorship records"
        )
        
        return exploded_records
    
    def _parse_directorship_array(self, value: Any) -> List[str]:
        """Parse directorship array from various formats"""
        
        if isinstance(value, list):
            # Already a list
            return [str(v) for v in value if v]
        
        elif isinstance(value, dict):
            # Nested object with company_ids key
            if 'company_ids' in value:
                return self._parse_directorship_array(value['company_ids'])
            elif 'companies' in value:
                return self._parse_directorship_array(value['companies'])
            else:
                # Unknown nested structure, skip
                return []
        
        elif isinstance(value, str):
            # Semicolon-separated string
            return [v.strip() for v in value.split(';') if v.strip()]
        
        else:
            # Unknown type, skip
            return []