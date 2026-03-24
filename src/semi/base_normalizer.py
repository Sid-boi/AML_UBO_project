# src/semi/base_normalizer.py

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime
import yaml
import pandas as pd

logger = logging.getLogger(__name__)


class BaseNormalizer(ABC):
    """
    Abstract base class for all normalizers.
    
    Provides:
    - Contract validation
    - Field mapping
    - Type conversion
    - Dead letter queue
    - CSV export for DINT
    """
    
    def __init__(self, contract_path: str = "src/contracts/obt_schema_v1.yaml"):
        self.contract = self._load_contract(contract_path)
        self.dead_letter_queue = []
        
        logger.info(f"{self.__class__.__name__} initialized")
    
    def _load_contract(self, path: str) -> Dict:
        """Load OBT schema contract"""
        with open(path, 'r') as f:
            contract = yaml.safe_load(f)
        
        logger.info(f"Loaded contract: {contract['schema_name']}")
        return contract
    
    @abstractmethod
    def parse(self, filepath: str) -> List[Dict]:
        """
        Parse source file and return list of records.
        Must be implemented by subclasses.
        """
        pass
    
    def normalize(
        self, 
        filepath: str, 
        source_type: str,
        batch_id: Optional[str] = None,
        output_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Main normalization pipeline.
        
        Steps:
        1. Parse source file
        2. Normalize each record (field mapping + type conversion)
        3. Export to CSV (for DINT)
        4. Return stats
        """
        
        logger.info(f"Starting normalization: {filepath}")
        
        if not batch_id:
            batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        if not output_path:
            output_path = f"src/outputs/normalized_{batch_id}.csv"
        
        # Step 1: Parse
        raw_records = self.parse(filepath)
        logger.info(f"Parsed {len(raw_records)} raw records")
        
        # Step 2: Normalize
        normalized_records = []
        
        for i, raw_record in enumerate(raw_records):
            try:
                normalized = self._normalize_record(
                    raw_record, 
                    source_type, 
                    batch_id,
                    record_num=i
                )
                normalized_records.append(normalized)
            
            except Exception as e:
                logger.error(f"Failed to normalize record {i}: {e}")
                self.dead_letter_queue.append({
                    'record': raw_record,
                    'error': str(e),
                    'batch_id': batch_id,
                    'record_num': i
                })
        
        # Step 3: Export to CSV
        if normalized_records:
            # Ensure all standard fields present (with empty strings if missing)
            for record in normalized_records:
                for field in self.contract['standard_fields']:
                    if field not in record:
                        record[field] = ''
            
            df = pd.DataFrame(normalized_records)
            df.to_csv(output_path, index=False)
            logger.info(f"✅ Exported {len(normalized_records)} records to: {output_path}")
        
        # Step 4: Stats
        stats = {
            'total_parsed': len(raw_records),
            'successfully_normalized': len(normalized_records),
            'failed': len(self.dead_letter_queue),
            'success_rate': len(normalized_records) / len(raw_records) if raw_records else 0,
            'batch_id': batch_id,
            'source_type': source_type,
            'output_file': output_path
        }
        
        logger.info(f"Normalization complete: {stats['success_rate']:.1%} success rate")
        
        return {
            'records': normalized_records,
            'stats': stats,
            'dead_letters': self.dead_letter_queue,
            'output_csv': output_path
        }
    
    def _normalize_record(
        self,
        raw_record: Dict,
        source_type: str,
        batch_id: str,
        record_num: int
    ) -> Dict:
        """Normalize a single record to OBT schema"""
        
        normalized = {}
        format_tag = self.__class__.__name__.lower().replace('normalizer', '')
        
        # 2. Generate the more descriptive record_id
        normalized['record_id'] = f"{batch_id}_{format_tag}_REC_{record_num:06d}"
        
        
        
        # Add metadata
        normalized['source_type'] = source_type
        normalized['batch_id'] = batch_id
        normalized['ingested_at'] = datetime.now().isoformat()
        
        # Map fields
        for obt_field in self.contract.get('field_types', {}).keys():
            value = self._extract_field(raw_record, obt_field)
            
            if value is not None:
                converted_value = self._convert_type(value, obt_field)
                normalized[obt_field] = converted_value
        
        # Validate required fields
        self._validate_required_fields(normalized)
        
        return normalized
    
    def _extract_field(self, raw_record: Dict, obt_field: str) -> Optional[Any]:
        """Extract field using mapping rules"""
        
        mappings = self.contract.get('field_mappings', {}).get(obt_field, [obt_field])
        
        for possible_name in mappings:
            if possible_name in raw_record:
                value = raw_record[possible_name]
                # Skip empty strings, None, NaN
                if value is not None and value != '' and str(value).lower() not in ['nan', 'none', 'null']:
                    return value
        
        return None
    
    def _convert_type(self, value: Any, field_name: str) -> Any:
        """Convert value to expected type"""
        
        if value is None or value == '':
            return None
        
        field_type = self.contract['field_types'].get(field_name)
        
        try:
            if field_type == 'integer':
                return int(float(value))  # Handle "5.0" → 5
            
            elif field_type == 'float':
                return float(value)
            
            elif field_type == 'boolean':
                if isinstance(value, bool):
                    return value
                return str(value).lower() in ['true', '1', 'yes', 'y']
            
            elif field_type in ['date', 'datetime']:
                return str(value)
            
            else:  # string or enum
                return str(value).strip('.0')
        
        except (ValueError, TypeError) as e:
            logger.warning(f"Type conversion failed for {field_name}={value}: {e}")
            return str(value)
    
    def _validate_required_fields(self, record: Dict) -> None:
        """Ensure all required fields are present"""
        
        required = self.contract.get('required_fields', [])
        missing = [f for f in required if f not in record or not record[f]]
        
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
    
    def export_dead_letters(self, filepath: str) -> None:
        """Export failed records for manual review"""
        
        import json
        
        with open(filepath, 'w') as f:
            json.dump(self.dead_letter_queue, f, indent=2, default=str)
        
        logger.info(f"Exported {len(self.dead_letter_queue)} dead letters to {filepath}")