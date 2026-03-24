# src/semi/csv_normalizer.py

import pandas as pd
from typing import List, Dict
import logging

from .base_normalizer import BaseNormalizer

logger = logging.getLogger(__name__)


class CSVNormalizer(BaseNormalizer):
    """
    CSV file normalizer with ARRAY EXPLOSION.
    
    Philosophy:
    - Semicolon-separated fields → Multiple records
    - One entity per record (1NF normalization)
    """
    
    def parse(self, filepath: str) -> List[Dict]:
        """Parse CSV with array explosion"""
        
        logger.info(f"Parsing CSV: {filepath}")
        
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
        except UnicodeDecodeError:
            logger.warning("UTF-8 failed, trying latin-1 encoding")
            df = pd.read_csv(filepath, encoding='latin-1')
        
        # Convert NaN to None
        df = df.where(pd.notnull(df), None)
        
        # Convert to list of dicts
        records = df.to_dict('records')
        
        # ✨ EXPLODE ARRAYS (NEW!)
        exploded = []
        for record in records:
            exploded_records = self._explode_arrays(record)
            exploded.extend(exploded_records)
        
        logger.info(
            f"Parsed {len(records)} CSV rows → "
            f"{len(exploded)} exploded records"
        )
        
        return exploded
    
    def _explode_arrays(self, record: Dict) -> List[Dict]:
        """Explode semicolon-separated fields with UNIQUE record IDs"""
        
        # Check for directorship field
        director_of = record.get('director_of_entity_ids')
        
        if not director_of or pd.isna(director_of):
            return [record]
        
        # Parse semicolon-separated
        company_ids = [cid.strip() for cid in str(director_of).split(';') if cid.strip()]
        
        if not company_ids:
            record_copy = record.copy()
            record_copy['director_of_entity_ids'] = None
            return [record_copy]
        
        # ✨ EXPLODE: One record per company with UNIQUE IDs
        exploded_records = []
        base_record_id = record.get('record_id')
        
        for idx, company_id in enumerate(company_ids):
            # Clone base record
            exploded_record = record.copy()
            
            # ✅ FIX: Generate unique record_id for each exploded record
            if idx == 0:
                # First record keeps original ID
                exploded_record['record_id'] = base_record_id
            else:
                # Subsequent records get suffixed IDs
                exploded_record['record_id'] = f"{base_record_id}_{idx:02d}"
            
            # Add single directorship
            exploded_record['director_of_entity_ids'] = company_id
            
            exploded_records.append(exploded_record)
        
        logger.debug(
            f"Exploded {base_record_id} ({record.get('full_name', 'Unknown')}) into "
            f"{len(exploded_records)} records with unique IDs"
        )
        
        return exploded_records