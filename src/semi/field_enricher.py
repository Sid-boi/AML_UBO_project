# src/semi/field_enricher.py

from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class FieldEnricher:
    """
    Enriches records with missing fields using heuristics.
    """
    
    def __init__(self):
        self.enrichment_stats = {
            'fields_enriched': 0,
            'records_enriched': 0
        }
        
        # PEP detection keywords
        self.pep_keywords = {
            'minister': ['minister', 'secretary of state', 'cabinet'],
            'judge': ['judge', 'justice', 'magistrate'],
            'diplomat': ['ambassador', 'consul', 'diplomat'],
            'parliament_member': ['mp', 'member of parliament', 'senator'],
            'senior_official': ['director general', 'permanent secretary']
        }
    
    def enrich(self, record: Dict) -> Dict:
        """Enrich a single record"""
        
        enriched = record.copy()
        original_count = len([v for v in record.values() if v])
        
        if record.get('entity_type') == 'person':
            enriched = self._enrich_person(enriched)
        elif record.get('entity_type') == 'company':
            enriched = self._enrich_company(enriched)
        
        enriched = self._enrich_location(enriched)
        
        new_count = len([v for v in enriched.values() if v])
        if new_count > original_count:
            self.enrichment_stats['records_enriched'] += 1
            self.enrichment_stats['fields_enriched'] += (new_count - original_count)
        
        return enriched
    
    def _enrich_person(self, record: Dict) -> Dict:
        """Enrich person fields"""
        
        # Nationality from country
        if not record.get('nationality') and record.get('country'):
            record['nationality'] = self._country_to_nationality(record['country'])
        
        # Entity ID from name + DOB
        if not record.get('entity_id') and record.get('full_name'):
            name_part = record['full_name'].replace(' ', '_').upper()[:20]
            dob_part = record.get('dob', 'UNKNOWN')[:10].replace('-', '') if record.get('dob') else 'UNKNOWN'
            record['entity_id'] = f"ENT_{name_part}_{dob_part}"
        
        # PEP detection (if not already set)
        if record.get('is_pep') is None:
            record['is_pep'] = False  # Default
        
        return record
    
    def _enrich_company(self, record: Dict) -> Dict:
        """Enrich company fields"""
        
        # Offshore detection
        if record.get('is_offshore') is None and record.get('country'):
            record['is_offshore'] = self._is_tax_haven(record['country'])
        
        # Entity ID from company number
        if not record.get('entity_id') and record.get('company_number'):
            record['entity_id'] = f"ENT_CO_{record['company_number']}"
        
        return record
    
    def _enrich_location(self, record: Dict) -> Dict:
        """Enrich location fields"""
        
        # Normalize country
        if record.get('country'):
            record['country'] = self._normalize_country(record['country'])
        
        return record
    
    def _country_to_nationality(self, country: str) -> str:
        """Convert country to nationality"""
        mappings = {
            'United Kingdom': 'British',
            'UK': 'British',
            'United States': 'American',
            'USA': 'American',
            'Germany': 'German',
            'France': 'French',
            'Netherlands': 'Dutch',
        }
        return mappings.get(country, country + 'an')
    
    def _is_tax_haven(self, country: str) -> bool:
        """Check if country is tax haven"""
        tax_havens = {
            'Cayman Islands', 'British Virgin Islands', 'BVI',
            'Bermuda', 'Panama', 'Jersey', 'Guernsey',
            'Isle of Man', 'Luxembourg', 'Switzerland',
            'Liechtenstein', 'Monaco', 'Bahamas'
        }
        return country in tax_havens
    
    def _normalize_country(self, country: str) -> str:
        """Normalize country names"""
        normalizations = {
            'UK': 'United Kingdom',
            'USA': 'United States',
            'US': 'United States',
        }
        return normalizations.get(country, country)
    
    def get_stats(self) -> Dict:
        """Get enrichment statistics"""
        return self.enrichment_stats