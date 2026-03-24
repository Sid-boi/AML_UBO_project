# src/semi/xml_normalizer.py

import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
import logging

from .base_normalizer import BaseNormalizer

logger = logging.getLogger(__name__)


class XMLNormalizer(BaseNormalizer):
    """
    XML file normalizer with ARRAY EXPLOSION.
    
    Philosophy:
    - One entity per record (1NF normalization)
    - Multiple <CompanyNumber> elements → Multiple records
    - Clean, simple DINT ingestion
    """
    
    def parse(self, filepath: str) -> List[Dict]:
        """Parse XML with array explosion"""
        
        logger.info(f"Parsing XML: {filepath}")
        
        tree = ET.parse(filepath)
        root = tree.getroot()
        
        records = []
        
        # Parse Person elements
        for person_elem in root.findall('.//Person'):
            person_records = self._parse_person_element(person_elem)
            records.extend(person_records)  # ← Note: extend, not append!
        
        # Parse Company elements
        for company_elem in root.findall('.//Company'):
            company_record = self._parse_company_element(company_elem)
            if company_record:
                records.append(company_record)
        
        logger.info(f"Parsed {len(records)} records from XML")
        
        return records
    
    def _parse_person_element(self, elem: ET.Element) -> List[Dict]:
        """
        Parse Person element with directorship explosion.
        
        Returns: List of records (one per directorship)
        """
        
        # Base person record
        base_record = {
            'entity_type': 'person',
            'full_name': self._get_text(elem, 'FullName'),
            'dob': self._get_text(elem, 'DateOfBirth'),
            'nationality': self._get_text(elem, 'Nationality'),
        }
        
        # PEP fields
        is_pep = self._get_text(elem, 'IsPEP')
        if is_pep:
            base_record['is_pep'] = is_pep.lower() in ['true', '1', 'yes']
        
        base_record['pep_level'] = self._get_text(elem, 'PEPLevel')
        base_record['pep_position'] = self._get_text(elem, 'Position')
        base_record['pep_start_date'] = self._get_text(elem, 'StartDate')
        base_record['pep_end_date'] = self._get_text(elem, 'EndDate')
        
        # Address
        address_elem = elem.find('.//Address')
        if address_elem is not None:
            address_parts = [
                self._get_text(address_elem, 'Line1'),
                self._get_text(address_elem, 'Line2'),
                self._get_text(address_elem, 'City'),
                self._get_text(address_elem, 'Country'),
            ]
            base_record['address'] = ', '.join(filter(None, address_parts))
            base_record['city'] = self._get_text(address_elem, 'City')
            base_record['country'] = self._get_text(address_elem, 'Country')
        
        # ✨ DIRECTORSHIP EXPLOSION (NEW!)
        director_of_elem = elem.find('.//DirectorOf')
        
        if director_of_elem is not None:
            # Get all company numbers
            company_numbers = [
                self._get_text(cn_elem, '.')  # Get text of element itself
                for cn_elem in director_of_elem.findall('.//CompanyNumber')
                if self._get_text(cn_elem, '.')
            ]
            
            if company_numbers:
                # ✨ EXPLODE: One record per company
                exploded_records = []
                
                for company_num in company_numbers:
                    # Clone base record
                    person_record = base_record.copy()
                    
                    # Add single directorship
                    person_record['director_of_entity_ids'] = f"ENT_CO_{company_num}"
                    
                    exploded_records.append(person_record)
                
                logger.debug(
                    f"Exploded {base_record['full_name']} into "
                    f"{len(exploded_records)} directorship records"
                )
                
                return exploded_records
        
        # No directorships, return base record only
        return [base_record] if base_record['full_name'] else []
    
    def _parse_company_element(self, elem: ET.Element) -> Optional[Dict]:
        """Parse Company element (no explosion needed)"""
        
        record = {
            'entity_type': 'company',
            'company_number': self._get_text(elem, 'CompanyNumber'),
            'full_name': self._get_text(elem, 'CompanyName'),
            'date_incorporated': self._get_text(elem, 'IncorporationDate'),
        }
        
        # Employee count
        emp_count = self._get_text(elem, 'EmployeeCount')
        if emp_count:
            try:
                record['employee_count'] = int(emp_count)
            except ValueError:
                pass
        
        # Offshore flag
        is_offshore = self._get_text(elem, 'IsOffshore')
        if is_offshore:
            record['is_offshore'] = is_offshore.lower() in ['true', '1', 'yes']
        
        # Address
        address_elem = elem.find('.//Address')
        if address_elem is not None:
            address_parts = [
                self._get_text(address_elem, 'Line1'),
                self._get_text(address_elem, 'Line2'),
                self._get_text(address_elem, 'City'),
                self._get_text(address_elem, 'Country'),
            ]
            record['address'] = ', '.join(filter(None, address_parts))
            record['city'] = self._get_text(address_elem, 'City')
            record['country'] = self._get_text(address_elem, 'Country')
        
        # Ownership
        owned_by_elem = elem.find('.//OwnedBy')
        if owned_by_elem is not None:
            entity_id = self._get_text(owned_by_elem, 'EntityID')
            if entity_id:
                record['owned_by_entity_id'] = entity_id
            
            percentage = self._get_text(owned_by_elem, 'Percentage')
            if percentage:
                try:
                    record['ownership_percentage'] = float(percentage)
                except ValueError:
                    pass
        
        return record if record['full_name'] else None
    
    def _get_text(self, elem: ET.Element, tag: str) -> Optional[str]:
        """Safely get text from XML element"""
        
        if tag == '.':
            # Get text of element itself
            return elem.text.strip() if elem.text else None
        
        child = elem.find(tag)
        if child is not None and child.text:
            return child.text.strip()
        return None