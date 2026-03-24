# src/semi/contract_validator.py

from typing import Any, Dict, List, Tuple, Optional
import re
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ContractValidator:
    """Validates records against OBT schema contract"""
    
    def __init__(self, contract: Dict):
        self.contract = contract
    
    def validate_batch(self, records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Validate batch of records.
        
        Returns:
        - valid_records
        - invalid_records (with errors)
        """
        
        valid_records = []
        invalid_records = []
        
        for i, record in enumerate(records):
            is_valid, errors = self.validate_record(record)
            
            if is_valid:
                valid_records.append(record)
            else:
                invalid_records.append({
                    'record': record,
                    'record_num': i,
                    'errors': errors
                })
        
        logger.info(f"Validation: {len(valid_records)}/{len(records)} passed")
        
        return valid_records, invalid_records
    
    def validate_record(self, record: Dict) -> Tuple[bool, List[str]]:
        """Validate single record"""
        
        errors = []
        
        # Check required fields
        for required_field in self.contract.get('required_fields', []):
            if required_field not in record or not record[required_field]:
                errors.append(f"Missing required field: {required_field}")
        
        # Validate each field
        for field_name, value in record.items():
            if value is None or value == '':
                continue
            
            # Get validation rules
            rules = [r for r in self.contract.get('validation_rules', []) 
                    if r['field'] == field_name]
            
            for rule in rules:
                if rule.get('optional') and (value is None or value == ''):
                    continue
                
                error = self._apply_rule(field_name, value, rule)
                if error:
                    errors.append(error)
        
        return len(errors) == 0, errors
    
    def _apply_rule(self, field_name: str, value: Any, rule: Dict) -> Optional[str]:
        """Apply validation rule"""
        
        rule_type = rule['rule']
        
        try:
            if rule_type == 'must_be_one_of':
                if value not in rule['values']:
                    return f"{field_name}='{value}' not in {rule['values']}"
            
            elif rule_type == 'min_length':
                if len(str(value)) < rule['value']:
                    return f"{field_name} too short (min {rule['value']})"
            
            elif rule_type == 'range':
                val_float = float(value)
                if not (rule['min'] <= val_float <= rule['max']):
                    return f"{field_name}={value} outside [{rule['min']}, {rule['max']}]"
            
            elif rule_type == 'min_value':
                if float(value) < rule['value']:
                    return f"{field_name}={value} below min {rule['value']}"
            
            elif rule_type == 'pattern':
                if not re.match(rule['value'], str(value)):
                    return f"{field_name}='{value}' doesn't match {rule['value']}"
        
        except Exception as e:
            return f"Validation error for {field_name}: {e}"
        
        return None