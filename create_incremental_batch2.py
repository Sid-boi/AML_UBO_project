#!/usr/bin/env python3
"""
Create Incremental Batch 2
===========================

This creates a second batch with:
1. NEW persons (never seen before)
2. NEW companies (never seen before)
3. UPDATES to existing persons (Mary Baker from different source)
4. NEW ownership relationships
5. Some PEPs and high-risk entities

This will test:
- Entity matching across batches
- Incremental graph growth
- Cross-source entity resolution
"""

import pandas as pd
from datetime import datetime
import random

def create_batch2():
    
    print("="*70)
    print("CREATING INCREMENTAL BATCH 2")
    print("="*70)
    
    batch2_records = []
    batch_id = "BATCH_20260301_120000"
    timestamp = datetime.now().isoformat()
    
    # ════════════════════════════════════════════════════════════
    # 1. UPDATE TO EXISTING ENTITY (Mary Baker from new source)
    # ════════════════════════════════════════════════════════════
    # This tests cross-source matching!
    
    print("\n1️⃣  Creating UPDATE records (existing entities from new source)")
    
    # Mary Baker - already in batch 1, now from "xml_companies_house" source
    batch2_records.append({
        'record_id': 'REC_BATCH2_001',
        'source_type': 'xml_companies_house',  # NEW SOURCE!
        'batch_id': batch_id,
        'ingested_at': timestamp,
        'entity_id': 'CH_PERSON_MB_001',  # Different ID format
        'entity_type': 'person',
        'full_name': 'Mary Baker',  # Same name
        'dob': '1960-05-18',  # Same DOB
        'nationality': 'Chinese',
        'is_pep': False,
        'address': '769 Queen Street, Beijing, China',  # Slightly different
        'city': 'Beijing',
        'country': 'China',
        'director_of_entity_ids': 'CH_COMPANY_001;CH_COMPANY_002',  # New directorships
        'pep_level': '',
        'company_number': '',
        'date_incorporated': '',
        'employee_count': '',
        'is_offshore': '',
        'owned_by_entity_id': '',
        'ownership_percentage': ''
    })
    
    print("   ✅ Mary Baker (from Companies House - should merge!)")
    
    # ════════════════════════════════════════════════════════════
    # 2. NEW PERSONS (never seen before)
    # ════════════════════════════════════════════════════════════
    
    print("\n2️⃣  Creating NEW person records")
    
    new_persons = [
        {
            'name': 'Alexandra Chen',
            'dob': '1975-11-23',
            'nationality': 'Singaporean',
            'is_pep': True,
            'pep_level': 'diplomat',
            'address': '88 Marina Bay, Singapore',
            'city': 'Singapore',
            'country': 'Singapore',
            'director_of': 'CH_COMPANY_003'
        },
        {
            'name': 'David O\'Connor',
            'dob': '1982-03-17',
            'nationality': 'Irish',
            'is_pep': False,
            'pep_level': '',
            'address': '12 St Stephen Green, Dublin, Ireland',
            'city': 'Dublin',
            'country': 'Ireland',
            'director_of': 'CH_COMPANY_004;CH_COMPANY_005'
        },
        {
            'name': 'Elena Volkov',
            'dob': '1968-09-05',
            'nationality': 'Russian',
            'is_pep': True,
            'pep_level': 'minister',
            'address': '45 Red Square, Moscow, Russia',
            'city': 'Moscow',
            'country': 'Russia',
            'director_of': ''
        }
    ]
    
    for i, person in enumerate(new_persons, start=2):
        batch2_records.append({
            'record_id': f'REC_BATCH2_00{i}',
            'source_type': 'xml_companies_house',
            'batch_id': batch_id,
            'ingested_at': timestamp,
            'entity_id': f'CH_PERSON_{i:03d}',
            'entity_type': 'person',
            'full_name': person['name'],
            'dob': person['dob'],
            'nationality': person['nationality'],
            'is_pep': person['is_pep'],
            'address': person['address'],
            'city': person['city'],
            'country': person['country'],
            'director_of_entity_ids': person['director_of'],
            'pep_level': person['pep_level'],
            'company_number': '',
            'date_incorporated': '',
            'employee_count': '',
            'is_offshore': '',
            'owned_by_entity_id': '',
            'ownership_percentage': ''
        })
        print(f"   ✅ {person['name']}" + (" (PEP!)" if person['is_pep'] else ""))
    
    # ════════════════════════════════════════════════════════════
    # 3. NEW COMPANIES (with various risk levels)
    # ════════════════════════════════════════════════════════════
    
    print("\n3️⃣  Creating NEW company records")
    
    new_companies = [
        {
            'name': 'Global Consulting Partners Ltd',
            'company_number': '12345678',
            'date_incorporated': '2020-05-15',
            'employees': 150,
            'offshore': False,
            'city': 'London',
            'country': 'United Kingdom',
            'owned_by': 'CH_PERSON_002',  # David O'Connor
            'ownership_pct': 60.0
        },
        {
            'name': 'Offshore Holdings BVI',
            'company_number': 'BVI789456',
            'date_incorporated': '2018-03-22',
            'employees': 1,  # SHELL COMPANY!
            'offshore': True,
            'city': 'Road Town',
            'country': 'British Virgin Islands',
            'owned_by': 'CH_PERSON_003',  # Elena Volkov (PEP!)
            'ownership_pct': 100.0
        },
        {
            'name': 'TechVentures Singapore Pte Ltd',
            'company_number': 'SG202300456',
            'date_incorporated': '2023-01-10',
            'employees': 25,
            'offshore': False,
            'city': 'Singapore',
            'country': 'Singapore',
            'owned_by': 'CH_PERSON_001',  # Mary Baker
            'ownership_pct': 45.0
        },
        {
            'name': 'Caribbean Trading Corp',
            'company_number': 'KY998877',
            'date_incorporated': '2015-08-30',
            'employees': 0,  # SHELL COMPANY!
            'offshore': True,
            'city': 'George Town',
            'country': 'Cayman Islands',
            'owned_by': '',
            'ownership_pct': 0
        },
        {
            'name': 'European Energy Solutions GmbH',
            'company_number': 'DE456789012',
            'date_incorporated': '2019-11-12',
            'employees': 500,
            'offshore': False,
            'city': 'Berlin',
            'country': 'Germany',
            'owned_by': 'CH_PERSON_002',
            'ownership_pct': 35.0
        }
    ]
    
    for i, company in enumerate(new_companies, start=1):
        batch2_records.append({
            'record_id': f'REC_BATCH2_C_{i:03d}',
            'source_type': 'xml_companies_house',
            'batch_id': batch_id,
            'ingested_at': timestamp,
            'entity_id': f'CH_COMPANY_{i:03d}',
            'entity_type': 'company',
            'full_name': company['name'],
            'dob': '',
            'nationality': '',
            'is_pep': '',
            'address': f"{random.randint(1,999)} Business Street, {company['city']}, {company['country']}",
            'city': company['city'],
            'country': company['country'],
            'director_of_entity_ids': '',
            'pep_level': '',
            'company_number': company['company_number'],
            'date_incorporated': company['date_incorporated'],
            'employee_count': company['employees'],
            'is_offshore': company['offshore'],
            'owned_by_entity_id': company['owned_by'],
            'ownership_percentage': company['ownership_pct'] if company['ownership_pct'] > 0 else ''
        })
        
        risk_flag = " 🚨 SHELL!" if (company['offshore'] and company['employees'] <= 2) else ""
        pep_owner = " (PEP owner!)" if company['owned_by'] == 'CH_PERSON_003' else ""
        print(f"   ✅ {company['name']}{risk_flag}{pep_owner}")
    
    # ════════════════════════════════════════════════════════════
    # 4. SAVE TO CSV
    # ════════════════════════════════════════════════════════════
    
    df_batch2 = pd.DataFrame(batch2_records)
    output_file = 'outputs/normalized_batch2.csv'
    df_batch2.to_csv(output_file, index=False)
    
    print("\n" + "="*70)
    print("✅ BATCH 2 CREATED!")
    print("="*70)
    print(f"File: {output_file}")
    print(f"Total records: {len(batch2_records)}")
    print(f"  - Persons: 4 (1 update to Mary Baker, 3 new)")
    print(f"  - Companies: 5 (all new)")
    print(f"  - PEPs: 2 (Alexandra Chen, Elena Volkov)")
    print(f"  - Shell companies: 2 (BVI, Cayman)")
    
    print("\n📊 EXPECTED RESULTS AFTER INCREMENTAL LOAD:")
    print("  Current: 64 entities (28 persons, 36 companies)")
    print("  After:   ~71 entities (31 persons, 40 companies)")
    print("           ^ Mary Baker should NOT duplicate (cross-source match!)")
    
    print("\n🎯 WHAT TO TEST:")
    print("  1. Mary Baker merges (same person, different source)")
    print("  2. New PEPs detected (Alexandra Chen, Elena Volkov)")
    print("  3. Shell companies flagged (BVI, Cayman)")
    print("  4. New ownership relationships created")
    print("  5. Total entity count increases by ~7 (not 9)")
    
    return output_file

if __name__ == "__main__":
    output = create_batch2()
    print(f"\n✅ Ready to load: {output}")
    print("\nNext step: Run incremental load script!")