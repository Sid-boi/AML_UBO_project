#!/usr/bin/env python3
"""
Generate realistic synthetic UBO data for testing.

Creates 50-100 entities with:
- Complex ownership chains
- Multiple PEPs
- Nominee directors
- Offshore structures
- Circular ownership
"""

import random
import json
import csv
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET
from xml.dom import minidom
import os

# ════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════

PERSON_COUNT = 40  # 40 persons
COMPANY_COUNT = 60  # 60 companies
TOTAL = 100  # Total entities

# ════════════════════════════════════════════════════════════
# REALISTIC DATA POOLS
# ════════════════════════════════════════════════════════════

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Mohammed", "Ahmed", "Fatima", "Ali",
    "Yuki", "Sakura", "Wei", "Li", "Dmitri", "Natasha", "Carlos", "Maria",
    "Giovanni", "Francesca", "Hans", "Greta", "Pierre", "Sophie", "Lars", "Emma"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Wilson", "Anderson", "Taylor", "Thomas", "Moore", "Jackson",
    "Martin", "Lee", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez",
    "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott",
    "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams", "Nelson", "Baker"
]

COMPANY_TYPES = [
    "Holdings Ltd", "Investments Inc", "Capital Partners", "Ventures Ltd", 
    "Group Plc", "Corporation", "Enterprises", "Solutions Ltd", "Technologies Inc",
    "Services Limited", "Trading Co", "Management Ltd", "Financial Services",
    "Consulting Group", "Development Corp", "Resources Ltd", "Industries Plc"
]

COMPANY_PREFIXES = [
    "Global", "International", "Premier", "United", "Advanced", "Strategic",
    "Dynamic", "Innovative", "Superior", "Elite", "Paramount", "Apex",
    "Nexus", "Zenith", "Quantum", "Vertex", "Horizon", "Pinnacle"
]

COMPANY_SECTORS = [
    "Tech", "Finance", "Property", "Energy", "Healthcare", "Retail",
    "Manufacturing", "Logistics", "Media", "Telecom", "Mining", "Agriculture"
]

UK_CITIES = [
    "London", "Manchester", "Birmingham", "Leeds", "Liverpool", "Edinburgh",
    "Glasgow", "Bristol", "Newcastle", "Cardiff", "Cambridge", "Oxford"
]

OFFSHORE_LOCATIONS = [
    ("George Town", "Cayman Islands"),
    ("Road Town", "British Virgin Islands"),
    ("Douglas", "Isle of Man"),
    ("St. Helier", "Jersey"),
    ("Hamilton", "Bermuda"),
    ("Panama City", "Panama"),
    ("Luxembourg City", "Luxembourg"),
    ("Nassau", "Bahamas"),
    ("Singapore", "Singapore"),
    ("Dubai", "United Arab Emirates")
]

PEP_LEVELS = [
    "diplomat", "judge", "parliament_member", "minister", 
    "central_bank", "military", "state_enterprise"
]

# ════════════════════════════════════════════════════════════
# GENERATOR FUNCTIONS
# ════════════════════════════════════════════════════════════

def random_date(start_year=1950, end_year=2000):
    """Generate random date of birth"""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")

def random_company_number():
    """Generate realistic UK company number"""
    return str(random.randint(10000000, 99999999))

def generate_person(person_id):
    """Generate a realistic person"""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    dob = random_date(1950, 1995)
    
    # 15% chance of being PEP
    is_pep = random.random() < 0.15
    pep_level = random.choice(PEP_LEVELS) if is_pep else None
    
    # UK or international
    if random.random() < 0.7:  # 70% UK
        city = random.choice(UK_CITIES)
        country = "United Kingdom"
        nationality = "British"
    else:  # 30% international
        nationalities = ["American", "German", "French", "Russian", "Chinese", "Indian"]
        nationality = random.choice(nationalities)
        city = random.choice(["New York", "Berlin", "Paris", "Moscow", "Beijing", "Mumbai"])
        country = random.choice(["USA", "Germany", "France", "Russia", "China", "India"])
    
    address = f"{random.randint(1, 999)} {random.choice(['High', 'Main', 'Park', 'King', 'Queen'])} Street, {city}, {country}"
    
    return {
        "record_id": f"REC_SYNTH_P_{person_id:03d}",
        "entity_id": f"ENT_PERSON_{person_id:03d}",
        "entity_type": "person",
        "full_name": f"{first} {last}",
        "dob": dob,
        "address": address,
        "nationality": nationality,
        "city": city,
        "country": country,
        "is_pep": is_pep,
        "pep_level": pep_level
    }

def generate_company(company_id, is_offshore=False):
    """Generate a realistic company"""
    
    if is_offshore:
        city, country = random.choice(OFFSHORE_LOCATIONS)
        name = f"{random.choice(COMPANY_PREFIXES)} {random.choice(['Holdings', 'Investments', 'Ventures', 'Capital'])} Inc"
        company_number = f"OFF{random.randint(10000, 99999)}"
        employee_count = random.choice([0, 1, 2])  # Shell companies
    else:
        sector = random.choice(COMPANY_SECTORS)
        prefix = random.choice(COMPANY_PREFIXES)
        suffix = random.choice(COMPANY_TYPES)
        name = f"{prefix} {sector} {suffix}"
        
        city = random.choice(UK_CITIES)
        country = "United Kingdom"
        company_number = random_company_number()
        employee_count = random.choice([5, 10, 15, 25, 50, 100, 250, 500])
    
    address = f"{random.randint(1, 999)} Business Park, {city}, {country}"
    date_inc = random_date(2000, 2023)
    
    return {
        "record_id": f"REC_SYNTH_C_{company_id:03d}",
        "entity_id": f"ENT_COMPANY_{company_id:03d}",
        "entity_type": "company",
        "full_name": name,
        "address": address,
        "city": city,
        "country": country,
        "company_number": company_number,
        "date_incorporated": date_inc,
        "employee_count": employee_count,
        "is_offshore": is_offshore
    }

# ════════════════════════════════════════════════════════════
# RELATIONSHIP GENERATORS
# ════════════════════════════════════════════════════════════

def generate_ownership_relationships(persons, companies):
    """Create realistic ownership structure"""
    relationships = []
    
    # Strategy 1: Simple ownership (60% of companies)
    for i, company in enumerate(companies[:int(len(companies) * 0.6)]):
        owner = random.choice(persons)
        percentage = random.choice([25, 30, 40, 51, 60, 75, 80, 100])
        
        relationships.append({
            "owner_id": owner["entity_id"],
            "company_id": company["entity_id"],
            "percentage": percentage
        })
    
    # Strategy 2: Multi-level ownership (create holding structures)
    # 20% of companies owned by other companies
    holding_companies = random.sample(companies, int(len(companies) * 0.2))
    target_companies = random.sample(companies, int(len(companies) * 0.2))
    
    for target in target_companies:
        holder = random.choice(holding_companies)
        if holder != target:  # Avoid self-ownership
            percentage = random.choice([50, 60, 75, 80, 100])
            relationships.append({
                "owner_id": holder["entity_id"],
                "company_id": target["entity_id"],
                "percentage": percentage
            })
    
    # Strategy 3: Create 3-5 circular ownership pairs
    for _ in range(random.randint(3, 5)):
        c1, c2 = random.sample(companies, 2)
        relationships.append({
            "owner_id": c1["entity_id"],
            "company_id": c2["entity_id"],
            "percentage": 50
        })
        relationships.append({
            "owner_id": c2["entity_id"],
            "company_id": c1["entity_id"],
            "percentage": 50
        })
    
    return relationships

def generate_directorship_relationships(persons, companies):
    """Create realistic directorship structure"""
    relationships = []
    
    # Strategy 1: Most persons are directors of 1-3 companies
    for person in persons:
        num_directorships = random.choices([0, 1, 2, 3], weights=[10, 50, 30, 10])[0]
        if num_directorships > 0:
            target_companies = random.sample(companies, min(num_directorships, len(companies)))
            for company in target_companies:
                relationships.append({
                    "person_id": person["entity_id"],
                    "company_id": company["entity_id"]
                })
    
    # Strategy 2: Create 3-5 nominee directors (10-20 directorships each)
    nominee_count = random.randint(3, 5)
    nominees = random.sample(persons, nominee_count)
    
    for nominee in nominees:
        num_directorships = random.randint(10, 20)
        target_companies = random.sample(companies, min(num_directorships, len(companies)))
        for company in target_companies:
            relationships.append({
                "person_id": nominee["entity_id"],
                "company_id": company["entity_id"]
            })
    
    return relationships

# ════════════════════════════════════════════════════════════
# EXPORT FUNCTIONS
# ════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════
# EXPORT FUNCTIONS (UPDATED)
# ════════════════════════════════════════════════════════════

def normalize_record_for_csv(record):
    """
    Ensure all records have the same fields for CSV export.
    Fill missing fields with None/empty string.
    """
    # Define ALL possible fields
    all_fields = [
        'record_id', 'entity_id', 'entity_type', 'full_name', 
        'dob', 'address', 'nationality', 'city', 'country',
        'is_pep', 'pep_level', 'director_of_entity_ids',
        'company_number', 'date_incorporated', 'employee_count', 'is_offshore',
        'owned_by_entity_id', 'ownership_percentage',
        'source', 'source_type', 'ingested_at'
    ]
    
    # Create normalized record
    normalized = {}
    for field in all_fields:
        normalized[field] = record.get(field, None)
    
    # Add metadata
    normalized['source'] = 'synthetic_data'
    normalized['source_type'] = 'csv_synthetic'
    normalized['ingested_at'] = datetime.now().isoformat()
    
    return normalized

def export_to_csv(data, filepath):
    """Export to CSV with unified schema"""
    if not data:
        return
    
    # Normalize all records
    normalized_data = [normalize_record_for_csv(record) for record in data]
    
    # Define field order
    fieldnames = [
        'record_id', 'entity_id', 'entity_type', 'full_name', 
        'dob', 'address', 'nationality', 'city', 'country',
        'is_pep', 'pep_level', 'director_of_entity_ids',
        'company_number', 'date_incorporated', 'employee_count', 'is_offshore',
        'owned_by_entity_id', 'ownership_percentage',
        'source', 'source_type', 'ingested_at'
    ]
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(normalized_data)
    
    print(f"✅ Exported {len(data)} records to {filepath}")

def export_to_json(data, filepath):
    """Export to JSON (already works fine)"""
    # Add metadata to each record
    for record in data:
        record['source'] = 'synthetic_data'
        record['source_type'] = 'json_synthetic'
        record['ingested_at'] = datetime.now().isoformat()
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)
    
    print(f"✅ Exported {len(data)} records to {filepath}")

def export_to_xml(data, filepath, root_name="records"):
    """Export to XML"""
    root = ET.Element(root_name)
    
    for item in data:
        # Add metadata
        item['source'] = 'synthetic_data'
        item['source_type'] = 'xml_synthetic'
        item['ingested_at'] = datetime.now().isoformat()
        
        record = ET.SubElement(root, "record")
        for key, value in item.items():
            if value is not None and value != '':  # ✅ Skip empty values
                child = ET.SubElement(record, key)
                child.text = str(value)
    
    # Pretty print
    xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="  ")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(xml_str)
    
    print(f"✅ Exported {len(data)} records to {filepath}")

# ════════════════════════════════════════════════════════════
# MAIN GENERATION LOGIC
# ════════════════════════════════════════════════════════════

def main():
    print("🏗️  Generating synthetic UBO dataset...")
    print(f"   Persons: {PERSON_COUNT}")
    print(f"   Companies: {COMPANY_COUNT}")
    print(f"   Total: {TOTAL}")
    print()
    
    # Create output directory
    os.makedirs("src/data/synthetic", exist_ok=True)
    
    # ═══════════════════════════════════════════════════════
    # STEP 1: Generate Entities
    # ═══════════════════════════════════════════════════════
    
    print("📝 Generating persons...")
    persons = [generate_person(i) for i in range(PERSON_COUNT)]
    
    print("🏢 Generating companies...")
    # 80% regular companies, 20% offshore
    regular_count = int(COMPANY_COUNT * 0.8)
    offshore_count = COMPANY_COUNT - regular_count
    
    companies = []
    for i in range(regular_count):
        companies.append(generate_company(i, is_offshore=False))
    for i in range(regular_count, COMPANY_COUNT):
        companies.append(generate_company(i, is_offshore=True))
    
    print(f"   Regular: {regular_count}, Offshore: {offshore_count}")
    
    # ═══════════════════════════════════════════════════════
    # STEP 2: Generate Relationships
    # ═══════════════════════════════════════════════════════
    
    print("🔗 Generating ownership relationships...")
    ownership_rels = generate_ownership_relationships(persons, companies)
    print(f"   Created {len(ownership_rels)} ownership relationships")
    
    print("👔 Generating directorship relationships...")
    directorship_rels = generate_directorship_relationships(persons, companies)
    print(f"   Created {len(directorship_rels)} directorship relationships")
    
    # ═══════════════════════════════════════════════════════
    # STEP 3: Combine into Full Records
    # ═══════════════════════════════════════════════════════
    
    print("🔨 Building full records with relationships...")
    
    # Add ownership info to companies
    for company in companies:
        owners = [r for r in ownership_rels if r["company_id"] == company["entity_id"]]
        if owners:
            # Pick primary owner
            primary = random.choice(owners)
            company["owned_by_entity_id"] = primary["owner_id"]
            company["ownership_percentage"] = primary["percentage"]
    
    # Add directorship info to persons
    for person in persons:
        directorships = [r for r in directorship_rels if r["person_id"] == person["entity_id"]]
        if directorships:
            company_ids = [r["company_id"] for r in directorships]
            person["director_of_entity_ids"] = ";".join(company_ids)
    
    # ═══════════════════════════════════════════════════════
    # STEP 4: Split Across Formats
    # ═══════════════════════════════════════════════════════
    
    print("📤 Exporting to multiple formats...")
    
    # Split persons: 40% CSV, 30% XML, 30% JSON
    persons_csv = persons[:int(PERSON_COUNT * 0.4)]
    persons_xml = persons[int(PERSON_COUNT * 0.4):int(PERSON_COUNT * 0.7)]
    persons_json = persons[int(PERSON_COUNT * 0.7):]
    
    # Split companies: 30% CSV, 40% XML, 30% JSON
    companies_csv = companies[:int(COMPANY_COUNT * 0.3)]
    companies_xml = companies[int(COMPANY_COUNT * 0.3):int(COMPANY_COUNT * 0.7)]
    companies_json = companies[int(COMPANY_COUNT * 0.7):]
    
    # Export
    export_to_csv(persons_csv + companies_csv, "src/data/synthetic/batch_synth_csv.csv")
    export_to_xml(persons_xml + companies_xml, "src/data/synthetic/batch_synth_xml.xml")
    export_to_json(persons_json + companies_json, "src/data/synthetic/batch_synth_json.json")
    
    # ═══════════════════════════════════════════════════════
    # STEP 5: Statistics
    # ═══════════════════════════════════════════════════════
    
    print()
    print("📊 DATASET STATISTICS:")
    print(f"   Total entities: {len(persons) + len(companies)}")
    print(f"   Persons: {len(persons)}")
    print(f"   Companies: {len(companies)}")
    print(f"   PEPs: {sum(1 for p in persons if p.get('is_pep'))}")
    print(f"   Offshore companies: {sum(1 for c in companies if c.get('is_offshore'))}")
    print(f"   Ownership relationships: {len(ownership_rels)}")
    print(f"   Directorship relationships: {len(directorship_rels)}")
    print()
    print("✅ Synthetic data generation complete!")

if __name__ == "__main__":
    main()