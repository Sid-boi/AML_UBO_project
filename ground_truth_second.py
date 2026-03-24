# generate_ground_truth.py

import pandas as pd
import json

# Load normalized CSV
df = pd.read_csv('src/outputs/normalized_combined.csv')

# Load golden records to verify
with open('outputs/golden_records_debug_evaluated.json') as f:
    golden_data = json.load(f)

print("="*70)
print("GROUND TRUTH GENERATOR")
print("="*70)

# Group by entity_id or name patterns
print("\n# Alice Johnson records:")
alice_records = df[df['full_name'].str.contains('Alice', na=False, case=False)]
print("ENTITY_ALICE = {")
for rid in alice_records['record_id'].values:
    print(f'    "{rid}",')
print("}")

print("\n# Bob Wilson / Robert Wilson records:")
bob_records = df[df['full_name'].str.contains('Bob|Robert Wilson', na=False, case=False, regex=True)]
print("ENTITY_BOB = {")
for rid in bob_records['record_id'].values:
    print(f'    "{rid}",')
print("}")

print("\n# Charlie Brown / C. Brown records:")
charlie_records = df[df['full_name'].str.contains('Charlie|C. Brown', na=False, case=False, regex=True)]
print("ENTITY_CHARLIE = {")
for rid in charlie_records['record_id'].values:
    print(f'    "{rid}",')
print("}")

print("\n# Sarah Nominee records:")
sarah_records = df[df['full_name'].str.contains('Sarah', na=False, case=False)]
print("ENTITY_SARAH = {")
for rid in sarah_records['record_id'].values:
    print(f'    "{rid}",')
print("}")

print("\n# TechStart records:")
tech_records = df[df['full_name'].str.contains('TechStart', na=False, case=False)]
print("ENTITY_TECHSTART = {")
for rid in tech_records['record_id'].values:
    print(f'    "{rid}",')
print("}")

print("\n# DataCorp records:")
data_records = df[df['full_name'].str.contains('DataCorp', na=False, case=False)]
print("ENTITY_DATACORP = {")
for rid in data_records['record_id'].values:
    print(f'    "{rid}",')
print("}")

print("\n# GreenEnergy records:")
green_records = df[df['full_name'].str.contains('GreenEnergy', na=False, case=False)]
print("ENTITY_GREENENERGY = {")
for rid in green_records['record_id'].values:
    print(f'    "{rid}",')
print("}")

print("\n# FinTech records:")
fintech_records = df[df['full_name'].str.contains('FinTech', na=False, case=False)]
print("ENTITY_FINTECH = {")
for rid in fintech_records['record_id'].values:
    print(f'    "{rid}",')
print("}")

print("\n# Property Investments records:")
prop_records = df[df['full_name'].str.contains('Property', na=False, case=False)]
print("ENTITY_PROPERTY = {")
for rid in prop_records['record_id'].values:
    print(f'    "{rid}",')
print("}")

print("\n# Global Holdings records:")
global_records = df[df['full_name'].str.contains('Global Holdings', na=False, case=False)]
print("ENTITY_GLOBAL_HOLDINGS = {")
for rid in global_records['record_id'].values:
    print(f'    "{rid}",')
print("}")

# Single-source entities
print("\n# Single-source entities:")
for name in ['Diana Prince', 'Edward Thompson', 'Frank Miller', 'Henry Thompson', 
             'Grace Lee', 'Ivan Petrov', 'Offshore Ventures', 'BVI Holdings', 
             'Blockchain', 'Circular Company A', 'Circular Company B']:
    records = df[df['full_name'].str.contains(name, na=False, case=False, regex=False)]
    if len(records) > 0:
        entity_name = name.upper().replace(' ', '_')
        print(f"\nENTITY_{entity_name} = {{")
        for rid in records['record_id'].values:
            print(f'    "{rid}",')
        print("}")