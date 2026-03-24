#!/usr/bin/env python3
"""
Batch 3 Workflow: Offshore Leak Data
====================================
 
This demonstrates:
1. JSON parsing (array explosion!)
2. Incremental loading
3. Circular ownership detection
4. PEP + offshore risk patterns
 
HIGH RISK PATTERNS IN THIS BATCH:
- Dmitri Petrov (PEP) → 100% owns → Offshore shell (BVI)
- Ahmed Al-Mansouri (PEP) → 100% owns → Offshore shell (UAE)
- CIRCULAR: Company 7 → Company 8 → Company 9 → Company 7 (!)
- Multi-layer: Petrov → BVI → Cyprus → Cayman (3 levels)
"""
 
import sys

 
import pandas as pd
import logging
from src.semi.json_normalizer import JSONNormalizer
from src.dint.ubo_graph import UBOGraph
 
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
 
 
def print_separator(title):
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)
 
 
def main():
    print_separator("BATCH 3: OFFSHORE LEAK DATA")
    
    # ════════════════════════════════════════════════════════
    # STEP 1: Normalize JSON → CSV
    # ════════════════════════════════════════════════════════
    
    print("\n[STEP 1] Normalizing JSON data...")
    
    normalizer = JSONNormalizer()
    result = normalizer.normalize(
        filepath='src/data/synthetic/offshore_leak_batch3.json',
        source_type='json_offshore_leak',
        output_path='outputs/normalized_batch3.csv'
    )

    print(f"✅ Normalized {result['stats']['successfully_normalized']} records")
    print(f"   Output: {result['stats']['output_file']}")
    
    # Show sample
    df = pd.read_csv('outputs/normalized_batch3.csv')
    print(f"\n📊 Normalized data preview:")
    print(f"   Total rows: {len(df)}")
    print(f"   Columns: {list(df.columns)[:10]}...")
    
    # Count by type
    persons = sum(1 for _, row in df.iterrows() if row['entity_type'] == 'person')
    companies = sum(1 for _, row in df.iterrows() if row['entity_type'] == 'company')
    print(f"   Persons: {persons}")
    print(f"   Companies: {companies}")


      # ════════════════════════════════════════════════════════
    # STEP 2: Load existing graph
    # ════════════════════════════════════════════════════════
    
    print("\n[STEP 2] Loading existing graph (batch 1 + 2)...")
    
    graph = UBOGraph()
    graph.load_graph('outputs/ubo_graph_batch2.pkl')
    
    nodes_before = graph.G.number_of_nodes()
    edges_before = graph.G.number_of_edges()
    
    print(f"✅ Loaded graph")
    print(f"   Nodes: {nodes_before}")
    print(f"   Edges: {edges_before}")
    
    # ════════════════════════════════════════════════════════
    # STEP 3: Process batch 3 incrementally
    # ════════════════════════════════════════════════════════
    
    print("\n[STEP 3] Processing batch 3 incrementally...")
    
    # Convert to records
    df = df.where(pd.notnull(df), None)
    batch3_records = df.to_dict('records')
    
    print(f"   Loading {len(batch3_records)} records...")
    
    stats = graph.process_incremental_batch(batch3_records)
    
    print(f"\n✅ Incremental processing complete!")
    print(f"   New entities:     {stats['new_entities']}")
    print(f"   Updates:          {stats['updated_entities']}")
    print(f"   Duplicates:       {stats['duplicate_sources']}")
    
    # ════════════════════════════════════════════════════════
    # STEP 4: Analyze results
    # ════════════════════════════════════════════════════════
    
    print_separator("GRAPH ANALYSIS AFTER BATCH 3")
    
    nodes_after = graph.G.number_of_nodes()
    edges_after = graph.G.number_of_edges()
    
    print(f"\n📈 GROWTH:")
    print(f"   Nodes: {nodes_before} → {nodes_after} (+{nodes_after - nodes_before})")
    print(f"   Edges: {edges_before} → {edges_after} (+{edges_after - edges_before})")
    
    # PEPs
    peps = [n for n, d in graph.G.nodes(data=True) if d.get('is_pep')]
    print(f"\n👥 PEPs: {len(peps)}")
    new_peps = [n for n in peps if 'LEAK' in str(graph.G.nodes[n].get('source_record_ids', [''])[0])]
    if new_peps:
        print(f"   NEW from batch 3:")
        for pep_id in new_peps:
            pep_data = graph.G.nodes[pep_id]
            print(f"     - {pep_data['full_name']}: {pep_data.get('pep_level', 'unknown')}")
    
    # Shell companies
    shells = [n for n, d in graph.G.nodes(data=True) 
              if d.get('entity_type') == 'company' 
              and d.get('is_offshore') 
              and d.get('employee_count', 999) <= 2]
    print(f"\n🏢 Shell companies: {len(shells)}")
    
    # Circular ownership
    import networkx as nx
    cycles = list(nx.simple_cycles(graph.G))
    print(f"\n🔄 CIRCULAR OWNERSHIP: {len(cycles)} cycles detected!")
    
    if cycles:
        print(f"   Showing first 3 cycles:")
        for i, cycle in enumerate(cycles[:3], 1):
            names = [graph.G.nodes[n].get('full_name', 'Unknown') for n in cycle]
            print(f"   {i}. {' → '.join(names)} → {names[0]}")
    
    # ════════════════════════════════════════════════════════
    # STEP 5: Find HIGH RISK patterns
    # ════════════════════════════════════════════════════════
    
    print_separator("🚨 HIGH RISK PATTERNS DETECTED")
    
    # Pattern 1: PEP owns offshore shell
    print("\n1️⃣  PEP → Offshore Shell Company:")
    
    for node_id, node_data in graph.G.nodes(data=True):
        if node_data.get('is_pep'):
            # Check what they own
            for _, target, edge_data in graph.G.out_edges(node_id, data=True):
                if edge_data.get('relationship_type') == 'owns':
                    target_data = graph.G.nodes[target]
                    if target_data.get('is_offshore') and target_data.get('employee_count', 999) <= 2:
                        pct = edge_data.get('ownership_percentage', 0)
                        print(f"   🚨 {node_data['full_name']} ({node_data.get('pep_level')})")
                        print(f"      → {pct}% of {target_data['full_name']}")
                        print(f"      → {target_data.get('country')}, {target_data.get('employee_count', 0)} employees")
    
    # Pattern 2: Multi-layer offshore chains
    print("\n2️⃣  Multi-Layer Offshore Chains (3+ levels):")
    
    for node_id, node_data in graph.G.nodes(data=True):
        if node_data.get('entity_type') == 'person':
            # Find 3-hop ownership chains
            paths = []
            for target1 in graph.G.successors(node_id):
                for target2 in graph.G.successors(target1):
                    for target3 in graph.G.successors(target2):
                        if graph.G.nodes[target3].get('entity_type') == 'company':
                            paths.append([node_id, target1, target2, target3])
            
            if len(paths) >= 1:
                for path in paths[:2]:  # Show first 2
                    names = [graph.G.nodes[n].get('full_name', 'Unknown') for n in path]
                    print(f"   {names[0]} → {names[1]} → {names[2]} → {names[3]}")
    
    # Pattern 3: Circular ownership details
    if cycles:
        print("\n3️⃣  Circular Ownership Structures:")
        for i, cycle in enumerate(cycles[:2], 1):
            print(f"\n   Cycle {i}:")
            for j, node_id in enumerate(cycle):
                node_data = graph.G.nodes[node_id]
                next_node = cycle[(j + 1) % len(cycle)]
                
                # Find ownership percentage
                pct = None
                for _, target, edge_data in graph.G.out_edges(node_id, data=True):
                    if target == next_node and edge_data.get('relationship_type') == 'owns':
                        pct = edge_data.get('ownership_percentage', 0)
                
                print(f"     {node_data.get('full_name', 'Unknown')}")
                print(f"       ↓ {pct}%")
    
    # ════════════════════════════════════════════════════════
    # STEP 6: Export updated graph
    # ════════════════════════════════════════════════════════
    
    print_separator("EXPORTING FINAL GRAPH")
    
    graph.save_graph('outputs/ubo_graph_batch3.pkl')
    print("✅ Saved: outputs/ubo_graph_batch3.pkl")
    
    graph.export_golden_records_json('outputs/golden_records_batch3.json')
    print("✅ Saved: outputs/golden_records_batch3.json")
    
    graph.export_rdf('outputs/ubo_graph_batch3.ttl')
    print("✅ Saved: outputs/ubo_graph_batch3.ttl")
    
    # ════════════════════════════════════════════════════════
    # FINAL SUMMARY
    # ════════════════════════════════════════════════════════
    
    print_separator("✅ BATCH 3 COMPLETE!")
    
    print("\n🎯 KEY FINDINGS:")
    print(f"   - {len(new_peps)} NEW PEPs added (Dmitri Petrov, Ahmed Al-Mansouri)")
    print(f"   - {len(cycles)} circular ownership structures detected")
    print(f"   - Multiple PEP → offshore shell patterns")
    print(f"   - 3-layer ownership chains created")
    
    print("\n📁 FILES READY:")
    print("   - normalized_batch3.csv (normalized data)")
    print("   - outputs/ubo_graph_batch3.ttl (← Load into GraphDB!)")
    
    print("\n🎯 NEXT: Load into GraphDB and run AML queries!")


if __name__ == "__main__":
    main()
    