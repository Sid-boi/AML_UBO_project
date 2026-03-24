#!/usr/bin/env python3
"""
Load Batch 2 Incrementally
===========================

This script:
1. Loads your existing graph (from batch 1)
2. Loads batch 2 data
3. Processes incrementally (entity matching!)
4. Exports updated graph as JSON and TTL
5. Shows before/after statistics
"""

import sys
sys.path.append('src')

import pandas as pd
import logging
from src.dint.ubo_graph import UBOGraph

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_graph_stats(graph, label):
    """Print detailed graph statistics"""
    print("\n" + "="*70)
    print(f"📊 GRAPH STATISTICS: {label}")
    print("="*70)
    
    # Entity counts
    persons = [n for n, d in graph.G.nodes(data=True) if d.get('entity_type') == 'person']
    companies = [n for n, d in graph.G.nodes(data=True) if d.get('entity_type') == 'company']
    
    print(f"\n🎯 ENTITIES:")
    print(f"   Total nodes:  {graph.G.number_of_nodes()}")
    print(f"   Persons:      {len(persons)}")
    print(f"   Companies:    {len(companies)}")
    print(f"   Total edges:  {graph.G.number_of_edges()}")
    
    # Cross-source matching
    cross_source = [n for n, d in graph.G.nodes(data=True) if d.get('source_count', 0) > 1]
    print(f"\n🔗 CROSS-SOURCE MATCHING:")
    print(f"   Multi-source entities: {len(cross_source)}")
    
    if cross_source:
        print(f"   Top cross-source entities:")
        # Sort by source count
        sorted_entities = sorted(
            [(n, graph.G.nodes[n]) for n in cross_source],
            key=lambda x: x[1].get('source_count', 0),
            reverse=True
        )[:5]
        
        for node_id, node_data in sorted_entities:
            sources = node_data.get('source_count', 0)
            name = node_data.get('full_name', 'Unknown')
            source_types = set()
            for rec in node_data.get('source_records', []):
                source_types.add(rec.get('source_type', 'unknown'))
            print(f"     - {name}: {sources} sources ({', '.join(source_types)})")
    
    # Risk analysis
    peps = [n for n, d in graph.G.nodes(data=True) if d.get('is_pep')]
    high_risk = [n for n, d in graph.G.nodes(data=True) if d.get('risk_level') == 'HIGH']
    shell_companies = [n for n, d in graph.G.nodes(data=True) 
                       if d.get('entity_type') == 'company' 
                       and d.get('is_offshore') 
                       and d.get('employee_count', 999) <= 2]
    
    print(f"\n⚠️  RISK ANALYSIS:")
    print(f"   PEPs:             {len(peps)}")
    print(f"   High-risk:        {len(high_risk)}")
    print(f"   Shell companies:  {len(shell_companies)}")
    
    if peps:
        print(f"\n   PEPs found:")
        for pep_id in peps:
            pep_data = graph.G.nodes[pep_id]
            print(f"     - {pep_data['full_name']}: {pep_data.get('pep_level', 'unknown')}")
    
    if shell_companies:
        print(f"\n   Shell companies:")
        for shell_id in shell_companies:
            shell_data = graph.G.nodes[shell_id]
            print(f"     - {shell_data['full_name']}: "
                  f"{shell_data.get('employee_count', 0)} employees, "
                  f"{shell_data.get('country', 'unknown')}")


def main():
    print("\n" + "="*70)
    print("INCREMENTAL BATCH LOADING - BATCH 2")
    print("="*70)
    
    # ════════════════════════════════════════════════════════════
    # STEP 1: Load existing graph
    # ════════════════════════════════════════════════════════════
    
    print("\n[STEP 1] Loading existing graph from batch 1...")
    
    graph = UBOGraph()
    
    try:
        graph.load_graph('outputs/ubo_graph_synthetic.pkl')
        print("✅ Loaded existing graph")
    except FileNotFoundError:
        print("❌ Graph file not found!")
        print("   Please run your pipeline first to create the initial graph:")
        print("   python test_complete_pipeline.py")
        return
    
    print_graph_stats(graph, "BEFORE Batch 2")
    
    # Save stats
    nodes_before = graph.G.number_of_nodes()
    edges_before = graph.G.number_of_edges()
    
    # ════════════════════════════════════════════════════════════
    # STEP 2: Load batch 2 data
    # ════════════════════════════════════════════════════════════
    
    print("\n[STEP 2] Loading batch 2 data...")
    
    try:
        df_batch2 = pd.read_csv('outputs/normalized_batch2.csv')
        print(f"✅ Loaded {len(df_batch2)} records from batch 2")
    except FileNotFoundError:
        print("❌ Batch 2 file not found!")
        print("   Create it first:")
        print("   python create_batch2_incremental.py")
        return
    
    # Convert to records
    df_batch2 = df_batch2.where(pd.notnull(df_batch2), None)
    batch2_records = df_batch2.to_dict('records')
    
    print(f"   Records: {len(batch2_records)}")
    persons = sum(1 for r in batch2_records if r.get('entity_type') == 'person')
    companies = sum(1 for r in batch2_records if r.get('entity_type') == 'company')
    print(f"   Persons: {persons}")
    print(f"   Companies: {companies}")
    
    # ════════════════════════════════════════════════════════════
    # STEP 3: Process incrementally
    # ════════════════════════════════════════════════════════════
    
    print("\n[STEP 3] Processing batch 2 incrementally...")
    print("   (This will match Mary Baker across sources!)")
    
    stats = graph.process_incremental_batch(batch2_records)
    
    print(f"\n✅ Incremental processing complete!")
    print(f"   New entities created:     {stats['new_entities']}")
    print(f"   Existing entities updated: {stats['updated_entities']}")
    print(f"   Duplicate sources skipped: {stats['duplicate_sources']}")
    
    # ════════════════════════════════════════════════════════════
    # STEP 4: Show updated statistics
    # ════════════════════════════════════════════════════════════
    
    print_graph_stats(graph, "AFTER Batch 2")
    
    # Calculate growth
    nodes_after = graph.G.number_of_nodes()
    edges_after = graph.G.number_of_edges()
    
    print("\n" + "="*70)
    print("📈 GROWTH ANALYSIS")
    print("="*70)
    print(f"   Nodes:  {nodes_before} → {nodes_after} (+{nodes_after - nodes_before})")
    print(f"   Edges:  {edges_before} → {edges_after} (+{edges_after - edges_before})")
    
    expected_new = len(batch2_records) - stats['updated_entities']
    actual_new = stats['new_entities']
    
    if actual_new == expected_new:
        print(f"\n   ✅ Entity matching working perfectly!")
        print(f"      Expected {expected_new} new entities (accounting for updates)")
        print(f"      Got {actual_new} new entities")
    else:
        print(f"\n   ⚠️  Some merging occurred:")
        print(f"      Expected max: {expected_new} new")
        print(f"      Actual: {actual_new} new")
        print(f"      Difference: {expected_new - actual_new} entities merged")
    
    # ════════════════════════════════════════════════════════════
    # STEP 5: Check Mary Baker cross-source match
    # ════════════════════════════════════════════════════════════
    
    print("\n" + "="*70)
    print("🔍 CROSS-SOURCE MATCHING CHECK: Mary Baker")
    print("="*70)
    
    mary_baker_nodes = [
        (node_id, node_data)
        for node_id, node_data in graph.G.nodes(data=True)
        if 'Mary' in node_data.get('full_name', '') and 'Baker' in node_data.get('full_name', '')
    ]
    
    if len(mary_baker_nodes) == 1:
        node_id, node_data = mary_baker_nodes[0]
        print(f"✅ SUCCESS! Mary Baker found as single entity:")
        print(f"   Golden ID: {node_id}")
        print(f"   Name: {node_data['full_name']}")
        print(f"   Source count: {node_data.get('source_count', 0)}")
        print(f"   Sources:")
        for rec in node_data.get('source_records', []):
            print(f"     - {rec.get('source_type', 'unknown')}: {rec.get('record_id', 'unknown')}")
        
        if node_data.get('source_count', 0) >= 2:
            print(f"\n   ✅ CROSS-SOURCE MATCH CONFIRMED!")
            print(f"      Mary Baker from 'csv_synthetic' + 'xml_companies_house'")
    elif len(mary_baker_nodes) > 1:
        print(f"⚠️  WARNING: Found {len(mary_baker_nodes)} Mary Baker entities!")
        print(f"   Should be 1 (merged across sources)")
        for node_id, node_data in mary_baker_nodes:
            print(f"   - {node_id}: {node_data.get('source_count', 0)} sources")
    else:
        print("❌ ERROR: Mary Baker not found!")
    
    # ════════════════════════════════════════════════════════════
    # STEP 6: Export updated graph
    # ════════════════════════════════════════════════════════════
    
    print("\n[STEP 6] Exporting updated graph...")
    
    # Save graph
    graph.save_graph('outputs/ubo_graph_batch2.pkl')
    print("✅ Saved: outputs/ubo_graph_batch2.pkl")
    
    # Export JSON
    graph.export_golden_records_json('outputs/golden_records_batch2.json')
    print("✅ Saved: outputs/golden_records_batch2.json")
    
    # Export RDF
    graph.export_rdf('outputs/ubo_graph_batch2.ttl')
    print("✅ Saved: outputs/ubo_graph_batch2.ttl")
    
    # ════════════════════════════════════════════════════════════
    # FINAL SUMMARY
    # ════════════════════════════════════════════════════════════
    
    print("\n" + "="*70)
    print("✅ INCREMENTAL LOADING COMPLETE!")
    print("="*70)
    print("\n📁 Output files:")
    print("   - outputs/ubo_graph_batch2.pkl")
    print("   - outputs/golden_records_batch2.json")
    print("   - outputs/ubo_graph_batch2.ttl  ← Load this into GraphDB!")
    
    print("\n🎯 NEXT STEPS:")
    print("   1. Open GraphDB Free")
    print("   2. Create repository (or use existing)")
    print("   3. Import: ubo_graph_batch2.ttl")
    print("   4. Run SPARQL queries")
    print("   5. Visualize the graph!")
    
    print("\n💡 KEY QUERIES TO TRY:")
    print("   - Find Mary Baker (should have 2 sources!)")
    print("   - Find all PEPs (should have 6 now)")
    print("   - Find shell companies (should have 2 new ones)")
    print("   - Find entities owned by PEPs")


if __name__ == "__main__":
    main()