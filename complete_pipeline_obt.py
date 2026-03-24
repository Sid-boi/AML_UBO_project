#!/usr/bin/env python3
"""
Complete SEMI → DINT → RDF pipeline test with synthetic data (100 entities)
"""

from src.semi.csv_normalizer import CSVNormalizer
from src.semi.xml_normalizer import XMLNormalizer
from src.semi.json_normalizer import JSONNormalizer
from src.dint.ubo_graph import UBOGraph
import pandas as pd
import networkx as nx


def test_synthetic_pipeline():
    """Test complete pipeline with 100 synthetic entities"""
    
    print("="*70)
    print("SYNTHETIC DATA PIPELINE TEST (100 ENTITIES)")
    print("="*70)
    
    # ═══════════════════════════════════════════════════════
    # STEP 1: SEMI - Normalize all formats
    # ═══════════════════════════════════════════════════════
    
    print("\n[STEP 1] SEMI - Normalizing CSV, XML, JSON...")
    
    all_records = []
    
    # CSV
    print("\n  [1.1] Processing CSV...")
    csv_norm = CSVNormalizer()
    csv_result = csv_norm.normalize(
        'src/data/synthetic/batch_synth_csv.csv',
        source_type='csv_synthetic',
        output_path='src/outputs/normalized_synth_csv.csv'
    )
    all_records.extend(csv_result['records'])
    print(f"  ✅ CSV: {len(csv_result['records'])} records")
    
    # XML
    print("\n  [1.2] Processing XML...")
    xml_norm = XMLNormalizer()
    xml_result = xml_norm.normalize(
        'src/data/synthetic/batch_synth_xml.xml',
        source_type='xml_synthetic',
        output_path='src/outputs/normalized_synth_xml.csv'
    )
    all_records.extend(xml_result['records'])
    print(f"  ✅ XML: {len(xml_result['records'])} records")
    
    # JSON
    print("\n  [1.3] Processing JSON...")
    json_norm = JSONNormalizer()
    json_result = json_norm.normalize(
        'src/data/synthetic/batch_synth_json.json',
        source_type='json_synthetic',
        output_path='src/outputs/normalized_synth_json.csv'
    )
    all_records.extend(json_result['records'])
    print(f"  ✅ JSON: {len(json_result['records'])} records")
    
    # Combined output
    print(f"\n  📊 Total normalized records: {len(all_records)}")
    
    df_combined = pd.DataFrame(all_records)
    df_combined.to_csv('src/outputs/normalized_synthetic_combined.csv', index=False)
    print(f"  ✅ Saved to: src/outputs/normalized_synthetic_combined.csv")
    
    # # ═══════════════════════════════════════════════════════
    # # STEP 2: DINT - Entity Resolution & Graph Building
    # # ═══════════════════════════════════════════════════════
    
    print("\n[STEP 2] DINT - Entity Resolution & Graph Building...")
    print("  (This may take 1-2 minutes for 100 entities)")
    
    graph = UBOGraph()
    graph.build_from_obt(all_records)
    
    print(f"\n  📊 Graph Statistics:")
    print(f"     Nodes: {graph.G.number_of_nodes()}")
    print(f"     Edges: {graph.G.number_of_edges()}")
    
    # ═══════════════════════════════════════════════════════
    # STEP 3: Analysis & Insights
    # ═══════════════════════════════════════════════════════
    
    print("\n[STEP 3] Analysis & Risk Detection...")
    
    # Count entity types
    persons = [n for n, d in graph.G.nodes(data=True) if d.get('entity_type') == 'person']
    companies = [n for n, d in graph.G.nodes(data=True) if d.get('entity_type') == 'company']
    
    print(f"\n  Entity Breakdown:")
    print(f"     Persons: {len(persons)}")
    print(f"     Companies: {len(companies)}")
    
    # Risk distribution
    risk_levels = {'HIGH': 0, 'MEDIUM': 0, 'LOW': 0, 'UNKNOWN': 0}
    for n, d in graph.G.nodes(data=True):
        level = d.get('risk_level', 'UNKNOWN')
        if level in risk_levels:
            risk_levels[level] += 1
        else:
            risk_levels['UNKNOWN'] += 1
    
    print(f"\n  Risk Distribution:")
    print(f"     HIGH: {risk_levels['HIGH']}")
    print(f"     MEDIUM: {risk_levels['MEDIUM']}")
    print(f"     LOW: {risk_levels['LOW']}")
    
    # PEP detection
    peps = [n for n, d in graph.G.nodes(data=True) if d.get('is_pep')]
    print(f"\n  🚨 PEPs Detected: {len(peps)}")
    if peps:
        for pep_id in peps[:5]:  # Show first 5
            pep_data = graph.G.nodes[pep_id]
            print(f"     - {pep_data.get('full_name', 'Unknown')}: {pep_data.get('pep_level', 'unknown')}")
        if len(peps) > 5:
            print(f"     ... and {len(peps) - 5} more")
    
    # Nominee directors
    nominees = [n for n, d in graph.G.nodes(data=True) if d.get('is_nominee_director')]
    print(f"\n  🚨 Nominee Directors: {len(nominees)}")
    if nominees:
        for nom_id in nominees:
            nom_data = graph.G.nodes[nom_id]
            director_count = nom_data.get('director_count', 0)
            print(f"     - {nom_data.get('full_name', 'Unknown')}: {director_count} directorships")
    
    # Offshore companies
    offshore = [n for n, d in graph.G.nodes(data=True) if d.get('is_offshore')]
    print(f"\n  🏝️  Offshore Companies: {len(offshore)}")
    if offshore:
        for off_id in offshore[:5]:  # Show first 5
            off_data = graph.G.nodes[off_id]
            print(f"     - {off_data.get('full_name', 'Unknown')} ({off_data.get('country', 'Unknown')})")
        if len(offshore) > 5:
            print(f"     ... and {len(offshore) - 5} more")
    
    # Circular ownership
    cycles = list(nx.simple_cycles(graph.G))
    if cycles:
        print(f"\n  ⚠️  Circular Ownership: {len(cycles)} cycles detected")
        for i, cycle in enumerate(cycles[:3], 1):  # Show first 3
            if len(cycle) <= 5:  # Only show small cycles
                names = [graph.G.nodes[n].get('full_name', 'Unknown') for n in cycle]
                print(f"     {i}. {' → '.join(names)} → {names[0]}")
        if len(cycles) > 3:
            print(f"     ... and {len(cycles) - 3} more cycles")
    
    # Cross-source matching
    multi_source = [n for n, d in graph.G.nodes(data=True) if d.get('source_count', 1) > 1]
    print(f"\n  🔗 Cross-Source Matches: {len(multi_source)} entities")
    if multi_source:
        # Show top matches by source count
        sorted_matches = sorted(
            [(n, graph.G.nodes[n]) for n in multi_source],
            key=lambda x: x[1].get('source_count', 0),
            reverse=True
        )
        for node_id, node_data in sorted_matches[:5]:
            count = node_data.get('source_count', 0)
            name = node_data.get('full_name', 'Unknown')
            print(f"     - {name}: {count} sources")
    
    # High-risk entities (detailed)
    high_risk = [n for n, d in graph.G.nodes(data=True) if d.get('risk_level') == 'HIGH']
    print(f"\n  🔴 High-Risk Entities: {len(high_risk)}")
    if high_risk:
        for entity_id in high_risk[:5]:
            entity_data = graph.G.nodes[entity_id]
            name = entity_data.get('full_name', 'Unknown')
            score = entity_data.get('risk_score', 0)
            factors = entity_data.get('risk_factors', [])
            
            print(f"     - {name}: score={score}")
            if factors and len(factors) > 0:
                for factor in factors[:2]:  # Show first 2 factors
                    if isinstance(factor, dict):
                        print(f"       • {factor.get('type', 'UNKNOWN')}: {factor.get('detail', '')}")
        if len(high_risk) > 5:
            print(f"     ... and {len(high_risk) - 5} more")
    
    # ═══════════════════════════════════════════════════════
    # STEP 4: Export Outputs
    # ═══════════════════════════════════════════════════════
    
    print("\n[STEP 4] Exporting Outputs...")
    
    # Save graph
    graph.save_graph('outputs/ubo_graph_synthetic.pkl')
    print("  ✅ Graph saved: outputs/ubo_graph_synthetic.pkl")
    
    # Export JSON
    graph.export_golden_records_json('outputs/golden_records_synthetic.json')
    print("  ✅ JSON exported: outputs/golden_records_synthetic.json")
    
    # Export RDF
    print("\n  [4.1] Exporting to RDF...")
    graph.export_rdf('outputs/ubo_graph_synthetic.ttl')
    print("  ✅ RDF exported: outputs/ubo_graph_synthetic.ttl")
    
    # ═══════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════
    
    print("\n" + "="*70)
    print("✅ PIPELINE COMPLETE")
    print("="*70)
    
    print(f"\n📊 Processing Summary:")
    print(f"  Input:")
    print(f"     - {len(all_records)} normalized records (CSV + XML + JSON)")
    print(f"  Graph:")
    print(f"     - {graph.G.number_of_nodes()} unique entities (after deduplication)")
    print(f"     - {graph.G.number_of_edges()} relationships")
    print(f"  Detection:")
    print(f"     - {len(peps)} PEPs")
    print(f"     - {len(nominees)} Nominee Directors")
    print(f"     - {len(offshore)} Offshore Companies")
    print(f"     - {len(high_risk)} High-Risk Entities")
    print(f"     - {len(cycles)} Circular Ownership Patterns")
    print(f"     - {len(multi_source)} Cross-Source Matches")
    
    print(f"\n📁 Output Files:")
    print(f"  SEMI (Normalized):")
    print(f"     - outputs/normalized_synth_csv.csv")
    print(f"     - outputs/normalized_synth_xml.csv")
    print(f"     - outputs/normalized_synth_json.csv")
    print(f"     - outputs/normalized_synthetic_combined.csv")
    print(f"  DINT (Golden Records):")
    print(f"     - outputs/ubo_graph_synthetic.pkl")
    print(f"     - outputs/golden_records_synthetic.json")
    print(f"  RDF (Knowledge Graph):")
    print(f"     - outputs/ubo_graph_synthetic.ttl")
    
    print("\n" + "="*70)
    print("🚀 Ready for Fuseki ingestion and SPARQL queries!")
    print("="*70)


if __name__ == "__main__":
    test_synthetic_pipeline()