import logging
from src.dint.ubo_graph import UBOGraph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_incremental():
    print("="*70)
    print("INCREMENTAL LOADING TEST")
    print("="*70)
    
    # Load existing graph
    graph = UBOGraph()
    graph.load_graph('outputs/ubo_graph_evaluated.pkl')
    
    print(f"\n📊 BEFORE INCREMENTAL UPDATE:")
    print(f"   Nodes: {graph.G.number_of_nodes()}")
    print(f"   Edges: {graph.G.number_of_edges()}")
    
    # Load incremental batch
    import pandas as pd
    df = pd.read_csv('test.csv')
    new_records = df.to_dict('records')
    
    print(f"\n📥 LOADING {len(new_records)} NEW RECORDS...")
    
    # Process incremental batch
    stats = graph.process_incremental_batch(new_records)
    
    print(f"\n📊 AFTER INCREMENTAL UPDATE:")
    print(f"   Nodes: {graph.G.number_of_nodes()}")
    print(f"   Edges: {graph.G.number_of_edges()}")
    print(f"\n📈 STATS:")
    print(f"   New entities: {stats['new_entities']}")
    print(f"   Updated entities: {stats['updated_entities']}")
    
    # Save updated graph
    graph.save_graph('outputs/ubo_graph.pkl')
    graph.export_golden_records_json(f'outputs/golden_records_debug_evaluated.json')
    
    # Export to RDF
    print(f"\n💾 EXPORTING TO RDF...")
    graph.export_rdf('outputs/ubo_graph.ttl')
    
    print(f"\n✅ DONE!")

if __name__ == '__main__':
    test_incremental()