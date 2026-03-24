# src/dint/ubo_graph.py

import networkx as nx
import json
import pickle
from datetime import datetime
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Set, Optional, Any
import logging
from rdflib import BNode
import yaml

from .entity_matcher import EntityMatcher
from .risk_scorer import RiskScorer

logger = logging.getLogger(__name__)


class UBOGraph:
    """Production-grade graph-first architecture"""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.G = nx.MultiDiGraph()
        self.matcher = EntityMatcher(config_path)
        self.risk_scorer = RiskScorer(config_path)

        self.golden_id_counter = 0

        # record_id + entity_id → golden_id
        self.source_to_golden = {}
        self.entity_id_to_golden_id = {}
        logger.info("UBOGraph initialized")

    # =========================================================
    # BUILD FROM OBT
    # =========================================================

    def build_from_obt(self, records: List[Dict]) -> None:
        logger.info(f"Building graph from {len(records)} OBT records")

        blocks = self._create_blocks(records)
        logger.info(f"Created {len(blocks)} blocks for matching")

        match_graph = nx.Graph()
        for record in records:
            match_graph.add_node(record['record_id'], data=record)

        total_comparisons = 0
       

        for block_records in blocks.values():
            for i, rec1 in enumerate(block_records):
                for rec2 in block_records[i + 1:]:
                    total_comparisons += 1

                    should_match, score, breakdown, reason = self.matcher.should_match(rec1, rec2)

                    if should_match:
                        match_graph.add_edge(
                            rec1['record_id'],
                            rec2['record_id'],
                            weight=score,
                            breakdown=breakdown,
                            match_reason=reason  # ← Track WHY they matched
                        )
                        
                        logger.info(
                            f"MATCH: {rec1['record_id']} ↔ {rec2['record_id']} | "
                            f"score={score:.3f} | reason={reason}"
                        )
                    else:
                        logger.debug(
                            f"No match: {rec1['record_id']} ↔ {rec2['record_id']} | "
                            f"score={score:.3f} | reason={reason}"
                        )

        logger.info(f"Performed {total_comparisons} comparisons")

        clusters = list(nx.connected_components(match_graph))
        logger.info(f"Found {len(clusters)} clusters")

        record_lookup = {r['record_id']: r for r in records}

        for cluster in clusters:
            cluster_records = [record_lookup[rid] for rid in cluster]
            self._create_golden_node(cluster_records)

        self._add_relationship_edges()
        self.risk_scorer.calculate_all_risks(self.G)

        logger.info(
            f"Graph built: {self.G.number_of_nodes()} nodes, "
            f"{self.G.number_of_edges()} edges"
        )

    # =========================================================
    # BLOCKING
    # =========================================================

    def _create_blocks(self, records: List[Dict]) -> Dict[str, List[Dict]]:
        blocks = defaultdict(list)

        for record in records:
            block_key = self.matcher.create_blocking_key(record)
            blocks[block_key].append(record)

        return dict(blocks)

    # =========================================================
    # GOLDEN NODE CREATION
    # =========================================================

    def _create_golden_node(self, cluster_records: List[Dict]) -> str:
        """Create a new golden node from cluster of matching records"""
        
        golden_id = f"GOLDEN_{self.golden_id_counter:06d}"
        self.golden_id_counter += 1

        golden_data = self._resolve_conflicts(cluster_records)
        golden_data['golden_id'] = golden_id
        golden_data['source_record_ids'] = [r['record_id'] for r in cluster_records]
        golden_data['source_records'] = list(cluster_records)
        golden_data['source_count'] = len(cluster_records)
        golden_data['created_at'] = datetime.now().isoformat()
        golden_data['blocking_key'] = self.matcher.create_blocking_key(golden_data)

        self.G.add_node(golden_id, **golden_data)

        # ═══════════════════════════════════════════════════════
        # Map ALL identifier formats to golden_id
        # ═══════════════════════════════════════════════════════
        for record in cluster_records:
            # Map record_id
            rid = record.get("record_id")
            if rid:
                self.source_to_golden[rid] = golden_id
            
            # Map entity_id
            eid = record.get("entity_id")
            if eid:
                self.source_to_golden[eid] = golden_id
                self.entity_id_to_golden_id[eid] = golden_id
            
            # ✅ Map company_number (for directorship edge lookups)
            company_num = record.get("company_number")
            if company_num:
                # Map both raw number and ENT_CO_* format
                self.source_to_golden[str(company_num)] = golden_id
                self.source_to_golden[f"ENT_CO_{company_num}"] = golden_id
                logger.debug(f"Mapped company_number {company_num} → {golden_id}")

        logger.debug(f"Created {golden_id} with {len(cluster_records)} source records")

        return golden_id

    # =========================================================
    # CONFLICT RESOLUTION
    # =========================================================

    def _resolve_conflicts(self, cluster_records: List[Dict]) -> Dict[str, Any]:
        

        with open("config/config.yaml") as f:
            config = yaml.safe_load(f)

        source_priorities = config['golden_record']['source_priorities']
        strategies = config['golden_record']['strategies']

        golden_data = {}

        # Resolve entity_type
        types = [
            r.get('entity_type')
            for r in cluster_records
            if r.get('entity_type')
        ]
        if types:
            golden_data['entity_type'] = Counter(types).most_common(1)[0][0]

        # Get all fields
        all_fields = set()
        for record in cluster_records:
            all_fields.update(record.keys())

        for field in all_fields:

            # Skip metadata fields
            if field in [
                'record_id',
                'entity_id',
                'source',
                'batch_id',
                'ingested_at'
            ]:
                continue

            # ═══════════════════════════════════════════════════════
            # ✅ SPECIAL HANDLING: Merge directorship fields
            # ═══════════════════════════════════════════════════════
            if field == 'director_of_entity_ids':
                all_directorships = []
                
                for record in cluster_records:
                    value = record.get('director_of_entity_ids')
                    if value:
                        if isinstance(value, str):
                            # Split semicolon-separated string
                            parts = [v.strip() for v in value.split(';') if v.strip()]
                            all_directorships.extend(parts)
                        elif isinstance(value, list):
                            # Already a list
                            all_directorships.extend(value)
                
                if all_directorships:
                    # Deduplicate and join
                    unique_directorships = list(set(all_directorships))
                    golden_data['director_of_entity_ids'] = ';'.join(unique_directorships)
                    logger.debug(
                        f"Merged {len(all_directorships)} directorships → "
                        f"{len(unique_directorships)} unique"
                    )
                
                continue  # ← Skip normal resolution for this field

            # ═══════════════════════════════════════════════════════
            # Normal field resolution (for all other fields)
            # ═══════════════════════════════════════════════════════
            values = [
                {
                    'value': r[field],
                    'source_type': r.get('source_type', 'external'),
                    'timestamp': r.get('ingested_at', '')
                }
                for r in cluster_records
                if field in r and r[field]
            ]

            if not values:
                continue

            strategy = strategies.get(field, 'most_complete')

            if strategy == 'most_complete':
                chosen = max(values, key=lambda x: len(str(x['value'])))

            elif strategy == 'source_priority':
                for v in values:
                    v['priority'] = source_priorities.get(
                        v['source_type'], 40
                    )
                chosen = max(values, key=lambda x: x['priority'])

            elif strategy == 'most_recent':
                chosen = max(values, key=lambda x: x['timestamp'])

            else:
                chosen = values[0]

            golden_data[field] = chosen['value']

        return golden_data

    # =========================================================
    # RELATIONSHIPS
    # =========================================================

    
    def _add_relationship_edges(self):
        """
        Add relationship edges (ownership, directorship) to the graph.
        Now resolves entity IDs through mapping.
        """
        logger.info("Adding relationship edges...")
        
        ownership_count = 0
        directorship_count = 0
        
        for node_id, node_data in self.G.nodes(data=True):
            
            # ════════════════════════════════════════════════════
            # OWNERSHIP EDGES
            # ════════════════════════════════════════════════════
            
            owned_by_id = node_data.get('owned_by_entity_id')
            ownership_pct = node_data.get('ownership_percentage')
            
            if owned_by_id:
                # ✅ RESOLVE OWNER ID (might be source ID like ENT_PERSON_001)
                owner_golden_id = self._resolve_entity_id(owned_by_id)
                
                if owner_golden_id and owner_golden_id in self.G:
                    self.G.add_edge(
                        owner_golden_id,
                        node_id,
                        relationship_type='owns',
                        ownership_percentage=ownership_pct,
                        ownership_type=node_data.get('ownership_type', 'beneficial')
                    )
                    ownership_count += 1
                else:
                    logger.warning(f"❌ Ownership edge FAILED: {owned_by_id} → {node_id} (owner not found)")
            
            # ════════════════════════════════════════════════════
            # DIRECTORSHIP EDGES
            # ════════════════════════════════════════════════════
            
            director_ids_str = node_data.get('director_of_entity_ids')
            
            if director_ids_str and node_data.get('entity_type') == 'person':
                # Split multiple company IDs
                company_ids = [cid.strip() for cid in str(director_ids_str).split(';') if cid.strip()]
                
                for company_id in company_ids:
                    # ✅ RESOLVE COMPANY ID (might be source ID like ENT_COMPANY_037)
                    company_golden_id = self._resolve_entity_id(company_id)
                    
                    if company_golden_id and company_golden_id in self.G:
                        self.G.add_edge(
                            node_id,
                            company_golden_id,
                            relationship_type='directorOf',
                            role=node_data.get('director_role', 'director')
                        )
                        directorship_count += 1
                    else:
                        logger.warning(f"❌ Directorship edge FAILED: {node_id} → {company_id} (company not found)")
        
        logger.info(f"Added {ownership_count} ownership edges and {directorship_count} directorship edges")


    def _resolve_entity_id(self, raw_id: str) -> Optional[str]:
        """Robust ID resolution — used by both ownership and directorship edges"""
        if not raw_id:
            return None

        raw = str(raw_id).strip()

        # 1. Already golden
        if raw.startswith("GOLDEN_") and raw in self.G:
            return raw

        # 2. Direct lookup in ALL mappings (this was missing!)
        if raw in self.source_to_golden:
            golden = self.source_to_golden[raw]
            if golden in self.G:
                return golden

        # 3. Common variations (your earlier mappings)
        variants = [
            raw,
            f"ENT_CO_{raw}",
            raw.replace("ENT_COMPANY_", ""),
            raw.replace("ENT_PERSON_", ""),
        ]
        for v in variants:
            if v in self.source_to_golden:
                golden = self.source_to_golden[v]
                if golden in self.G:
                    return golden

        logger.warning(f"❌ Could not resolve entity ID: {raw}")
        return None
    # =========================================================
    # INCREMENTAL
    # =========================================================

    def process_incremental_batch(self, new_records: List[Dict]) -> Dict[str, Any]:
        logger.info(f"Processing incremental batch: {len(new_records)} records")
        
        
        
        seen_source_ids = set(self.source_to_golden.keys())
        new_records_filtered = [r for r in new_records if r['record_id'] not in seen_source_ids]
        
        for record in new_records_filtered:
            # ⭐ USE BLOCKING: Only compare against same block
            new_block_key = self.matcher.create_blocking_key(record)
            
            logger.info(f"Processing incremental batch: {len(new_records)} records")
    
        stats = {
            "new_entities": 0,
            "updated_entities": 0,
            "duplicate_sources": 0,
        }
        
        seen_source_ids = set(self.source_to_golden.keys())
        new_records_filtered = [r for r in new_records if r['record_id'] not in seen_source_ids]
    
        for record in new_records_filtered:
            # ✅ TRY blocking first
            new_block_key = self.matcher.create_blocking_key(record)
            
            # Find candidates
            if new_block_key and new_block_key != "P:UNKNOWN" and new_block_key != "CO:UNKNOWN":
                # Use blocking (fast path)
                candidates = [
                    (node_id, node_data)
                    for node_id, node_data in self.G.nodes(data=True)
                    if node_data.get('blocking_key') == new_block_key
                ]

                if not candidates:
                      # ✅ FALLBACK: Compare against ALL nodes (slow but thorough)
                    candidates = list(self.G.nodes(data=True))
                    
                    logger.warning(
                        f"Record {record['record_id']} has no matching block key, "
                        f"comparing against ALL {len(candidates)} nodes (slow!)"
                    )
                
                logger.info(
                    f"Record {record['record_id']} blocked to {len(candidates)} candidates "
                    f"(blocking key: {new_block_key})"
                )
           
              
            
                
            logger.info(f"Record {record['record_id']} blocked to {len(candidates)} candidates (was {self.G.number_of_nodes()})")
            
            best_match = None
            best_score = 0.0
            
            for node_id, node_data in candidates:  # Only compare against block
                should_match, score, breakdown, reason = self.matcher.should_match(record, node_data)
                print(f"Comparing {record} to {node_data} | score={score:.3f} | reason={reason}")


                if should_match and score > best_score:
                    best_match = node_id
                    best_score = score
                    best_reason = reason

            # After loop:
            if best_match:
                logger.info(f"Incremental MATCH: {record['record_id']} → {best_match} | reason={best_reason}")
                self._update_golden_node(best_match, record, best_score)
                stats["updated_entities"] += 1
            else:
                self._create_golden_node([record])
                stats["new_entities"] += 1
        
        self._add_relationship_edges()
        self.risk_scorer.calculate_all_risks(self.G)
        
        return stats

    # =========================================================
    # UPDATE NODE
    # =========================================================

    def _update_golden_node(
    self,
    golden_id: str,
    new_record: Dict,
    match_score: float
) -> None:
        """Update existing golden node with new matched record"""
        
        node_data = self.G.nodes[golden_id]

        # ═══════════════════════════════════════════════════════
        # 1️⃣ Update mappings for new record
        # ═══════════════════════════════════════════════════════
        rid = new_record.get("record_id")
        if rid:
            self.source_to_golden[rid] = golden_id
        
        eid = new_record.get("entity_id")
        if eid:
            self.source_to_golden[eid] = golden_id
        
        # ✅ Map company_number (for directorship edge lookups)
        company_num = new_record.get("company_number")
        if company_num:
            # Map both raw number and ENT_CO_* format
            self.source_to_golden[str(company_num)] = golden_id
            self.source_to_golden[f"ENT_CO_{company_num}"] = golden_id
            logger.debug(f"Mapped company_number {company_num} → {golden_id} (incremental)")

        # ═══════════════════════════════════════════════════════
        # 2️⃣ Get existing cluster records
        # ═══════════════════════════════════════════════════════
        cluster_records = node_data.get('source_records', [])

        # ═══════════════════════════════════════════════════════
        # 3️⃣ Append new record
        # ═══════════════════════════════════════════════════════
        cluster_records.append(new_record)

        # ═══════════════════════════════════════════════════════
        # 4️⃣ Re-resolve conflicts using SAME logic as initial build
        # ═══════════════════════════════════════════════════════
        resolved_data = self._resolve_conflicts(cluster_records)

        # ═══════════════════════════════════════════════════════
        # 5️⃣ Update node with resolved fields
        # ═══════════════════════════════════════════════════════
        for key, value in resolved_data.items():
            node_data[key] = value

        # ═══════════════════════════════════════════════════════
        # 6️⃣ Update metadata
        # ═══════════════════════════════════════════════════════
        node_data['source_records'] = cluster_records
        node_data['source_record_ids'] = [r['record_id'] for r in cluster_records]
        node_data['source_count'] = len(cluster_records)
        node_data['updated_at'] = datetime.now().isoformat()
        node_data['last_match_score'] = match_score
        
        logger.debug(
            f"Updated {golden_id} with new record {rid} "
            f"(now {len(cluster_records)} source records)"
        )
        # =========================================================
        # LOAD GRAPH
        # =========================================================

    def load_graph(self, filepath: str) -> None:
        """Load graph from pickle file"""
        
        with open(filepath, 'rb') as f:
            self.G = pickle.load(f)

        # ═══════════════════════════════════════════════════════
        # Rebuild ALL mappings from loaded graph
        # ═══════════════════════════════════════════════════════
        self.source_to_golden = {}

        for node_id, node_data in self.G.nodes(data=True):
            
            # Map all source record_ids
            for rid in node_data.get("source_record_ids", []):
                self.source_to_golden[rid] = node_id
            
            # Map entity_id (from golden node data)
            eid = node_data.get("entity_id")
            if eid:
                self.source_to_golden[eid] = node_id
            
            # ✅ Map company_number (for directorship lookups)
            company_num = node_data.get("company_number")
            if company_num:
                self.source_to_golden[str(company_num)] = node_id
                self.source_to_golden[f"ENT_CO_{company_num}"] = node_id

        # ═══════════════════════════════════════════════════════
        # Reset golden_id counter
        # ═══════════════════════════════════════════════════════
        existing_ids = [
            int(n.split('_')[1])
            for n in self.G.nodes
            if n.startswith("GOLDEN_")
        ]

        self.golden_id_counter = (
            max(existing_ids) + 1 if existing_ids else 0
        )
        
        logger.info(
            f"Loaded graph: {self.G.number_of_nodes()} nodes, "
            f"{self.G.number_of_edges()} edges, "
            f"{len(self.source_to_golden)} mappings"
        )
    def save_graph(self, filepath: str) -> None:
            """Persist graph"""
            with open(filepath, 'wb') as f:
                pickle.dump(self.G, f)
            
            logger.info(f"Saved graph to {filepath}")

        
    def export_golden_records_json(self, filepath: str) -> None:
        """Export as JSON"""
        records = {}
        
        for node_id, node_data in self.G.nodes(data=True):
            record = dict(node_data)
            
            # Add relationships
            record['owns'] = []
            record['owned_by'] = []
            record['director_of'] = []
            record['has_director'] = []
            
            for source, target, edge_data in self.G.out_edges(node_id, data=True):
                if edge_data.get('relationship_type') == 'owns':
                    record['owns'].append({
                        'entity_id': target,
                        'percentage': edge_data.get('ownership_percentage')
                    })
                elif edge_data.get('relationship_type') == 'directorOf':
                    record['director_of'].append(target)
            
            for source, target, edge_data in self.G.in_edges(node_id, data=True):
                if edge_data.get('relationship_type') == 'owns':
                    record['owned_by'].append({
                        'entity_id': source,
                        'percentage': edge_data.get('ownership_percentage')
                    })
                elif edge_data.get('relationship_type') == 'directorOf':
                    record['has_director'].append(source)
            
            records[node_id] = record
        
        output = {
            "records": records,
            "metadata": {
                "total_nodes": self.G.number_of_nodes(),
                "total_edges": self.G.number_of_edges(),
                "created_at": datetime.now().isoformat(),
                "source_records_processed": len(self.source_to_golden)
            }
        }
        
        with open(filepath, 'w') as f:
            json.dump(output, f, indent=2, default=str)
        
        logger.info(f"Exported golden records to {filepath}")
    
    def export_rdf(self, filepath: str) -> None:
        """Export NetworkX graph to RDF with proper validation"""
        from rdflib import Graph, Namespace, Literal, URIRef
        
        from rdflib.namespace import RDF, XSD, PROV
        
        with open("config/config.yaml") as f:
            config = yaml.safe_load(f)
        
        rdf_config = config['rdf_export']
        
        rdf_graph = Graph()
        
        UBO = Namespace(rdf_config['ubo_namespace'])
        ENTITY = Namespace(rdf_config['entity_namespace'])
        PROV = Namespace("http://www.w3.org/ns/prov#")
        rdf_graph.bind('ubo', UBO)
        rdf_graph.bind('entity', ENTITY)
        rdf_graph.bind('prov', PROV)
        
        logger.info(f"Exporting {self.G.number_of_nodes()} nodes to RDF...")
        
        # =========================================
        # EXPORT NODES
        # =========================================
        
        for node_id, node_data in self.G.nodes(data=True):
            entity_uri = ENTITY[node_id]
            
            # Entity type
            entity_type = node_data.get('entity_type', 'unknown')
            
            if entity_type == 'person':
                rdf_graph.add((entity_uri, RDF.type, UBO.Person))
                
                if node_data.get('is_pep'):
                    rdf_graph.add((entity_uri, RDF.type, UBO.PoliticallyExposedPerson))
                
                if node_data.get('is_nominee_director'):
                    rdf_graph.add((entity_uri, RDF.type, UBO.NomineeDirector))
            
            elif entity_type == 'company':
                rdf_graph.add((entity_uri, RDF.type, UBO.Company))
                
                if node_data.get('is_offshore'):
                    rdf_graph.add((entity_uri, RDF.type, UBO.OffshoreCompany))
                
                # ✅ FIX: Check employee_count properly
                emp_count = node_data.get('employee_count')
                if emp_count is not None and emp_count == 0:
                    rdf_graph.add((entity_uri, RDF.type, UBO.ShellCompany))
            
            # Name & Golden ID
            rdf_graph.add((entity_uri, UBO.hasName, 
                        Literal(node_data.get('full_name', 'Unknown'))))
            
            rdf_graph.add((entity_uri, UBO.hasGoldenId, 
                        Literal(node_data.get('golden_id', node_id))))
            
            # ══════════════════════════════════════════════════════
            # PERSON FIELDS
            # ══════════════════════════════════════════════════════
            if entity_type == 'person':
                dob = node_data.get('dob')
                if dob:
                    rdf_graph.add((entity_uri, UBO.dateOfBirth, 
                                Literal(dob, datatype=XSD.date)))
                
                nationality = node_data.get('nationality')
                if nationality:
                    rdf_graph.add((entity_uri, UBO.hasNationality, 
                                Literal(nationality)))
                
                is_pep = node_data.get('is_pep', False)
                rdf_graph.add((entity_uri, UBO.isPEP, 
                            Literal(is_pep, datatype=XSD.boolean)))
                
                pep_level = node_data.get('pep_level')
                if pep_level:
                    rdf_graph.add((entity_uri, UBO.pepLevel, 
                                Literal(pep_level)))
                
                director_count = node_data.get('director_count')
                if director_count is not None:
                    rdf_graph.add((entity_uri, UBO.directorCount, 
                                Literal(director_count, datatype=XSD.integer)))
                
                is_nominee = node_data.get('is_nominee_director', False)
                rdf_graph.add((entity_uri, UBO.isNominee, 
                            Literal(is_nominee, datatype=XSD.boolean)))
            
            # ══════════════════════════════════════════════════════
            # COMPANY FIELDS
            # ══════════════════════════════════════════════════════
            elif entity_type == 'company':
                company_number = node_data.get('company_number')
                if company_number:
                    rdf_graph.add((entity_uri, UBO.companyNumber, 
                                Literal(str(company_number))))
                
                date_inc = node_data.get('date_incorporated')
                if date_inc:
                    rdf_graph.add((entity_uri, UBO.dateIncorporated, 
                                Literal(date_inc, datatype=XSD.date)))
                
                employee_count = node_data.get('employee_count')
                if employee_count is not None:
                    try:
                        emp_int = int(float(employee_count))
                        rdf_graph.add((entity_uri, UBO.employeeCount, 
                                    Literal(emp_int, datatype=XSD.integer)))
                    except (ValueError, TypeError):
                        pass
                
                is_offshore = node_data.get('is_offshore', False)
                rdf_graph.add((entity_uri, UBO.isOffshore, 
                            Literal(is_offshore, datatype=XSD.boolean)))
            
            # ══════════════════════════════════════════════════════
            # COMMON FIELDS
            # ══════════════════════════════════════════════════════
            
            address = node_data.get('address')
            if address:
                rdf_graph.add((entity_uri, UBO.hasAddress, Literal(str(address))))
            
            country = node_data.get('country')
            if country:
                rdf_graph.add((entity_uri, UBO.hasCountry, Literal(str(country))))
            
            city = node_data.get('city')
            if city:
                rdf_graph.add((entity_uri, UBO.hasCity, Literal(str(city))))
            
            # ══════════════════════════════════════════════════════
            # RISK FIELDS
            # ══════════════════════════════════════════════════════
            
            risk_score = node_data.get('risk_score')
            if risk_score is not None:
                rdf_graph.add((entity_uri, UBO.riskScore, 
                            Literal(float(risk_score), datatype=XSD.decimal)))
            
            risk_level = node_data.get('risk_level')
            if risk_level:
                rdf_graph.add((entity_uri, UBO.riskLevel, Literal(str(risk_level))))
            
            # ✅ FIX: Properly handle risk_factors (list of dicts)
            risk_factors = node_data.get('risk_factors')
            if risk_factors and isinstance(risk_factors, list):
                for factor in risk_factors:
                    if isinstance(factor, dict):
                        factor_str = f"{factor.get('type', 'UNKNOWN')}: {factor.get('detail', '')}"
                        rdf_graph.add((entity_uri, UBO.riskFactor, Literal(factor_str)))
                    else:
                        rdf_graph.add((entity_uri, UBO.riskFactor, Literal(str(factor))))
            
            # Circular ownership flag
            if node_data.get('has_circular_ownership'):
                rdf_graph.add((entity_uri, UBO.hasCircularOwnership, 
                            Literal(True, datatype=XSD.boolean)))
            
            # ══════════════════════════════════════════════════════
            # METADATA
            # ══════════════════════════════════════════════════════
            
            source_count = node_data.get('source_count')
            if source_count:
                rdf_graph.add((entity_uri, UBO.sourceCount, 
                            Literal(source_count, datatype=XSD.integer)))
            
            
            created_at = node_data.get('created_at')
            if created_at:
                rdf_graph.add((entity_uri, PROV.generatedAtTime, 
                            Literal(created_at, datatype=XSD.dateTime)))
            
            # Link to every original source record (exactly like your JSON)
            for sid in node_data.get('source_record_ids', []):
                # Create a stable URI for the original source record
                source_uri = ENTITY[f"RECORD_{sid}"]
                
                rdf_graph.add((entity_uri, PROV.wasDerivedFrom, source_uri))
                
                # Optional: describe the source record (very useful for auditors)
                rdf_graph.add((source_uri, RDF.type, PROV.Entity))
                rdf_graph.add((source_uri, PROV.atLocation, 
                            Literal(f"urn:source:record:{sid}")))
                rdf_graph.add((source_uri, PROV.label, 
                            Literal(f"Original record {sid}")))
            
            # Optional: who generated it (the pipeline)
            rdf_graph.add((entity_uri, PROV.wasAttributedTo, 
                        URIRef("urn:pipeline:ubo-graph-v1")))
            
            # If you have updated_at, you can also add prov:wasRevisedBy etc.
            updated_at = node_data.get('updated_at')
            if updated_at:
                rdf_graph.add((entity_uri, PROV.invalidatedAtTime, 
                            Literal(updated_at, datatype=XSD.dateTime)))
        
        # =========================================
        # EXPORT EDGES
        # =========================================
        
        logger.info(f"Exporting {self.G.number_of_edges()} edges...")
    
        for source, target, edge_data in self.G.edges(data=True):
            source_uri = ENTITY[source]
            target_uri = ENTITY[target]
            
            rel_type = edge_data.get('relationship_type')
            
            if rel_type == 'owns':
                # ✅ SIMPLE PROPERTY (for basic queries like "who owns what")
                rdf_graph.add((source_uri, UBO.owns, target_uri))
                
                # 🔥 NEW: REIFIED OWNERSHIP STAKE (for precise percentage tracking)
                ownership_pct = edge_data.get('ownership_percentage')
                if ownership_pct is not None:
                    # Create blank node for this specific ownership relationship
                    stake = BNode()  # ← Creates _:b1, _:b2, etc.
                    
                    # Link owner to stake
                    rdf_graph.add((source_uri, UBO.hasStake, stake))
                    
                    # Define what the stake is
                    rdf_graph.add((stake, RDF.type, UBO.OwnershipStake))
                    
                    # Link stake to company
                    rdf_graph.add((stake, UBO.inCompany, target_uri))
                    
                    # Add percentage to THIS specific stake
                    rdf_graph.add((stake, UBO.percentage, 
                                Literal(float(ownership_pct), datatype=XSD.decimal)))
                    
                    # ✅ OPTIONAL: Add ownership type
                    ownership_type = edge_data.get('ownership_type', 'beneficial')
                    rdf_graph.add((stake, UBO.ownershipType, Literal(ownership_type)))
                    
                    logger.debug(
                        f"Created ownership stake: {source} → {target} ({ownership_pct}%)"
                    )

                    # ← NEW: Provenance on the stake itself
                    rdf_graph.add((stake, PROV.generatedAtTime, 
                                Literal(created_at or datetime.now().isoformat(), datatype=XSD.dateTime)))
                    rdf_graph.add((stake, PROV.wasDerivedFrom, entity_uri))
            
            elif rel_type == 'directorOf':
                # ✅ SIMPLE PROPERTY (for basic queries)
                rdf_graph.add((source_uri, UBO.directorOf, target_uri))
                
                # 🔥 NEW: REIFIED DIRECTORSHIP (for role, dates, etc.)
                appointment = BNode()
                
                # Link person to appointment
                rdf_graph.add((source_uri, UBO.hasAppointment, appointment))
                
                # Define what the appointment is
                rdf_graph.add((appointment, RDF.type, UBO.DirectorshipAppointment))
                
                # Link appointment to company
                rdf_graph.add((appointment, UBO.atCompany, target_uri))
                
                # ✅ OPTIONAL: Add role
                role = edge_data.get('role', 'director')
                rdf_graph.add((appointment, UBO.role, Literal(role)))
                
                # ✅ OPTIONAL: Add appointment date
                appointed_date = edge_data.get('appointed_date')
                if appointed_date:
                    rdf_graph.add((appointment, UBO.appointedOn, 
                                Literal(appointed_date, datatype=XSD.date)))
                
                # ✅ OPTIONAL: Add active status
                is_active = edge_data.get('is_active', True)
                rdf_graph.add((appointment, UBO.isActive, 
                            Literal(is_active, datatype=XSD.boolean)))
                rdf_graph.add((appointment, PROV.generatedAtTime, 
                            Literal(created_at or datetime.now().isoformat(), datatype=XSD.dateTime)))
                rdf_graph.add((appointment, PROV.wasDerivedFrom, source_uri))
        
        
        # =========================================
        # SERIALIZE & VALIDATE
        # =========================================
        
        rdf_graph.serialize(destination=filepath, format=rdf_config['format'])
        
        # ✅ Validate file was created
        import os
        if os.path.exists(filepath):
            file_size = os.path.getsize(filepath)
            logger.info(f"✅ RDF export successful!")
            logger.info(f"   File: {filepath}")
            logger.info(f"   Size: {file_size:,} bytes")
            logger.info(f"   Triples: {len(rdf_graph):,}")
            logger.info(f"   Nodes: {self.G.number_of_nodes()}")
            logger.info(f"   Edges: {self.G.number_of_edges()}")
        else:
            logger.error(f"❌ RDF export failed - file not created: {filepath}")