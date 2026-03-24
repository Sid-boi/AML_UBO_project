# src/dint/risk_scorer.py

import networkx as nx
import yaml
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class RiskScorer:
    """Calculate comprehensive risk scores for entities"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path) as f:
            config = yaml.safe_load(f)
        
        self.config = config['risk_scoring']
        logger.info("RiskScorer initialized")
    
    def calculate_all_risks(self, G: nx.MultiDiGraph) -> None:
        """Calculate risk scores for all nodes in the graph"""
        
        logger.info("Starting risk scoring...")
        
        # ═══════════════════════════════════════════════════════
        # STEP 1: Detect Circular Ownership
        # ═══════════════════════════════════════════════════════
        cycles = list(nx.simple_cycles(G))
        cycle_nodes = set()
        for cycle in cycles:
            cycle_nodes.update(cycle)
        
        logger.info(f"Detected {len(cycles)} circular ownership cycles involving {len(cycle_nodes)} nodes")
        
        # ═══════════════════════════════════════════════════════
        # STEP 2: Calculate Risk for Each Node
        # ═══════════════════════════════════════════════════════
        high_risk_count = 0
        medium_risk_count = 0
        
        for node_id, node_data in G.nodes(data=True):
            risk_score = 0
            risk_factors = []
            
            entity_type = node_data.get('entity_type')
            full_name = node_data.get('full_name', 'Unknown')
            
            # ───────────────────────────────────────────────────
            # PERSON RISK FACTORS
            # ───────────────────────────────────────────────────
            if entity_type == 'person':
                # Risk 1: Nominee Director (directors of many companies)
                director_edges = [
                    (source, target, data)
                    for source, target, data in G.out_edges(node_id, data=True)
                    if data.get('relationship_type') == 'directorOf'
                ]
                director_count = len(director_edges)
                
                # Store director count for reporting
                G.nodes[node_id]['director_count'] = director_count
                
                if director_count >= self.config['nominee_director_threshold']:
                    risk_score += 35
                    risk_factors.append({
                        'type': 'NOMINEE_DIRECTOR',
                        'severity': 'HIGH',
                        'detail': f'Director of {director_count} companies'
                    })
                    G.nodes[node_id]['is_nominee_director'] = True
                    logger.info(f"🚨 HIGH RISK: {full_name} is nominee director ({director_count} directorships)")
                
                # Risk 2: PEP (already in data)
                if node_data.get('is_pep'):
                    pep_score = self.config.get('pep_risk_score', 20)
                    risk_score += pep_score
                    pep_level = node_data.get('pep_level', 'unknown')
                    risk_factors.append({
                        'type': 'POLITICALLY_EXPOSED',
                        'severity': 'HIGH',
                        'detail': f'PEP: {pep_level}'
                    })
                    logger.info(f"🚨 HIGH RISK: {full_name} is PEP ({pep_level})")
            
            # ───────────────────────────────────────────────────
            # COMPANY RISK FACTORS
            # ───────────────────────────────────────────────────
            elif entity_type == 'company':
                # Get company attributes
                is_offshore = node_data.get('is_offshore', False)
                employee_count = node_data.get('employee_count')
                
                # Convert employee_count safely
                try:
                    emp_count = int(employee_count) if employee_count not in [None, '', 'nan'] else None
                except (ValueError, TypeError):
                    emp_count = None
                
                # Risk 1: Shell Company Detection
                shell_config = self.config['shell_company_indicators']
                
                # Shell company criteria:
                # - Must be offshore
                # - Must have 0 or very few employees (≤ max_employees)
                
                if is_offshore and emp_count is not None and emp_count <= shell_config['max_employees']:
                    risk_score += 30
                    risk_factors.append({
                        'type': 'SHELL_COMPANY',
                        'severity': 'HIGH',
                        'detail': f'Offshore, {emp_count} employees'
                    })
                    logger.info(f"🚨 HIGH RISK: {full_name} is shell company (offshore, {emp_count} employees)")
                
                # Risk 2: Complex Ownership Structure
                out_edges = list(G.out_edges(node_id, data=True))
                owns_edges = [e for e in out_edges if e[2].get('relationship_type') == 'owns']
                
                if len(owns_edges) >= 5:
                    risk_score += 15
                    risk_factors.append({
                        'type': 'COMPLEX_OWNERSHIP',
                        'severity': 'MEDIUM',
                        'detail': f'Owns {len(owns_edges)} companies'
                    })
                    logger.info(f"⚠️  MEDIUM RISK: {full_name} has complex ownership ({len(owns_edges)} companies)")
            
            # ───────────────────────────────────────────────────
            # UNIVERSAL RISK FACTORS
            # ───────────────────────────────────────────────────
            
            # Risk: Circular Ownership
            if node_id in cycle_nodes:
                risk_score += 35
                risk_factors.append({
                    'type': 'CIRCULAR_OWNERSHIP',
                    'severity': 'CRITICAL',
                    'detail': 'Part of circular ownership structure'
                })
                G.nodes[node_id]['has_circular_ownership'] = True
                logger.info(f"🚨 CRITICAL RISK: {full_name} in circular ownership")
            
            # ───────────────────────────────────────────────────
            # SET FINAL RISK SCORE & LEVEL
            # ───────────────────────────────────────────────────
            
            # Cap risk score at 100
            final_risk_score = min(risk_score, 100)
            
            # Store risk data in node
            G.nodes[node_id]['risk_score'] = final_risk_score
            G.nodes[node_id]['risk_factors'] = risk_factors
            
            # Determine risk level
            risk_levels = self.config['risk_levels']
            if final_risk_score >= risk_levels['high']:
                G.nodes[node_id]['risk_level'] = 'HIGH'
                high_risk_count += 1
            elif final_risk_score >= risk_levels['medium']:
                G.nodes[node_id]['risk_level'] = 'MEDIUM'
                medium_risk_count += 1
            else:
                G.nodes[node_id]['risk_level'] = 'LOW'
        
        # ═══════════════════════════════════════════════════════
        # SUMMARY LOG
        # ═══════════════════════════════════════════════════════
        logger.info(
            f"✅ Risk scoring complete: "
            f"{high_risk_count} HIGH, {medium_risk_count} MEDIUM risk entities | "
            f"{len(cycle_nodes)} nodes in circular ownership"
        )