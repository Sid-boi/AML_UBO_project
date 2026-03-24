# src/dint/evaluator.py

from itertools import combinations
import logging
from typing import Dict, List, Set, Any, Tuple

logger = logging.getLogger(__name__)


class F1Evaluator:
    """
    Evaluate clustering quality using pairwise F1 score.
    
    Philosophy:
    - Convert clusters to pairs (record_id1, record_id2)
    - Compare ground truth pairs vs predicted pairs
    - Calculate precision, recall, F1
    """
    
    def __init__(self, ground_truth: Dict[str, Set[str]], predicted_clusters: List[Set[str]]):
        """
        Args:
            ground_truth: Dict mapping entity_id → set of record_ids
            predicted_clusters: List of sets, each set contains record_ids that matched
        """
        self.ground_truth = ground_truth
        self.predicted = predicted_clusters
    
    def calculate_metrics(self) -> Dict[str, Any]:
        """Calculate Precision, Recall, F1"""
        
        # Convert clusters to pairs
        gt_pairs = self._get_all_pairs(self.ground_truth.values())
        pred_pairs = self._get_all_pairs(self.predicted)
        
        # Calculate TP, FP, FN
        tp = len(gt_pairs & pred_pairs)  # Correctly matched pairs
        fp = len(pred_pairs - gt_pairs)  # Incorrectly matched pairs
        fn = len(gt_pairs - pred_pairs)  # Missed pairs
        
        # Calculate metrics
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
        
        logger.info(f"F1 Metrics: P={precision:.3f}, R={recall:.3f}, F1={f1:.3f}")
        
        return {
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "true_positives": tp,
            "false_positives": fp,
            "false_negatives": fn,
            "total_ground_truth_pairs": len(gt_pairs),
            "total_predicted_pairs": len(pred_pairs)
        }
    
    def _get_all_pairs(self, clusters) -> Set[Tuple[str, str]]:
        """
        Convert clusters to pairs.
        
        Example:
        Cluster: {A, B, C}
        Pairs: (A,B), (A,C), (B,C)
        """
        all_pairs = set()
        for cluster in clusters:
            if len(cluster) < 2:
                continue  # Skip single-element clusters
            pairs = combinations(sorted(cluster), 2)
            all_pairs.update(pairs)
        return all_pairs
    
    def cluster_purity_analysis(self) -> List[Dict[str, Any]]:
        """
        Analyze cluster purity.
        
        Purity = (# records from dominant entity) / (total records in cluster)
        
        Example:
        Predicted cluster: {Alice_CSV, Alice_XML, Bob_XML}
        Ground truth: Alice has 2, Bob has 1
        Purity: 2/3 = 0.67 (impure!)
        """
        analysis = []
        
        for i, pred_cluster in enumerate(self.predicted):
            # Count which ground truth entities are in this cluster
            gt_entities = {}
            
            for record_id in pred_cluster:
                # Find which ground truth entity this record belongs to
                for gt_entity_id, gt_records in self.ground_truth.items():
                    if record_id in gt_records:
                        gt_entities[gt_entity_id] = gt_entities.get(gt_entity_id, 0) + 1
                        break
            
            if gt_entities:
                # Find dominant entity
                dominant_entity = max(gt_entities.items(), key=lambda x: x[1])
                purity = dominant_entity[1] / len(pred_cluster)
                
                analysis.append({
                    "cluster_id": i,
                    "size": len(pred_cluster),
                    "purity": purity,
                    "is_pure": purity == 1.0,
                    "entities_mixed": len(gt_entities),
                    "dominant_entity": dominant_entity[0]
                })
        
        return analysis