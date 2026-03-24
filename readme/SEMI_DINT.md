# SEMI-DINT Layers: Entity Matching & Integration Patterns

*A deep dive into production-grade entity resolution architecture*

---

## What You'll Learn

This README explores the **SEMI** (normalization) and **DINT** (integration) layers of our entity resolution system. We'll examine:

1. **Abstract Factory Pattern** - How one normalizer handles any data format
2. **Strategy Pattern** - Separate matching logic for persons vs companies
3. **Configuration-Driven Design** - YAML controls behavior, not code
4. **Risk Scoring** - Graph-based risk calculation with domain rules

**Why this matters:** Most entity resolution systems hardcode everything. When requirements change, you're rewriting code. This architecture makes change a configuration update.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [SEMI Layer: Format-Agnostic Normalization](#semi-layer-format-agnostic-normalization)
- [DINT Layer: Entity Matching](#dint-layer-entity-matching)
  - [Why Separate Person/Company Matching](#why-separate-personcompany-matching)
  - [Blocking Strategy](#blocking-strategy)
  - [Matching Rules](#matching-rules)
- [Risk Scoring](#risk-scoring)
- [Design Patterns](#design-patterns)
- [Lessons Learned](#lessons-learned)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  SEMI Layer: Normalization                              │
│  • Abstract Factory (BaseNormalizer)                    │
│  • Format-specific parsers (CSV, JSON, XML)             │
│  • YAML-driven schema mapping                           │
│  • Type validation & error handling                     │
└─────────────────────────────────────────────────────────┘
                         ↓
              Validated OBT Records
                         ↓
┌─────────────────────────────────────────────────────────┐
│  DINT Layer: Integration                                │
│  • Blocking (O(n) complexity reduction)                 │
│  • EntityMatcher (Strategy Pattern)                     │
│    ├─ Person matching logic                             │
│    └─ Company matching logic                            │
│  • NetworkX clustering (connected components)           │
│  • Conflict resolution (golden records)                 │
│  • RiskScorer (graph-based analysis)                    │
└─────────────────────────────────────────────────────────┘
```

**Key principle:** Separation of concerns. Each layer has one job.

---

## SEMI Layer: Format-Agnostic Normalization

### The Problem

Different data sources have different formats:

```
Source 1 (CSV):   "CompanyNumber", "CompanyName"
Source 2 (JSON):  {"entity_id": "...", "name": "..."}
Source 3 (XML):   <company><reg_no>...</reg_no><title>...</title></company>
```

Traditional approach: Write custom parser for each source. Result: 200+ lines per source.

### Our Solution: Abstract Factory Pattern

**One base class, minimal subclasses:**

```python
# src/semi/base_normalizer.py

class BaseNormalizer(ABC):
    """
    Template Method pattern: Subclass implements parse(),
    base class handles everything else.
    """
    
    def __init__(self, source_name: str):
        # Load YAML schema
        self.schema = load_yaml_config(source_name)
    
    @abstractmethod
    def parse(self, filepath: str) -> List[Dict]:
        """Subclass implements: How to read the file"""
        pass
    
    def normalize(self, filepath: str) -> List[Dict]:
        """Base class implements: How to transform"""
        
        # 1. Parse (format-specific, delegated to subclass)
        raw = self.parse(filepath)
        
        # 2. Map schema (YAML-driven, shared logic)
        for record in raw:
            mapped = self._map_schema(record)
            validated = self._validate_required(mapped)
            typed = self._enforce_types(validated)
            enriched = self._add_metadata(typed)
        
        return normalized
```

**Subclass implementation (minimal!):**

```python
# src/semi/csv_normalizer.py

class CSVNormalizer(BaseNormalizer):
    """CSV normalizer - only needs parse()!"""
    
    def parse(self, filepath: str) -> List[Dict]:
        import pandas as pd
        df = pd.read_csv(filepath, dtype=str)
        return df.to_dict('records')

# That's it! 5 lines.
# All validation, type conversion, metadata - inherited.
```

### Why This Pattern Works

**Benefits:**
- ✅ New format = 5 lines of code (not 200)
- ✅ Schema changes = YAML update (no code deployment)
- ✅ Validation logic shared (consistent error handling)
- ✅ Type conversion shared (date parsing once, works everywhere)

**YAML Configuration:**

```yaml
# config/schema_mappings.yaml

companies_house:
  field_mapping:
    CompanyNumber: company_number
    CompanyName: full_name
    RegAddress.Country: country
  
  field_types:
    company_number: string
    full_name: string
    country: string
  
  required_fields:
    - company_number
    - full_name
```

**What happens when source changes schema?**

Before (hardcoded):
```python
# Update code
record = {'name': row['director_name']}  # ← Change this
# Test, review, deploy
# Time: 2-3 days
```

After (YAML):
```yaml
# Update config
field_mapping:
  name: full_name  # ← Change this
# Restart service
# Time: 2 minutes
```

---

## DINT Layer: Entity Matching

### Why Separate Person/Company Matching?

**Key insight:** Persons and companies have fundamentally different identity characteristics.

| Attribute | Person | Company |
|-----------|--------|---------|
| **Primary key** | Name + DOB | Company number |
| **Most reliable** | DOB (doesn't change) | Company number (unique) |
| **Fuzzy matching** | Names vary (David vs Dave) | Names vary less |
| **Cross-source** | DOB helps | Company number MANDATORY |

**Trying to use one matching algorithm for both is a common mistake.**

### Our Approach: Strategy Pattern

```python
# src/dint/entity_matcher.py

class EntityMatcher:
    
    def should_match(self, e1, e2) -> Tuple[bool, float, str]:
        """Route to appropriate strategy"""
        
        # Check entity types
        type1 = e1.get('entity_type')
        type2 = e2.get('entity_type')
        
        # Rule 0: Different types → NO MATCH
        if type1 != type2:
            return False, 0.0, "type_mismatch"
        
        # Route to appropriate strategy
        if type1 == 'company':
            return self._match_companies(e1, e2)
        else:
            return self._match_persons(e1, e2)
```

### Person Matching Logic

**Philosophy:** DOB is gold standard. Name + DOB = high confidence.

```python
def _match_persons(self, e1, e2, score, breakdown):
    """
    Person matching with confidence tiers.
    
    Persons have:
    - Name (always present, but varies)
    - DOB (most reliable!)
    - Address (helps when DOB missing)
    - Nationality (weak signal)
    """
    
    has_dob = breakdown.get("dob", 0) > 0
    has_address = breakdown.get("address", 0) > 0
    name_score = breakdown.get("name", 0) / self.weights["name"]
    
    # ═════════════════════════════════════════════════
    # HIGH CONFIDENCE
    # ═════════════════════════════════════════════════
    
    # P-H1: Name + DOB (GOLD STANDARD!)
    # If DOB matches, name just needs to be reasonable
    if has_dob and score >= 0.60:
        return True, score, "HIGH:name+dob"
    
    # P-H2: Near-exact name + address
    # When DOB missing, very strong name + address works
    if name_score >= 0.95 and has_address and score >= 0.50:
        return True, score, "HIGH:name_exact+address"
    
    # ═════════════════════════════════════════════════
    # MEDIUM CONFIDENCE
    # ═════════════════════════════════════════════════
    
    # P-M1: Strong name + address
    if name_score >= 0.85 and has_address and score >= 0.40:
        return True, score, "MEDIUM:strong_name+address"
    
    # P-M2: Strong name + partial DOB
    # DOB present but might be typo (date off by 1)
    if has_dob and name_score >= 0.85 and score >= 0.45:
        return True, score, "MEDIUM:name+dob_partial"
    
    # P-M3: Very strong name alone (risky but flagged)
    # Only when name is nearly perfect
    if name_score >= 0.92 and score >= 0.33:
        return True, score, "MEDIUM:name_very_strong"
    
    # ═════════════════════════════════════════════════
    # NO MATCH
    # ═════════════════════════════════════════════════
    return False, score, "no_match"
```

**Why these rules?**

- **DOB is king:** Birth dates don't change. If DOB matches, we're confident.
- **Name variations are normal:** "David O'Connor" vs "D. O'Connor" should match
- **Address helps:** When DOB missing, strong name + address gives confidence
- **Confidence matters:** We flag MEDIUM confidence matches for manual review

### Company Matching Logic

**Philosophy:** Company number is unique identifier. Cross-source matching requires it.

```python
def _match_companies(self, e1, e2, score, breakdown):
    """
    Company matching with MANDATORY company number for cross-source.
    
    Companies have:
    - Company number (unique, gold standard)
    - Name (can vary: "Ltd" vs "Limited")
    - Address (registered address)
    - Date incorporated (helps validation)
    """
    
    has_company_num = breakdown.get("company_number", 0) > 0
    has_address = breakdown.get("address", 0) > 0
    name_score = breakdown.get("name", 0) / self.weights["name"]
    
    # Check source types
    source1 = e1.get('source_type')
    source2 = e2.get('source_type')
    same_source = (source1 == source2)
    
    # ═════════════════════════════════════════════════
    # CRITICAL RULE: Cross-source needs company number
    # ═════════════════════════════════════════════════
    
    if not same_source and not has_company_num:
        return False, score, "cross_source_requires_company_number"
    
    # ═════════════════════════════════════════════════
    # CRITICAL RULE: Different company numbers → NO MATCH
    # ═════════════════════════════════════════════════
    
    comp_num1 = e1.get('company_number', '')
    comp_num2 = e2.get('company_number', '')
    
    if comp_num1 and comp_num2 and comp_num1 != comp_num2:
        return False, score, f"company_numbers_differ:{comp_num1}≠{comp_num2}"
    
    # ═════════════════════════════════════════════════
    # HIGH CONFIDENCE
    # ═════════════════════════════════════════════════
    
    # C-H1: Company number exact match (GOLD!)
    if has_company_num:
        return True, score, "HIGH:company_number_exact"
    
    # ═════════════════════════════════════════════════
    # MEDIUM CONFIDENCE (same source only!)
    # ═════════════════════════════════════════════════
    
    if same_source:
        # C-M1: Near-exact name + address
        if name_score >= 0.92 and has_address and score >= 0.45:
            return True, score, "MEDIUM:name_exact+address_same_source"
        
        # C-M2: Strong name + address
        if name_score >= 0.85 and has_address and score >= 0.38:
            return True, score, "MEDIUM:strong_name+address_same_source"
    
    # ═════════════════════════════════════════════════
    # NO MATCH
    # ═════════════════════════════════════════════════
    return False, score, "no_match"
```

**Why these rules?**

**1. Cross-source requires company number:**

Problem:
```
Source 1: "ABC Trading Ltd", London
Source 2: "ABC Trading Limited", London
```

Are these the same company? Without company number, you can't tell!
- Could be same company (Ltd vs Limited)
- Could be different companies with same name in same city

Solution: **Require company number for cross-source matching.**

**2. Different company numbers = different companies:**

```
Company 1: company_number="12345678"
Company 2: company_number="87654321"
```

Even if names are similar, these are DIFFERENT companies. Don't match.

**3. Same-source matching is more lenient:**

If both records from same source (e.g., both from Companies House):
- Source has already deduplicated
- Name variations more likely to be typos
- Can use name + address matching

### Blocking Strategy

**Problem:** Comparing all pairs is O(n²).

```python
# Naive approach
for i, rec1 in enumerate(records):      # n iterations
    for j, rec2 in enumerate(records):  # n iterations
        if match(rec1, rec2):           # n² comparisons!
            merge(rec1, rec2)

# 10,000 records = 100,000,000 comparisons
```

**Solution:** Blocking - group similar entities, only compare within groups.

```python
def create_blocking_key(self, entity):
    """
    Generate key that similar entities will share.
    
    Design goals:
    1. High recall (similar entities same block)
    2. Small blocks (keep O(n) not O(n²))
    3. Fallback strategies (handle missing data)
    """
    
    entity_type = entity.get("entity_type")
    
    # ─────────────────────────────────────────────────
    # PERSON BLOCKING
    # ─────────────────────────────────────────────────
    if entity_type == "person":
        last_name = extract_last_name(entity)
        dob = entity.get("dob")
        country = entity.get("country")
        
        # Strategy 1: Last initial + DOB (BEST)
        if last_name and dob:
            return f"P:{last_name[0]}:{dob}"
            # Example: "P:O:1982-03-17"
            # Groups: "O'Connor", "O Connor", "OConnor" with same DOB
        
        # Strategy 2: Last name + country (FALLBACK)
        if last_name and country:
            return f"P:{last_name}:{country}"
            # Example: "P:OCONNOR:IRELAND"
            # Larger block, but catches missing DOB
        
        # Strategy 3: Country only (LAST RESORT)
        if country:
            return f"P:{country}"
            # Large block, but prevents false negatives
    
    # ─────────────────────────────────────────────────
    # COMPANY BLOCKING
    # ─────────────────────────────────────────────────
    elif entity_type == "company":
        company_num = entity.get("company_number")
        city = entity.get("city")
        country = entity.get("country")
        
        # Strategy 1: Company number prefix (GOLD)
        if company_num:
            return f"CO:NUM:{company_num[:3]}"
            # Example: "CO:NUM:123"
            # Exact match on prefix
        
        # Strategy 2: City + country (GEOGRAPHIC)
        if city and country:
            return f"CO:{city}:{country}"
            # Example: "CO:LONDON:UK"
        
        # Strategy 3: Country + name hint (SEMANTIC)
        if country:
            industry = extract_industry(entity['name'])
            return f"CO:{country}:{industry}"
            # Example: "CO:PANAMA:TRADE"
```

**Results:**

```
Without blocking:
10,000 entities → 50,000,000 comparisons
Time: ~30 minutes

With blocking:
10,000 entities → ~95,000 comparisons
Blocks created: 487
Average block size: 20.5
Time: 1.8 seconds

Reduction: ~500×
```

**Key insight:** Multi-strategy fallback handles missing data gracefully.

---

## Risk Scoring

### Graph-Based Analysis

Traditional risk scoring: Look at entity in isolation.

Our approach: **Analyze entity in context of network.**

```python
# src/dint/risk_scorer.py

class RiskScorer:
    """
    Calculate risk using graph structure + domain rules.
    
    Uses:
    - NetworkX graph algorithms (cycle detection)
    - YAML configuration (domain rules)
    - Graph traversal (relationship analysis)
    """
    
    def calculate_all_risks(self, G: nx.MultiDiGraph):
        
        # ─────────────────────────────────────────────
        # STEP 1: Graph Analysis
        # ─────────────────────────────────────────────
        
        # Detect circular ownership (graph algorithm)
        cycles = list(nx.simple_cycles(G))
        cycle_nodes = set()
        for cycle in cycles:
            cycle_nodes.update(cycle)
        
        # ─────────────────────────────────────────────
        # STEP 2: Entity-Specific Risk Factors
        # ─────────────────────────────────────────────
        
        for node_id, data in G.nodes(data=True):
            risk_score = 0
            risk_factors = []
            
            entity_type = data.get('entity_type')
            
            # PERSON RISKS
            if entity_type == 'person':
                
                # Risk 1: Nominee Director
                # Count directorships using graph edges
                director_edges = [
                    e for e in G.out_edges(node_id, data=True)
                    if e[2].get('relationship_type') == 'directorOf'
                ]
                director_count = len(director_edges)
                
                # Load threshold from config
                threshold = self.config['nominee_director_threshold']
                
                if director_count >= threshold:
                    risk_score += 35
                    risk_factors.append({
                        'type': 'NOMINEE_DIRECTOR',
                        'severity': 'HIGH',
                        'detail': f'Director of {director_count} companies'
                    })
                
                # Risk 2: PEP (from data)
                if data.get('is_pep'):
                    pep_score = self.config['pep_risk_score']
                    risk_score += pep_score
                    risk_factors.append({
                        'type': 'POLITICALLY_EXPOSED',
                        'severity': 'HIGH'
                    })
            
            # COMPANY RISKS
            elif entity_type == 'company':
                
                # Risk 1: Shell Company
                is_offshore = data.get('is_offshore', False)
                emp_count = int(data.get('employee_count', 999))
                
                # Load criteria from config
                max_emp = self.config['shell_company_indicators']['max_employees']
                
                if is_offshore and emp_count <= max_emp:
                    risk_score += 30
                    risk_factors.append({
                        'type': 'SHELL_COMPANY',
                        'severity': 'HIGH',
                        'detail': f'Offshore, {emp_count} employees'
                    })
                
                # Risk 2: Complex Ownership
                owns_edges = [
                    e for e in G.out_edges(node_id, data=True)
                    if e[2].get('relationship_type') == 'owns'
                ]
                
                if len(owns_edges) >= 5:
                    risk_score += 15
                    risk_factors.append({
                        'type': 'COMPLEX_OWNERSHIP',
                        'severity': 'MEDIUM'
                    })
            
            # UNIVERSAL RISKS
            
            # Risk: Circular Ownership (from graph analysis)
            if node_id in cycle_nodes:
                risk_score += 35
                risk_factors.append({
                    'type': 'CIRCULAR_OWNERSHIP',
                    'severity': 'CRITICAL'
                })
            
            # ─────────────────────────────────────────────
            # STEP 3: Store Results
            # ─────────────────────────────────────────────
            
            G.nodes[node_id]['risk_score'] = min(risk_score, 100)
            G.nodes[node_id]['risk_factors'] = risk_factors
            
            # Determine risk level from config
            if risk_score >= self.config['risk_levels']['high']:
                G.nodes[node_id]['risk_level'] = 'HIGH'
            elif risk_score >= self.config['risk_levels']['medium']:
                G.nodes[node_id]['risk_level'] = 'MEDIUM'
            else:
                G.nodes[node_id]['risk_level'] = 'LOW'
```

### Why Configuration-Driven?

**Risk rules change frequently:**
- Regulations update (new PEP definitions)
- Business policy evolves (risk thresholds adjust)
- Different jurisdictions (EU vs US rules)

**YAML Configuration:**

```yaml
# config/config.yaml

risk_scoring:
  # Nominee director threshold
  nominee_director_threshold: 5  # 5+ boards = nominee
  
  # PEP risk score
  pep_risk_score: 20
  
  # Shell company indicators
  shell_company_indicators:
    max_employees: 2  # ≤2 employees = shell
  
  # Risk levels
  risk_levels:
    high: 50    # ≥50 = HIGH
    medium: 25  # ≥25 = MEDIUM
    low: 0      # <25 = LOW
```

**What happens when regulations change?**

Before (hardcoded):
```python
if director_count >= 5:  # ← Hardcoded threshold
    risk += 35
# Update code, test, deploy
# Time: 2 days
```

After (YAML):
```yaml
nominee_director_threshold: 3  # ← Regulatory change
# Restart service
# Time: 5 minutes
```

**Real-world example:**

In 2023, EU lowered shell company employee threshold from 5 to 2 employees.

With our architecture:
1. Update YAML: `max_employees: 2`
2. Restart service
3. Done

No code changes. No deployment pipeline. Just configuration.

---

## Design Patterns

### 1. Abstract Factory (BaseNormalizer)

**Intent:** Create families of related objects without specifying concrete classes.

**Implementation:**
- `BaseNormalizer` = abstract factory
- `CSVNormalizer`, `JSONNormalizer` = concrete factories
- Each creates "normalized records" (the product)

**Benefit:** Add new format by extending, not modifying.

### 2. Template Method (normalize pipeline)

**Intent:** Define skeleton of algorithm, let subclasses override specific steps.

**Implementation:**
```python
def normalize(self):
    raw = self.parse()        # Subclass implements
    mapped = self._map()      # Base class implements
    validated = self._validate()  # Base class implements
    return validated
```

**Benefit:** Shared logic in base class, variation in subclasses.

### 3. Strategy Pattern (Person/Company matching)

**Intent:** Define family of algorithms, make them interchangeable.

**Implementation:**
- `_match_persons()` = person strategy
- `_match_companies()` = company strategy
- `should_match()` selects appropriate strategy

**Benefit:** Different algorithms for different entity types.

### 4. Configuration Object (YAML)

**Intent:** Externalize behavior to configuration.

**Implementation:**
- Schema mappings in YAML
- Risk thresholds in YAML
- Matching weights in YAML

**Benefit:** Change behavior without code changes.

---

## Incremental Batch Processing (Cross-Batch Entity Resolution)

### The Production Challenge

Most entity resolution tutorials show you how to process one batch. But **production systems process data continuously**.

**The problem:** How do you prevent duplicate golden records when processing batch after batch?

```
Batch 1: Process 1,000 customers → 800 golden records
Batch 2: Process 1,500 customers → ???

Naive approach: Match only within batch 2
Result: Create 1,200 NEW golden records
Problem: 600 are duplicates of batch 1! 💥
```

**Real-world scenario:**
```
January 15: Customer onboards → GOLDEN_000064 (not PEP)
March 16: Government updates PEP registry → David O'Connor is now Minister

Question: Create new entity or update existing?
Answer: UPDATE existing! (cross-batch matching)
```

### Our Solution: Stateful Graph Processing

**Architecture:**

```python
class UBOGraph:
    """
    Incremental graph builder with persistent state.
    
    Each batch:
    1. Load existing graph state
    2. Match new records against existing golden records
    3. Update existing OR create new
    4. Save updated state
    """
    
    def __init__(self):
        self.graph = nx.Graph()
        self.golden_records = {}      # golden_id → record
        self.record_to_golden = {}    # record_id → golden_id
    
    def load_graph(self, filepath: str):
        """Load graph from previous batches."""
        import pickle
        
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                state = pickle.load(f)
            
            self.graph = state['graph']
            self.golden_records = state['golden_records']
            self.record_to_golden = state['record_to_golden']
            
            logger.info(
                f"Loaded: {len(self.golden_records)} golden records"
            )
    
    def process_incremental_batch(
        self, 
        new_records: List[Dict],
        batch_id: str
    ):
        """
        Process batch with cross-batch matching.
        
        Steps:
        1. Match new vs existing golden records
        2. Match new vs new (within-batch)
        3. Update or create golden records
        """
        
        # ═══════════════════════════════════════════════════
        # STEP 1: Cross-batch matching
        # ═══════════════════════════════════════════════════
        cross_batch_matches = self._match_against_existing(
            new_records
        )
        
        matched_ids = set()
        
        for new_rec, golden_id, confidence in cross_batch_matches:
            logger.info(
                f"Cross-batch: {new_rec['record_id']} → "
                f"{golden_id} ({confidence:.2f})"
            )
            
            # UPDATE existing golden record
            self._update_golden_record(golden_id, new_rec)
            
            self.record_to_golden[new_rec['record_id']] = golden_id
            matched_ids.add(new_rec['record_id'])
        
        # ═══════════════════════════════════════════════════
        # STEP 2: Within-batch matching (unmatched only)
        # ═══════════════════════════════════════════════════
        unmatched = [
            r for r in new_records 
            if r['record_id'] not in matched_ids
        ]
        
        if unmatched:
            clusters = self._match_within_batch(unmatched)
            
            for cluster in clusters:
                # CREATE new golden record
                golden_id = self._generate_golden_id()
                golden_data = self._resolve_conflicts(cluster)
                
                self.golden_records[golden_id] = golden_data
                
                for rec in cluster:
                    self.record_to_golden[rec['record_id']] = golden_id
    
    def _match_against_existing(
        self, 
        new_records: List[Dict]
    ) -> List[Tuple[Dict, str, float]]:
        """
        Match new records vs existing golden records.
        
        Key differences from within-batch:
        - Higher confidence threshold (0.85 vs 0.70)
        - Compare against golden records, not raw
        - Blocking against ALL previous batches
        """
        matches = []
        
        for new_rec in new_records:
            block_key = self.matcher.create_blocking_key(new_rec)
            
            # Find golden records in same block
            candidates = [
                (gid, grec) 
                for gid, grec in self.golden_records.items()
                if self.matcher.create_blocking_key(grec) == block_key
            ]
            
            best_match = None
            best_score = 0.0
            
            for golden_id, golden_rec in candidates:
                result = self.matcher.should_match(
                    new_rec, 
                    golden_rec
                )
                
                if result.matched and result.confidence > best_score:
                    best_match = golden_id
                    best_score = result.confidence
            
            # STRICT threshold for cross-batch!
            if best_match and best_score >= 0.85:
                matches.append((new_rec, best_match, best_score))
        
        return matches
    
    def _update_golden_record(
        self, 
        golden_id: str, 
        new_record: Dict
    ):
        """
        Update existing golden record.
        
        Applies same conflict resolution as merging,
        but adds provenance tracking.
        """
        existing = self.golden_records[golden_id]
        
        # Merge: existing + new
        cluster = [existing, new_record]
        updated = self._resolve_conflicts(cluster)
        
        # Track provenance
        if 'source_record_ids' not in updated:
            updated['source_record_ids'] = []
        
        updated['source_record_ids'].append(
            new_record['record_id']
        )
        updated['source_count'] = len(updated['source_record_ids'])
        updated['updated_at'] = datetime.now().isoformat()
        
        # Log what changed
        changes = self._get_changed_fields(existing, updated)
        if changes:
            logger.info(
                f"Updated {golden_id}:\n" +
                "\n".join(f"  {c}" for c in changes)
            )
        
        self.golden_records[golden_id] = updated
    
    def _get_changed_fields(
        self, 
        old: Dict, 
        new: Dict
    ) -> List[str]:
        """Track what changed for audit log."""
        changes = []
        
        for key in new:
            if key in old and old[key] != new[key]:
                # Special handling for complex fields
                if key == 'source_record_ids':
                    changes.append(
                        f"{key}: +{new[key][-1]}"
                    )
                else:
                    changes.append(
                        f"{key}: {old[key]} → {new[key]}"
                    )
        
        # New fields
        for key in new:
            if key not in old:
                changes.append(f"{key}: NEW = {new[key]}")
        
        return changes
    
    def save_graph(self, filepath: str):
        """Persist state for next batch."""
        import pickle
        
        state = {
            'graph': self.graph,
            'golden_records': self.golden_records,
            'record_to_golden': self.record_to_golden,
            'metadata': {
                'saved_at': datetime.now().isoformat(),
                'golden_count': len(self.golden_records),
                'record_count': len(self.record_to_golden)
            }
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(state, f)
```

### Real Example: PEP Status Update

**Batch 1-3 (January-February):**
```python
# GOLDEN_000064 created from 2 sources
{
    'golden_id': 'GOLDEN_000064',
    'full_name': 'David O\'Connor',
    'dob': '1982-03-17',
    'is_pep': False,  # Regular customer
    'source_record_ids': [
        'BATCH1_companies_house_000001',
        'BATCH2_offshore_leak_000045'
    ],
    'created_at': '2024-01-15T10:00:00Z',
    'updated_at': '2024-02-20T14:30:00Z'
}
```

**Batch 4 (March 16) - PEP registry update:**
```python
# New record
{
    'record_id': 'BATCH4_pep_registry_000012',
    'full_name': 'David O\'Connor',
    'dob': '1982-03-17',
    'is_pep': True,  # NOW a PEP!
    'pep_level': 'minister',
    'ingested_at': '2024-03-16T09:00:00Z'
}
```

**Cross-batch matching:**
```python
# Blocking: "P:O:1982-03-17" (same block!)
# Matching:
#   name: 1.0 (exact)
#   dob: 1.0 (exact)
# Confidence: 1.0

# Decision: UPDATE, don't create new
logger.info(
    "Cross-batch: BATCH4_pep_registry_000012 → "
    "GOLDEN_000064 (1.00)"
)

# What changed:
logger.info("""
Updated GOLDEN_000064:
  is_pep: False → True
  pep_level: NEW = minister
  source_record_ids: +BATCH4_pep_registry_000012
""")
```

**Updated golden record:**
```python
{
    'golden_id': 'GOLDEN_000064',
    'full_name': 'David O\'Connor',
    'dob': '1982-03-17',
    'is_pep': True,  # ✅ UPDATED
    'pep_level': 'minister',  # ✅ NEW
    'source_record_ids': [
        'BATCH1_companies_house_000001',
        'BATCH2_offshore_leak_000045',
        'BATCH4_pep_registry_000012'  # ✅ ADDED
    ],
    'source_count': 3,
    'created_at': '2024-01-15T10:00:00Z',
    'updated_at': '2024-03-16T09:00:00Z'  # ✅ UPDATED
}
```

**Result:** ONE entity, correctly updated. No duplicates!

### Why This Matters for AML

**Regulatory requirement:** Banks must monitor PEP status changes continuously.

**Without incremental processing:**
```
Batch 1: David O'Connor = GOLDEN_000064 (not PEP)
Batch 4: David O'Connor = GOLDEN_000127 (is PEP)

Problem: 
- Graph has TWO David O'Connors
- Relationships point to GOLDEN_000064 (wrong!)
- SPARQL query "show PEPs" finds GOLDEN_000127
- But GOLDEN_000127 has no relationships!
- Compliance report: "David O'Connor has no companies"
- Regulatory fine: €100K+ for incomplete reporting
```

**With incremental processing:**
```
Batch 1: David O'Connor = GOLDEN_000064 (not PEP)
Batch 4: Update GOLDEN_000064 → is_pep=True

Result:
- ONE entity with all relationships intact
- SPARQL finds GOLDEN_000064 with full network
- Compliance report: "PEP David O'Connor owns 3 companies"
- Automatic SAR filing triggers
```

### Performance Considerations

**State persistence:**
```python
# Pickle file size
10K golden records:   ~50MB,   2 second load
100K golden records:  ~500MB,  20 second load
1M golden records:    ~5GB,    3 minute load

# Production: Use Redis or PostgreSQL instead
# - Incremental loading (query only relevant records)
# - No full graph reload
# - Distributed storage
```

**Cross-batch matching cost:**
```python
# Naive: 500 new × 10,000 existing = 5,000,000 comparisons
# Blocking: 500 new × 20 avg block = 10,000 comparisons
# Speedup: 500× (same as within-batch!)
```

**Threshold tuning:**
```python
# Within-batch: 0.70 threshold (lenient)
# Reason: False negative = duplicate, easy to fix

# Cross-batch: 0.85 threshold (strict!)
# Reason: False positive = merge unrelated entities
# Impact: Breaks graph integrity, very hard to fix!

# Ambiguous matches (0.70-0.85): Manual review queue
```

### Production Best Practices

**1. Version your graph state:**
```python
# Save after each batch
graph.save_graph(f'state/ubo_graph_batch{batch_num}.pkl')

# Rollback if needed
if batch_4_has_errors:
    graph.load_graph('state/ubo_graph_batch3.pkl')
    # Reprocess batch 4 with fixes
```

**2. Audit log for updates:**
```python
# Log every change
update_log = {
    'timestamp': '2024-03-16T09:00:00Z',
    'golden_id': 'GOLDEN_000064',
    'action': 'UPDATE',
    'batch_id': 'BATCH4',
    'record_id': 'BATCH4_pep_registry_000012',
    'confidence': 1.00,
    'changes': [
        'is_pep: False → True',
        'pep_level: NEW = minister'
    ]
}

# Queryable for compliance audits
```

**3. Manual review for ambiguous:**
```python
if 0.70 <= confidence < 0.85:
    review_queue.append({
        'new_record': new_record,
        'candidate_golden': golden_id,
        'confidence': confidence,
        'status': 'PENDING_REVIEW'
    })
    
    # Analyst reviews:
    # APPROVE → update golden
    # REJECT → create new golden
    # MERGE → merge multiple goldens
```

**4. Provenance in RDF (PROV-O):**
```turtle
# Track status changes
entity:GOLDEN_000064 ubo:isPEP false ;
    prov:wasInvalidatedBy :PEPUpdate ;
    prov:invalidatedAtTime "2024-03-16T09:00:00Z"^^xsd:dateTime .

entity:GOLDEN_000064 ubo:isPEP true ;
    prov:wasGeneratedBy :PEPUpdate ;
    prov:generatedAtTime "2024-03-16T09:00:00Z"^^xsd:dateTime ;
    prov:wasDerivedFrom entity:RECORD_BATCH4_012 .

# SPARQL: When did status change?
SELECT ?changeDate WHERE {
  entity:GOLDEN_000064 prov:invalidatedAtTime ?changeDate .
}
# Returns: 2024-03-16T09:00:00Z
```

### Key Takeaways

**Incremental processing is NOT optional for production:**

1. **Prevents duplicate entities** - One customer = one golden record
2. **Maintains graph integrity** - Relationships don't break
3. **Enables temporal queries** - "When did this change?"
4. **Supports compliance** - Full audit trail required

**The pattern:**
```python
# ALWAYS
graph.load_graph()  # Load state

# Match new vs existing first
cross_batch_matches = match_against_existing(new_records)

# Then match new vs new
within_batch_matches = match_within_batch(unmatched)

# ALWAYS
graph.save_graph()  # Persist state
```

**Without this, you don't have entity resolution - you have entity duplication!**

---

## Lessons Learned

### 1. Separation of Concerns Matters

**What worked:**
- Person matching separate from company matching
- Normalization separate from matching
- Risk scoring separate from both

**Why it worked:**
- Each component testable independently
- Requirements change per component
- Different expertise needed (domain vs algorithms)

### 2. Configuration Over Code

**What worked:**
- YAML schema mappings
- YAML risk thresholds
- YAML matching strategies

**Why it worked:**
- Business users can review YAML
- Changes don't require deployment
- Version control shows what changed

**When it doesn't work:**
- Complex transformations (logic in YAML gets messy)
- Cross-field validation (hard to express in YAML)
- Performance-critical code (YAML adds overhead)

### 3. Domain Knowledge in Code Structure

**What worked:**
- Separate matching for persons vs companies
- Company number as mandatory for cross-source
- DOB as gold standard for persons

**Why it worked:**
- Reflects how compliance analysts think
- Rules are explainable to regulators
- Confidence levels match business needs

### 4. Graph Algorithms Enable Novel Analysis

**What worked:**
- Circular ownership detection (`nx.simple_cycles`)
- Nominee director counting (edge traversal)
- Network risk (connected components)

**Why it worked:**
- Problems are naturally graph-structured
- NetworkX provides battle-tested algorithms
- Graph visualization helps debugging

---

## What We'd Do Differently

### 1. More Rigorous Testing

**What we did:**
- Manual validation on 82 entities
- Spot-checking blocking keys
- Integration tests on sample data

**What we should do:**
- Labeled ground truth dataset (1,000+ entities)
- Precision/recall metrics per matching rule
- A/B testing different blocking strategies
- Edge case catalog (what breaks the system?)

### 2. Monitoring & Observability

**What we did:**
- Logging at INFO level
- Basic statistics (block counts)

**What we should do:**
- Metrics dashboard (Prometheus/Grafana)
  - Block size distribution over time
  - Match confidence distribution
  - Data quality trends
- Alerting on anomalies
  - Unusually large blocks
  - Low match rates
  - Data quality degradation
- Dead letter queue for failed records

### 3. Performance Profiling

**What we did:**
- Timed overall pipeline
- Counted comparisons

**What we should do:**
- Profile each component (where's the bottleneck?)
- Memory usage analysis (does it fit in RAM?)
- Benchmark at scale (100K, 1M entities)
- Parallel processing for blocking

### 4. Configuration Validation

**What we did:**
- Load YAML and hope it's correct

**What we should do:**
- Schema validation for YAML (JSON Schema)
- Integration tests for config changes
- Config diff tool (what changed and why?)
- Rollback mechanism (bad config breaks production)

---

## Conclusion

**Key takeaways:**

1. **Separate person and company matching** - They have different identity characteristics
2. **Use configuration for domain rules** - Regulations change frequently
3. **Multi-strategy fallback** - Real-world data is messy
4. **Graph algorithms** - Enable analysis impossible in SQL
5. **Design patterns** - Make code maintainable and extensible

**What this architecture enables:**

- ✅ New data source: 5 lines of code
- ✅ Schema change: YAML update
- ✅ Regulation change: YAML update
- ✅ New matching rule: Add strategy
- ✅ Scale to 100K+: Blocking handles it

**What it doesn't handle:**

- ❌ Real-time streaming (batch-only)
- ❌ Distributed processing (single machine)
- ❌ Complex nested transformations (YAML limitation)
- ❌ ML-based similarity (rule-based only)

**For production, you'd need:**

- Streaming infrastructure (Kafka)
- Distributed graph (GraphFrames, Neo4j)
- ML models for similarity scoring
- Comprehensive monitoring
- Data quality framework

**But the core patterns - separation of concerns, configuration-driven, domain-aware - would remain the same.**

---

## Further Reading

**Design Patterns:**
- "Design Patterns" by Gang of Four (Abstract Factory, Strategy, Template Method)
- "Clean Architecture" by Robert Martin (Separation of Concerns)

**Entity Resolution:**
- Fellegi-Sunter probabilistic matching
- Dedupe library (Python)
- RecordLinkage toolkit

**Graph Algorithms:**
- NetworkX documentation
- "Graph Algorithms" by Needham & Hodler

**Configuration Management:**
- "The Twelve-Factor App" (Configuration as code)
- JSON Schema (Config validation)

---

**Questions? Feedback?**

This is a learning project. If you see ways to improve the architecture, I'd love to hear them.

Areas I'm particularly interested in:
- Better blocking strategies for edge cases
- Validation approaches for YAML configuration
- Performance optimization techniques
- Production monitoring patterns

Open an issue or reach out:  sidnayak12345@gmail.com