# SINT Layer: Semantic Web & Knowledge Graphs

*How OWL ontologies, RDF triples, and SPARQL queries solve real AML compliance problems*

---

## What You'll Learn

This README explores the **SINT** (Semantic Integration) layer - how we transform a property graph (NetworkX) into a knowledge graph (RDF) using semantic web standards.

You'll learn:

1. **Why RDF over JSON** - Type safety, reasoning, and standards
2. **OWL Ontology Design** - How we model domain knowledge
3. **Property Chains** - Automatic multi-hop query inference
4. **Reification** - Modeling complex relationships with metadata
5. **SPARQL Queries** - Solving real AML compliance problems
6. **Integration Pattern** - How UBOGraph structure maps to RDF

**Why this matters:** Most graph databases store data without semantics. RDF + OWL gives you a **formal model** that can be reasoned over, validated, and queried with standard languages.

---

## Table of Contents

- [Why Semantic Web?](#why-semantic-web)
- [RDF Basics](#rdf-basics)
- [OWL Ontology Design](#owl-ontology-design)
  - [Class Hierarchy](#class-hierarchy)
  - [Property Chains](#property-chains)
  - [Auto-Classification](#auto-classification)
  - [Reified Relationships](#reified-relationships)
- [Integration with UBOGraph](#integration-with-ubograph)
- [SPARQL Queries](#sparql-queries)
- [Best Practices](#best-practices)
- [Lessons Learned](#lessons-learned)

---

## Why Semantic Web?

### The JSON Problem

Traditional approach: Store graph as JSON.

```json
{
  "person": {
    "name": "David O'Connor",
    "is_pep": "true",
    "owns": [
      {"company": "Offshore BVI", "percentage": 100}
    ]
  }
}
```

**Problems:**

**1. No type safety:**
```javascript
// JavaScript treats string "true" as truthy
if (person.is_pep) {  // ALWAYS TRUE! Even if "false"
  flag_for_review()
}
```

**2. No formal semantics:**
- Is "owns" transitive? (If A owns B and B owns C, does A own C?)
- Can a company own itself? (Circular ownership possible?)
- Is "is_pep" functional? (Can someone be both PEP and non-PEP?)

You have to remember these rules. The system doesn't know them.

**3. No reasoning:**
```json
{
  "entity": "Shell Co",
  "is_offshore": true,
  "employee_count": 0
}
```

Is this a shell company? You have to write code to check.

```python
# Hardcoded logic
def is_shell(company):
    return company['is_offshore'] and company['employee_count'] == 0
```

Change rule → change code → deploy.

### The RDF/OWL Solution

**1. Type safety (data types enforced):**

```turtle
entity:GOLDEN_001 ubo:isPEP true .  # ← xsd:boolean, not string

# Ontology declares type
ubo:isPEP a owl:DatatypeProperty, owl:FunctionalProperty ;
    rdfs:range xsd:boolean .

# Reasoner validates:
# ✓ Type is boolean (not string)
# ✓ Only one value (functional property)
# ✗ Rejects: ubo:isPEP "true" (string, not boolean)
```

**2. Formal semantics (relationships defined):**

```turtle
# Ontology declares transitivity
ubo:controls a owl:TransitiveProperty ;
    owl:propertyChainAxiom ( ubo:owns ubo:owns ) .

# Data:
person:A ubo:owns company:B .
company:B ubo:owns company:C .

# Reasoner infers (automatically!):
person:A ubo:controls company:B .
person:A ubo:controls company:C .

# No code! Just data + ontology.
```

**3. Automatic reasoning (rules in ontology):**

```turtle
# Ontology defines shell company
ubo:ShellCompany owl:equivalentClass [
    a owl:Class ;
    owl:intersectionOf (
        ubo:OffshoreCompany
        [ owl:onProperty ubo:employeeCount ;
          owl:maxInclusive 0 ]
    )
] .

# Data:
company:X a ubo:OffshoreCompany ;
    ubo:employeeCount 0 .

# Reasoner infers:
company:X a ubo:ShellCompany .  # Automatic!

# Change rule? Update ontology (data, not code).
```

**This is why banks and governments use RDF for regulatory data.**

---

## RDF Basics

### The Triple Model

Everything is a triple: `<subject> <predicate> <object>`

```turtle
entity:GOLDEN_001 a ubo:Person .
                  ↑    ↑
              subject predicate object
```

**Example entity:**

```turtle
entity:GOLDEN_001 a ubo:Person ;
    ubo:hasName "David O'Connor" ;
    ubo:dateOfBirth "1982-03-17"^^xsd:date ;
    ubo:hasCountry "Ireland" ;
    ubo:isPEP true ;
    ubo:pepLevel "minister" ;
    ubo:owns entity:GOLDEN_002 .
```

Breaks down to 6 triples:

```
<entity:GOLDEN_001> <rdf:type> <ubo:Person>
<entity:GOLDEN_001> <ubo:hasName> "David O'Connor"
<entity:GOLDEN_001> <ubo:dateOfBirth> "1982-03-17"^^xsd:date
<entity:GOLDEN_001> <ubo:hasCountry> "Ireland"
<entity:GOLDEN_001> <ubo:isPEP> true
<entity:GOLDEN_001> <ubo:owns> <entity:GOLDEN_002>
```

### Why Triples?

**1. Graph-native:**
Each triple is an edge: `subject --predicate--> object`

**2. Extensible:**
Add new predicate without schema change:
```turtle
entity:GOLDEN_001 ubo:newProperty "value" .  # Just add it
```

**3. Queryable:**
SPARQL queries are pattern matching on triples.

**4. Merge-friendly:**
Triples from different sources combine automatically:
```turtle
# Source 1
entity:GOLDEN_001 ubo:hasName "David O'Connor" .

# Source 2
entity:GOLDEN_001 ubo:isPEP true .

# Merged automatically (same subject)
```

---

## OWL Ontology Design

Our ontology (`ontology/ubo_ontology.ttl`) defines the domain model.

### Class Hierarchy

**Basic structure:**

```turtle
ubo:Entity a owl:Class .

ubo:Person a owl:Class ;
    rdfs:subClassOf ubo:Entity .

ubo:Company a owl:Class ;
    rdfs:subClassOf ubo:Entity .

# Disjoint: Can't be both person AND company
[] a owl:AllDisjointClasses ;
   owl:members ( ubo:Person ubo:Company ) .
```

**Why disjoint matters:**

```turtle
# Invalid data
entity:X a ubo:Person, ubo:Company .  # ✗ Reasoner rejects

# Reasoner detects inconsistency
# This prevents data quality errors
```

### Auto-Classification

**Defining PEPs:**

```turtle
ubo:PoliticallyExposedPerson owl:equivalentClass [
    a owl:Class ;
    owl:intersectionOf (
        ubo:Person
        [ a owl:Restriction ;
          owl:onProperty ubo:isPEP ;
          owl:hasValue true ]
    )
] .
```

**What this means:**

```turtle
# You write:
entity:GOLDEN_001 a ubo:Person ;
    ubo:isPEP true .

# Reasoner infers:
entity:GOLDEN_001 a ubo:PoliticallyExposedPerson .

# Automatic classification! No code needed.
```

**Defining shell companies:**

```turtle
ubo:ShellCompany owl:equivalentClass [
    a owl:Class ;
    owl:intersectionOf (
        ubo:OffshoreCompany
        [ a owl:Restriction ;
          owl:onProperty ubo:employeeCount ;
          owl:maxInclusive 0 ]
    )
] .
```

**In practice:**

```turtle
# Data
company:X a ubo:OffshoreCompany ;
    ubo:employeeCount 0 .

# Reasoner infers
company:X a ubo:ShellCompany .

# SPARQL query
SELECT ?shell WHERE {
  ?shell a ubo:ShellCompany .
}
# Returns company:X automatically!
```

**Why this is powerful:**

- ✅ Business rule in ontology (not code)
- ✅ Change rule = edit ontology (not deploy code)
- ✅ Explainable (regulator can read ontology)
- ✅ Verifiable (reasoner checks consistency)

### Property Chains

**The problem:**

```sql
-- SQL: Find ultimate beneficial owners (any depth)
WITH RECURSIVE ownership AS (
  SELECT owner_id, company_id, 1 as depth
  FROM stakes
  WHERE owner_type = 'PERSON'
  
  UNION ALL
  
  SELECT o.owner_id, s.company_id, o.depth + 1
  FROM ownership o
  JOIN stakes s ON o.company_id = s.owner_id
  WHERE o.depth < 10  -- Arbitrary limit!
)
SELECT * FROM ownership;
```

50+ lines, depth-limited, breaks on cycles.

**The OWL solution:**

```turtle
# Define transitive control
ubo:controls a owl:ObjectProperty, owl:TransitiveProperty ;
    owl:propertyChainAxiom ( ubo:owns ubo:owns ) .
```

**What this means:**

```turtle
# Data
person:A ubo:owns company:B .
company:B ubo:owns company:C .
company:C ubo:owns company:D .

# Reasoner infers (automatically!):
person:A ubo:controls company:B .
person:A ubo:controls company:C .
person:A ubo:controls company:D .

# SPARQL query
SELECT ?company WHERE {
  person:A ubo:controls ?company .
}
# Returns B, C, D - no recursion needed!
```

**Property chain axiom explained:**

```turtle
owl:propertyChainAxiom ( ubo:owns ubo:owns )
```

Means: `ubo:controls` is equivalent to `ubo:owns` followed by `ubo:owns` (any number of times).

So:
- `A owns B` + `B owns C` → `A controls C`
- `A owns B` + `B owns C` + `C owns D` → `A controls D`
- etc.

**Why this is revolutionary:**

- ✅ 5 lines OWL vs 50+ lines SQL
- ✅ No depth limit (handles any chain length)
- ✅ Handles cycles (doesn't infinite loop)
- ✅ Standard (W3C spec, works in any reasoner)

### Reified Relationships

**The problem:** Relationships have metadata.

Ownership has:
- Percentage (25%, 50%, 100%)
- Type (direct, beneficial, nominee)
- Date acquired
- Source of information

Can't express in simple triple:
```turtle
person:A ubo:owns company:B .  # Where's the percentage?
```

**The solution: Reification**

Create an intermediate object representing the relationship:

```turtle
# Simple property (for basic queries)
person:A ubo:owns company:B .

# Reified stake (for detailed queries)
person:A ubo:hasStake stake:1 .

stake:1 a ubo:OwnershipStake ;
    ubo:inCompany company:B ;
    ubo:percentage 75.5 ;
    ubo:ownershipType "beneficial" ;
    prov:generatedAtTime "2024-01-15T10:00:00Z"^^xsd:dateTime ;
    prov:wasDerivedFrom entity:RECORD_123 .
```

**Pattern:**

```
Simple:     A --owns--> B
           
Reified:    A --hasStake--> [Stake] --inCompany--> B
                              ↓
                         percentage: 75.5
                         type: beneficial
                         date: 2024-01-15
```

**Why both?**

- **Simple property:** Fast queries ("who owns what?")
- **Reified property:** Detailed queries ("who owns >25%?")

**Example queries:**

```sparql
# Simple: All ownership (fast)
SELECT ?person ?company WHERE {
  ?person ubo:owns ?company .
}

# Detailed: Ownership >25% with dates
SELECT ?person ?company ?percentage ?date WHERE {
  ?person ubo:hasStake ?stake .
  ?stake ubo:inCompany ?company ;
         ubo:percentage ?percentage ;
         prov:generatedAtTime ?date .
  FILTER(?percentage > 25)
}
```

**Same pattern for directorships:**

```turtle
# Simple
person:A ubo:directorOf company:B .

# Reified
person:A ubo:hasAppointment appointment:1 .

appointment:1 a ubo:DirectorshipAppointment ;
    ubo:atCompany company:B ;
    ubo:role "CEO" ;
    ubo:appointedOn "2020-05-01"^^xsd:date ;
    ubo:isActive true .
```

---

## Integration with UBOGraph

### How NetworkX Maps to RDF

**NetworkX structure:**

```python
G = nx.MultiDiGraph()

# Add node
G.add_node('GOLDEN_001', 
    full_name='David O\'Connor',
    entity_type='person',
    is_pep=True,
    pep_level='minister')

# Add edge
G.add_edge('GOLDEN_001', 'GOLDEN_002',
    relationship_type='owns',
    ownership_percentage=75.5)
```

**Exported to RDF:**

```turtle
# Node → Entity
entity:GOLDEN_001 a ubo:Person ;
    ubo:hasName "David O'Connor" ;
    ubo:isPEP true ;
    ubo:pepLevel "minister" .

# Edge → Simple property
entity:GOLDEN_001 ubo:owns entity:GOLDEN_002 .

# Edge → Reified property
entity:GOLDEN_001 ubo:hasStake _:stake1 .
_:stake1 a ubo:OwnershipStake ;
    ubo:inCompany entity:GOLDEN_002 ;
    ubo:percentage 75.5 .
```

### Export Process

**From `ubo_graph.py`:**

```python
def export_rdf(self, output_path: str):
    """Export graph to RDF/Turtle format"""
    
    from rdflib import Graph, Namespace, Literal, URIRef
    from rdflib.namespace import RDF, XSD
    
    # Initialize RDF graph
    rdf = Graph()
    
    UBO = Namespace("http://example.org/ubo#")
    ENTITY = Namespace("http://example.org/entity/")
    
    # ─────────────────────────────────────────────────
    # STEP 1: Export nodes as entities
    # ─────────────────────────────────────────────────
    for node_id, data in self.G.nodes(data=True):
        entity_uri = ENTITY[node_id]
        
        # rdf:type based on entity_type
        if data.get('entity_type') == 'person':
            rdf.add((entity_uri, RDF.type, UBO.Person))
        else:
            rdf.add((entity_uri, RDF.type, UBO.Company))
        
        # Add properties
        if 'full_name' in data:
            rdf.add((entity_uri, UBO.hasName, 
                     Literal(data['full_name'])))
        
        if 'dob' in data:
            rdf.add((entity_uri, UBO.dateOfBirth, 
                     Literal(data['dob'], datatype=XSD.date)))
        
        if 'is_pep' in data:
            rdf.add((entity_uri, UBO.isPEP, 
                     Literal(data['is_pep'], datatype=XSD.boolean)))
    
    # ─────────────────────────────────────────────────
    # STEP 2: Export edges as relationships
    # ─────────────────────────────────────────────────
    for source, target, data in self.G.edges(data=True):
        source_uri = ENTITY[source]
        target_uri = ENTITY[target]
        
        rel_type = data.get('relationship_type')
        
        if rel_type == 'owns':
            # Simple property
            rdf.add((source_uri, UBO.owns, target_uri))
            
            # Reified property (if has percentage)
            if 'ownership_percentage' in data:
                stake = BNode()
                rdf.add((source_uri, UBO.hasStake, stake))
                rdf.add((stake, RDF.type, UBO.OwnershipStake))
                rdf.add((stake, UBO.inCompany, target_uri))
                rdf.add((stake, UBO.percentage, 
                         Literal(data['ownership_percentage'],
                                datatype=XSD.decimal)))
        
        elif rel_type == 'directorOf':
            # Simple property
            rdf.add((source_uri, UBO.directorOf, target_uri))
            
            # Reified property
            appointment = BNode()
            rdf.add((source_uri, UBO.hasAppointment, appointment))
            rdf.add((appointment, RDF.type, UBO.DirectorshipAppointment))
            rdf.add((appointment, UBO.atCompany, target_uri))
    
    # ─────────────────────────────────────────────────
    # STEP 3: Serialize to Turtle
    # ─────────────────────────────────────────────────
    rdf.serialize(output_path, format='turtle')
```

**Key decisions:**

**1. Both simple and reified:**
- Simple for basic queries (fast)
- Reified for detailed queries (metadata)

**2. Blank nodes for reification:**
- Stakes/appointments are intermediate objects
- Don't need global IDs
- BNode() creates anonymous identifier

**3. Typed literals:**
- Dates: `xsd:date`
- Booleans: `xsd:boolean`
- Decimals: `xsd:decimal`

**4. Property paths enabled:**
- Simple `ubo:owns` enables `ubo:owns+` queries
- Reasoner can infer `ubo:controls`

---

## SPARQL Queries

### Query 1: Ultimate Beneficial Ownership

**Business question:** "Who ultimately owns this company?"

**Regulatory context:** FATF Recommendation 24 requires identifying UBOs with >25% control through ANY chain.

**SPARQL:**

```sparql
PREFIX ubo: <http://example.org/ubo#>

# Find all persons who own/control this company
SELECT ?personName ?pathLength WHERE {
  # Start with any person
  ?person a ubo:Person ;
          ubo:hasName ?personName .
  
  # Multi-hop ownership (1+ hops)
  ?person ubo:owns+ entity:TARGET_COMPANY .
  
  # Optional: Calculate path length
  OPTIONAL {
    ?person ubo:owns{1,10} entity:TARGET_COMPANY .
    BIND(1 as ?pathLength)
  }
}
```

**Property path magic:**

- `ubo:owns+` = one or more hops
- `ubo:owns*` = zero or more hops
- `ubo:owns{1,5}` = 1 to 5 hops
- `ubo:owns{3}` = exactly 3 hops

**Why better than SQL:**

```sql
-- SQL needs recursive CTE (50+ lines)
WITH RECURSIVE ownership AS (...)

-- SPARQL: 5 lines with property paths
?person ubo:owns+ ?company .
```

### Query 2: PEP Offshore Exposure

**Business question:** "Does this PEP own offshore shell companies?"

**Regulatory context:** 5AMLD Enhanced Due Diligence for PEPs.

**SPARQL:**

```sparql
PREFIX ubo: <http://example.org/ubo#>

SELECT ?pepName ?offshoreName ?isShell WHERE {
  # Find all PEPs (automatic classification!)
  ?pep a ubo:PoliticallyExposedPerson ;
       ubo:hasName ?pepName .
  
  # Follow ownership (multi-hop)
  ?pep ubo:owns+ ?offshore .
  
  # Filter for offshore companies
  ?offshore a ubo:OffshoreCompany ;
            ubo:hasName ?offshoreName .
  
  # Check if shell company (auto-classified!)
  BIND(EXISTS { ?offshore a ubo:ShellCompany } as ?isShell)
}
```

**OWL reasoning benefits:**

```turtle
# You don't write:
SELECT ?pep WHERE {
  ?pep ubo:isPEP true .  # Manual filter
}

# You write:
SELECT ?pep WHERE {
  ?pep a ubo:PoliticallyExposedPerson .  # Auto-classified!
}

# Reasoner handles the logic
```

### Query 3: Circular Ownership

**Business question:** "Find fraud structures (companies owning themselves)."

**SPARQL:**

```sparql
PREFIX ubo: <http://example.org/ubo#>

# Find companies that own themselves through any chain
SELECT ?companyName WHERE {
  # Company owns itself (circular path)
  ?company ubo:owns+ ?company ;
           ubo:hasName ?companyName .
}
```

**SQL equivalent:** Infinite loop! 💥

**SPARQL handles cycles naturally.**

### Query 4: Ownership Percentage Threshold

**Business question:** "Who owns >25% (UBO threshold)?"

**Uses reified properties:**

```sparql
PREFIX ubo: <http://example.org/ubo#>

SELECT ?personName ?companyName ?percentage WHERE {
  ?person a ubo:Person ;
          ubo:hasName ?personName ;
          ubo:hasStake ?stake .
  
  ?stake ubo:inCompany ?company ;
         ubo:percentage ?percentage .
  
  ?company ubo:hasName ?companyName .
  
  # UBO threshold
  FILTER(?percentage > 25)
}
```

**Why reification matters:**

Simple property (`ubo:owns`) doesn't have percentage.
Reified property (`ubo:hasStake`) includes all metadata.

### Query 5: Provenance Tracking

**Business question:** "When did we learn this person is a PEP?"

**Uses PROV-O:**

```sparql
PREFIX ubo: <http://example.org/ubo#>
PREFIX prov: <http://www.w3.org/ns/prov#>

SELECT ?name ?pepLevel ?source ?timestamp WHERE {
  ?person a ubo:PoliticallyExposedPerson ;
          ubo:hasName ?name ;
          ubo:pepLevel ?pepLevel ;
          prov:wasDerivedFrom ?sourceRecord .
  
  ?sourceRecord prov:atLocation ?source ;
                prov:generatedAtTime ?timestamp .
}
ORDER BY ?timestamp
```

**Regulatory compliance:**

Auditor asks: "How do you know David O'Connor is a minister?"

Answer: Query shows:
- Source: PEP Registry
- Timestamp: 2024-03-16T09:00:00Z
- Record ID: BATCH4_012

Full audit trail, automatic.

---

## Best Practices

### 1. Use Standard Vocabularies

**Good:**
```turtle
@prefix prov: <http://www.w3.org/ns/prov#> .

entity:X prov:wasDerivedFrom entity:Y .
entity:X prov:generatedAtTime "2024-01-01"^^xsd:dateTime .
```

**Why:** PROV-O is W3C standard. Tools understand it.

**Bad:**
```turtle
entity:X custom:sourceRecord entity:Y .  # Non-standard
entity:X custom:timestamp "2024-01-01" .  # Non-standard
```

**Why:** Custom properties require custom tools.

### 2. Type Your Literals

**Good:**
```turtle
entity:X ubo:dateOfBirth "1982-03-17"^^xsd:date .
entity:X ubo:isPEP true .  # xsd:boolean implied
entity:X ubo:percentage 75.5 .  # xsd:decimal implied
```

**Bad:**
```turtle
entity:X ubo:dateOfBirth "1982-03-17" .  # Just string
entity:X ubo:isPEP "true" .  # String, not boolean!
```

**Why:** Reasoners validate types. Queries filter correctly.

### 3. Both Simple and Reified

**Pattern:**
```turtle
# Simple (for basic queries)
person:A ubo:owns company:B .

# Reified (for detailed queries)
person:A ubo:hasStake stake:1 .
stake:1 ubo:inCompany company:B ;
        ubo:percentage 75 .
```

**Why:** Simple properties enable property paths. Reified properties add metadata.

### 4. Functional vs Non-Functional

**Functional property (exactly one value):**
```turtle
ubo:dateOfBirth a owl:FunctionalProperty .

# Valid
person:X ubo:dateOfBirth "1982-03-17"^^xsd:date .

# Invalid (two DOBs!)
person:X ubo:dateOfBirth "1982-03-17"^^xsd:date ,
                         "1982-03-18"^^xsd:date .
# Reasoner detects inconsistency
```

**Non-functional (multiple values OK):**
```turtle
ubo:hasStake a owl:ObjectProperty .  # NOT functional

# Valid (multiple stakes)
person:X ubo:hasStake stake:1, stake:2, stake:3 .
```

### 5. Disjoint Classes

**Prevent invalid data:**
```turtle
[] a owl:AllDisjointClasses ;
   owl:members ( ubo:Person ubo:Company ) .

# Invalid
entity:X a ubo:Person, ubo:Company .  # Can't be both!

# Reasoner rejects
```

---

## Lessons Learned

### 1. Reasoning Has Cost

**HermiT reasoner on 10K triples:**
- Time: ~5 seconds
- Memory: ~500MB

**On 100K triples:**
- Time: ~2 minutes
- Memory: ~4GB

**On 1M triples:**
- Time: Hours
- Memory: Out of memory

**Lesson:** Reasoning doesn't scale linearly. For large graphs:
- Use rule engines (Jena, GraphDB)
- Materialize inferences
- Partition data

### 2. SPARQL != SQL

**SQL thinking:**
```sparql
# ❌ Trying to use SQL patterns
SELECT ?person WHERE {
  ?person ubo:isPEP ?pep_status .
  FILTER(?pep_status = true)
}
```

**SPARQL thinking:**
```sparql
# ✅ Use patterns and inference
SELECT ?person WHERE {
  ?person a ubo:PoliticallyExposedPerson .
}
```

**Lesson:** Trust the reasoner. Write declarative patterns, not imperative filters.

### 3. Blank Nodes for Reification

**Good:**
```turtle
person:A ubo:hasStake _:stake1 .
_:stake1 ubo:inCompany company:B ;
         ubo:percentage 75 .
```

**Why:** Blank nodes (BNodes) don't need global IDs.

**When NOT to use:**
```turtle
# ❌ Don't use for entities that might be referenced externally
_:company1 ubo:hasName "Acme Corp" .

# ✅ Use URIs for entities
entity:GOLDEN_001 ubo:hasName "Acme Corp" .
```

### 4. Property Chains vs Reasoning

**Property chains (fast):**
```turtle
ubo:controls owl:propertyChainAxiom ( ubo:owns ubo:owns ) .

# Query works immediately
SELECT ?company WHERE {
  person:A ubo:controls ?company .
}
```

**Transitive property (requires reasoning):**
```turtle
ubo:owns a owl:TransitiveProperty .

# Need to run reasoner first
# Then inferred triples exist
```

**Lesson:** Property chains are declarative. Reasoner must materialize them.

### 5. Validation vs Inference

**Validation (SHACL):**
```turtle
# Check: Every person must have exactly one DOB
:PersonShape a sh:NodeShape ;
    sh:targetClass ubo:Person ;
    sh:property [
        sh:path ubo:dateOfBirth ;
        sh:minCount 1 ;
        sh:maxCount 1 ;
    ] .
```

**Inference (OWL):**
```turtle
# Infer: Person + isPEP=true → PoliticallyExposedPerson
ubo:PoliticallyExposedPerson owl:equivalentClass ...
```

**Lesson:** Use both. OWL infers new knowledge. SHACL validates data quality.

---

## What We'd Do Differently

### 1. SHACL Validation

**What we did:**
- Ontology defines model
- Trust data is valid

**What we should do:**
- SHACL shapes validate data
- Catch errors before reasoning
- Report validation errors

**Example:**
```turtle
:PersonShape a sh:NodeShape ;
    sh:targetClass ubo:Person ;
    sh:property [
        sh:path ubo:dateOfBirth ;
        sh:datatype xsd:date ;  # Must be date
        sh:minCount 1 ;          # Required
        sh:maxCount 1 ;          # Only one
    ] .
```

### 2. Materialization Strategy

**What we did:**
- Export RDF
- Load in GraphDB
- Run reasoner

**What we should do:**
- Materialize common inferences
- Store as asserted triples
- Re-reason only on updates

**Why:** Reasoning is expensive. Materialize once, query many times.

### 3. SPARQL Query Optimization

**What we did:**
- Write queries that work
- Hope they're fast

**What we should do:**
- Use EXPLAIN to see query plan
- Add hints for optimizer
- Profile slow queries
- Create indexes on common patterns

### 4. Versioning

**What we did:**
- Single graph
- Updates overwrite

**What we should do:**
- Named graphs for versions
- Temporal queries (what was true when?)
- Track provenance of inferences

**Example:**
```turtle
# Data in different graphs
GRAPH <urn:batch:1> {
  person:X ubo:isPEP false .
}

GRAPH <urn:batch:2> {
  person:X ubo:isPEP true .
}

# Query specific version
SELECT ?isPep FROM <urn:batch:2> WHERE {
  person:X ubo:isPEP ?isPep .
}
```

---

## Conclusion

**Key takeaways:**

1. **RDF gives formal semantics** - Not just data, but meaning
2. **OWL enables reasoning** - Infer new knowledge automatically
3. **SPARQL is declarative** - Pattern matching, not imperative code
4. **Standards matter** - W3C specs work everywhere
5. **Reification adds metadata** - When simple triples aren't enough

**What semantic web enables:**

- ✅ Type safety (reasoner validates)
- ✅ Automatic inference (rules in ontology)
- ✅ Standard queries (SPARQL works anywhere)
- ✅ Formal semantics (no ambiguity)
- ✅ Provenance tracking (PROV-O)

**What it doesn't handle:**

- ❌ Doesn't scale to billions of triples (reasoning expensive)
- ❌ Learning curve steep (OWL is complex)
- ❌ Tooling less mature than SQL databases
- ❌ Performance unpredictable (query optimization hard)

**For production:**

- Use rule engines (Jena, GraphDB) not reasoners
- Materialize common inferences
- Partition large graphs
- Monitor query performance
- Validate with SHACL

**But the core idea - formal semantics, automatic reasoning, standard languages - makes RDF powerful for regulatory data.**

---

## Further Reading

**RDF & SPARQL:**
- "Learning SPARQL" by Bob DuCharme
- W3C RDF Primer
- SPARQL 1.1 Query Language

**OWL:**
- "A Semantic Web Primer" by Antoniou & van Harmelen
- OWL 2 Web Ontology Language Primer
- Protégé ontology editor

**Provenance:**
- PROV-O: The PROV Ontology
- W3C PROV family of documents

**Validation:**
- SHACL (Shapes Constraint Language)
- ShEx (Shape Expressions)

**Tools:**
- GraphDB (semantic database)
- Apache Jena (Java framework)
- RDFLib (Python library)
- Protégé (ontology editor)

---

**Questions? Feedback?**

This is a learning project. If you see better ways to model the domain or structure queries, I'd love to hear.

Areas I'm particularly interested in:
- Performance optimization techniques
- Scalability strategies for large graphs
- Alternative modeling approaches
- Production deployment patterns

Open an issue or reach out: sidnayak12345@gmail.com