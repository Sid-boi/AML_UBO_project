all info is present in readme folder
i have divided into 3 parts so that people understand the project and good tech practices that i have used

## Questions This System Can Answer

1. Ultimate Beneficial Ownership
   - "Who ultimately owns Company X?"
   - Uses: SPARQL property paths (ubo:owns+)

2. PEP Exposure
   - "Is this customer connected to a Politically Exposed Person?"
   - Uses: graph traversal across ownership + relationships

3. Circular Ownership Detection
   - "Does any entity own itself indirectly?"
   - Uses: cycle detection via SPARQL paths

4. Cross-Batch Identity Tracking
   - "Did this entity change risk profile over time?"
   - Uses: incremental entity resolution

5. Offshore Risk Analysis
   - "Which entities are connected to offshore companies?"
   - Uses: type-based filtering + graph traversal
have  a nice read.
