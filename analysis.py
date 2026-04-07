
from neo4j import GraphDatabase
import pandas as pd
import os

# ==============================
# 1. Neo4j Connection
# ==============================
uri = "neo4j://127.0.0.1:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "bioinformatics"))

try:
    with driver.session() as session:
        result = session.run("RETURN 'Neo4j Connected!' AS message")
        print(result.single()['message'])
except Exception as e:
    print("Connection failed:", e)
    exit()

# ==============================
# 2. Node Count Validation
# ==============================
print("\n========== NODE COUNTS ==========")

with driver.session() as session:

    drug_count = session.run("MATCH (d:Drug) RETURN count(d) AS count").single()['count']
    print(f"Total Drugs     : {drug_count}")

    disease_count = session.run("MATCH (ds:Disease) RETURN count(ds) AS count").single()['count']
    print(f"Total Diseases  : {disease_count}")

    gene_count = session.run("MATCH (g:Gene) RETURN count(g) AS count").single()['count']
    print(f"Total Genes     : {gene_count}")

    total_nodes = drug_count + disease_count + gene_count
    print(f"Total Nodes     : {total_nodes}")

# ==============================
# 3. Relationship Count Validation
# ==============================
print("\n========== RELATIONSHIP COUNTS ==========")

with driver.session() as session:

    treats_count = session.run("MATCH ()-[r:TREATS]->() RETURN count(r) AS count").single()['count']
    print(f"TREATS relationships          : {treats_count}")

    assoc_count = session.run("MATCH ()-[r:ASSOCIATED_WITH]->() RETURN count(r) AS count").single()['count']
    print(f"ASSOCIATED_WITH relationships : {assoc_count}")

    total_rel = treats_count + assoc_count
    print(f"Total Relationships           : {total_rel}")

# ==============================
# 4. List All Drugs
# ==============================
print("\n========== ALL DRUGS ==========")

with driver.session() as session:
    results = session.run("MATCH (d:Drug) RETURN d.id AS id, d.name AS name ORDER BY d.id")
    for record in results:
        print(f"  {record['id']} -> {record['name']}")

# ==============================
# 5. List All Diseases
# ==============================
print("\n========== ALL DISEASES ==========")

with driver.session() as session:
    results = session.run("MATCH (ds:Disease) RETURN ds.id AS id, ds.name AS name ORDER BY ds.id")
    for record in results:
        print(f"  {record['id']} -> {record['name']}")

# ==============================
# 6. List All Genes
# ==============================
print("\n========== ALL GENES ==========")

with driver.session() as session:
    results = session.run("MATCH (g:Gene) RETURN g.id AS id, g.name AS name ORDER BY g.id")
    for record in results:
        print(f"  {record['id']} -> {record['name']}")

# ==============================
# 7. Drug -> Disease Relationships
# ==============================
print("\n========== DRUG TREATS DISEASE ==========")

with driver.session() as session:
    results = session.run("""
        MATCH (d:Drug)-[:TREATS]->(ds:Disease)
        RETURN d.name AS Drug, ds.name AS Disease
        ORDER BY d.name
    """)
    for record in results:
        print(f"  {record['Drug']}  --TREATS-->  {record['Disease']}")

# ==============================
# 8. Gene -> Disease Relationships
# ==============================
print("\n========== GENE ASSOCIATED WITH DISEASE ==========")

with driver.session() as session:
    results = session.run("""
        MATCH (g:Gene)-[:ASSOCIATED_WITH]->(ds:Disease)
        RETURN g.name AS Gene, ds.name AS Disease
        ORDER BY g.name
    """)
    for record in results:
        print(f"  {record['Gene']}  --ASSOCIATED_WITH-->  {record['Disease']}")

# ==============================
# 9. Disease Hub Analysis
# ==============================
print("\n========== DISEASE HUB ANALYSIS ==========")
print("(Which disease has most connections?)")

with driver.session() as session:
    results = session.run("""
        MATCH (n)-[r]->(ds:Disease)
        RETURN ds.name AS Disease, count(r) AS Connections
        ORDER BY Connections DESC
    """)
    for record in results:
        print(f"  {record['Disease']}  ->  {record['Connections']} connection(s)")

# ==============================
# 10. Export Summary to CSV
# ==============================
print("\n========== EXPORTING SUMMARY TO CSV ==========")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
summary_data = []

with driver.session() as session:
    results = session.run("""
        MATCH (d:Drug)-[:TREATS]->(ds:Disease)
        RETURN d.id AS source_id, d.name AS source_name,
               'TREATS' AS relationship,
               ds.id AS target_id, ds.name AS target_name
    """)
    for record in results:
        summary_data.append({
            'source_id': record['source_id'],
            'source_name': record['source_name'],
            'relationship': record['relationship'],
            'target_id': record['target_id'],
            'target_name': record['target_name']
        })

    results = session.run("""
        MATCH (g:Gene)-[:ASSOCIATED_WITH]->(ds:Disease)
        RETURN g.id AS source_id, g.name AS source_name,
               'ASSOCIATED_WITH' AS relationship,
               ds.id AS target_id, ds.name AS target_name
    """)
    for record in results:
        summary_data.append({
            'source_id': record['source_id'],
            'source_name': record['source_name'],
            'relationship': record['relationship'],
            'target_id': record['target_id'],
            'target_name': record['target_name']
        })

df = pd.DataFrame(summary_data)
csv_path = os.path.join(BASE_DIR, "knowledge_graph_summary.csv")
df.to_csv(csv_path, index=False)
print(f"Summary exported to: knowledge_graph_summary.csv")
print(df.to_string(index=False))

# ==============================
# Final Summary
# ==============================
print("\n========== FINAL VALIDATION SUMMARY ==========")
print(f"  Drugs          : {drug_count}")
print(f"  Diseases       : {disease_count}")
print(f"  Genes          : {gene_count}")
print(f"  Total Nodes    : {total_nodes}")
print(f"  TREATS         : {treats_count}")
print(f"  ASSOCIATED_WITH: {assoc_count}")
print(f"  Total Rels     : {total_rel}")
print("\nKnowledge Graph Analysis Complete!")

driver.close()
