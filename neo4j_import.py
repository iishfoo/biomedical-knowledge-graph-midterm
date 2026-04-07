
from neo4j import GraphDatabase
import pandas as pd

# ==============================
# 1️⃣ Neo4j connection
# ==============================
uri = "neo4j://127.0.0.1:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "bioinformatics"))  # replace password

# ==============================
# 2️⃣ Connection test
# ==============================
try:
    with driver.session() as session:
        result = session.run("RETURN 'Neo4j Connected!' AS message")
        print(result.single()['message'])
except Exception as e:
    print("Connection failed:", e)
    exit()

# ==============================
# 3️⃣ Load CSVs
# ==============================
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    drugs        = pd.read_csv(os.path.join(BASE_DIR, "data/raw/drugs.csv"))
    diseases     = pd.read_csv(os.path.join(BASE_DIR, "data/raw/diseases.csv"))
    genes        = pd.read_csv(os.path.join(BASE_DIR, "data/raw/genes.csv"))
    drug_disease = pd.read_csv(os.path.join(BASE_DIR, "data/raw/drug_disease.csv"))
    gene_disease = pd.read_csv(os.path.join(BASE_DIR, "data/raw/gene_disease.csv"))
    print("✅ CSV files loaded successfully!")
except Exception as e:
    print("CSV loading failed:", e)
    exit()
# ==============================
# 4️⃣ Create Nodes
# ==============================
def create_drug(tx, id, name):
    tx.run("MERGE (d:Drug {id:$id, name:$name})", id=id, name=name)

def create_disease(tx, id, name):
    tx.run("MERGE (d:Disease {id:$id, name:$name})", id=id, name=name)

def create_gene(tx, id, name):
    tx.run("MERGE (g:Gene {id:$id, name:$name})", id=id, name=name)

with driver.session() as session:
    for _, row in drugs.iterrows():
        session.execute_write(create_drug, row['id'], row['name'])
    for _, row in diseases.iterrows():
        session.execute_write(create_disease, row['id'], row['name'])
    for _, row in genes.iterrows():
        session.execute_write(create_gene, row['id'], row['name'])
    print("✅ Nodes created successfully!")

# ==============================
# 5️⃣ Create Relationships
# ==============================
def create_treats(tx, drug_id, disease_id):
    tx.run("""
        MATCH (d:Drug {id:$drug_id}), (ds:Disease {id:$disease_id})
        MERGE (d)-[:TREATS]->(ds)
    """, drug_id=drug_id, disease_id=disease_id)

def create_associated(tx, gene_id, disease_id):
    tx.run("""
        MATCH (g:Gene {id:$gene_id}), (ds:Disease {id:$disease_id})
        MERGE (g)-[:ASSOCIATED_WITH]->(ds)
    """, gene_id=gene_id, disease_id=disease_id)

with driver.session() as session:
    for _, row in drug_disease.iterrows():
        session.execute_write(create_treats, row['drug_id'], row['disease_id'])
    for _, row in gene_disease.iterrows():
        session.execute_write(create_associated, row['gene_id'], row['disease_id'])
    print("✅ Relationships created successfully!")

print("🎉 Knowledge Graph fully imported into Neo4j!")