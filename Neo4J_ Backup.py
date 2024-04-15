from neo4j import GraphDatabase
import json
import time

class Neo4jBackup:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def _serialize_node(self, node):
        serialized_node = {
            'id': node.id,
            'labels': list(node.labels),
            'properties': dict(node.items())
        }
        return serialized_node

    def backup_nodes(self, node_type, step_size):
        with self._driver.session() as session:
            skip = 0
            nodes = []
            nodes_remaining = True
            while nodes_remaining:
                try:
                    result = session.run(
                        f'MATCH (n:{node_type}) RETURN n SKIP $skip LIMIT $limit',
                        node_type=node_type,
                        skip=skip,
                        limit=step_size
                    )

                    nodes_remaining = False
                    for record in result:
                        nodes_remaining = True
                        nodes.append(self._serialize_node(record['n']))

                    skip += step_size

                except Exception as e:
                    print(f"An error occurred: {str(e)}")
                    print("Retrying in 5 seconds...")
                    time.sleep(5) 
                    
            filename = f'{node_type}_nodes.json'
            with open(filename, 'w') as file:
                json.dump(nodes, file, indent=4)
            print(f'Backup {filename} created.')

    def _serialize_edge(self, relationship):
        serialized_edge = {
            'id': relationship.id,
            'type': relationship.type,
            'start_node_id': relationship.start_node.id,
            'end_node_id': relationship.end_node.id,
            'properties': dict(relationship.items())
        }
        return serialized_edge

    def backup_edges(self, step_size):
        with self._driver.session() as session:
            skip = 0
            edges_remaining = True
            while edges_remaining:
                try:
                    result = session.run(
                        'MATCH ()-[r]->() RETURN r SKIP $skip LIMIT $limit',
                        skip=skip,
                        limit=step_size
                    )

                    edges_remaining = False
                    edges = []  
                    for record in result:
                        edges_remaining = True
                        edges.append(self._serialize_edge(record['r']))
                    

                    filename = f'Neo4J_edges_{skip}.json'  
                    with open(filename, 'w') as file:
                        json.dump(edges, file, indent=4)
                    print(f'Backup {filename} created.')

                    skip += step_size

                except Exception as e:
                    print(f"An error occurred: {str(e)}")
                    print("Retrying in 5 seconds...")
                    time.sleep(5)

neo4j_uri = "neo4j+ssc://v17.cl.uni-heidelberg.de:7687"
neo4j_user = "username"
neo4j_password = "password"

step_size = 200000

backup = Neo4jBackup(neo4j_uri, neo4j_user, neo4j_password)

node_types = ['CommonSenseConnection', 'User', 'amr', 'argument', 'argument_structure', 'argument_unit', 'topic']
for node_type in node_types:
    backup.backup_nodes(node_type, step_size)

backup.backup_edges(step_size)

backup.close()
