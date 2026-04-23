"""
Entity Relationship Graph - Lightweight in-memory graph database.
Fixed with auto schema detection.
"""

import json
import logging
from typing import Dict, List, Set, Tuple, Optional, Any
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


class EntityGraph:
    """In-memory entity relationship graph."""
    
    def __init__(self):
        self.nodes: Dict[str, Dict] = {}
        self.edges: List[Dict] = []
        self.adjacency: Dict[str, Set[str]] = defaultdict(set)
    
    def add_node(self, entity_id: str, entity_type: str, label: str, **properties) -> str:
        """Add or update a node."""
        node_id = f"{entity_type}:{label}"
        
        if node_id not in self.nodes:
            self.nodes[node_id] = {
                'id': node_id,
                'label': label,
                'type': entity_type,
                'frequency': 1,
                **properties
            }
        else:
            self.nodes[node_id]['frequency'] += 1
        
        return node_id
    
    def add_edge(self, source: str, target: str, weight: float = 1.0) -> None:
        """Add or update an edge."""
        if source not in self.nodes or target not in self.nodes:
            return
        
        edge_id = f"{source}--{target}"
        
        for edge in self.edges:
            if edge['source'] == source and edge['target'] == target:
                edge['weight'] += weight
                return
        
        self.edges.append({
            'source': source,
            'target': target,
            'weight': weight
        })
        
        self.adjacency[source].add(target)
        self.adjacency[target].add(source)
    
    def extract_co_occurrence(self, entities: List[Tuple[str, str]]) -> int:
        """Extract co-occurrence relationships."""
        # Add nodes
        node_ids = []
        for text, etype in entities:
            node_id = self.add_node(text, etype, text)
            node_ids.append(node_id)
        
        # Create edges
        edge_count = 0
        for i, source in enumerate(node_ids):
            for target in node_ids[i+1:]:
                self.add_edge(source, target)
                edge_count += 1
        
        return edge_count
    
    def get_stats(self) -> Dict:
        """Get graph statistics."""
        node_types = defaultdict(int)
        for node in self.nodes.values():
            node_types[node['type']] += 1
        
        return {
            'total_nodes': len(self.nodes),
            'total_edges': len(self.edges),
            'node_types': dict(node_types)
        }
    
    def get_central_nodes(self, limit: int = 10) -> List[Dict]:
        """Get most central nodes."""
        degrees = {nid: len(adj) for nid, adj in self.adjacency.items()}
        sorted_nodes = sorted(degrees.items(), key=lambda x: x[1], reverse=True)
        
        central = []
        for nid, degree in sorted_nodes[:limit]:
            node = self.nodes[nid].copy()
            node['degree'] = degree
            central.append(node)
        
        return central
    
    def export_cytoscape(self) -> Dict:
        """Export for visualization."""
        elements = []
        
        for node_id, node in self.nodes.items():
            elements.append({
                'data': {
                    'id': node_id,
                    'label': node['label'],
                    'type': node['type'],
                    'frequency': node['frequency']
                }
            })
        
        for edge in self.edges:
            elements.append({
                'data': {
                    'source': edge['source'],
                    'target': edge['target'],
                    'weight': edge['weight']
                }
            })
        
        return {'elements': elements, 'stats': self.get_stats()}


# Global instance
_graph = EntityGraph()


def get_graph() -> EntityGraph:
    return _graph


class GraphService:
    """Service for building entity graph."""
    
    def build_from_database(self, limit: int = 100) -> Dict:
        """Build graph from database with auto schema detection."""
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor()
        
        # Get column names
        cur.execute("SELECT * FROM extracted_entities LIMIT 0")
        columns = [desc[0] for desc in cur.description]
        print(f"Found columns: {columns}")
        
        # Find entity text and type columns
        text_col = next((c for c in columns if 'text' in c.lower() or 'entity' in c.lower()), columns[1] if len(columns) > 1 else None)
        type_col = next((c for c in columns if 'type' in c.lower() or 'label' in c.lower()), columns[2] if len(columns) > 2 else None)
        
        # Find item/document ID column
        id_col = next((c for c in columns if 'item' in c.lower() or 'doc' in c.lower() or c.endswith('_id')), None)
        
        print(f"Using: text={text_col}, type={type_col}, id={id_col}")
        
        # Build query
        if id_col:
            query = f'''
                SELECT {id_col}, {text_col}, {type_col}
                FROM extracted_entities
                LIMIT %s
            '''
        else:
            query = f'''
                SELECT {text_col}, {type_col}
                FROM extracted_entities
                LIMIT %s
            '''
        
        cur.execute(query, (limit,))
        rows = cur.fetchall()
        
        cur.close()
        conn.close()
        
        if not rows:
            return {'documents_processed': 0, 'nodes': 0, 'edges': 0}
        
        # Group by document if ID column exists
        if id_col and len(rows[0]) >= 3:
            doc_entities = defaultdict(list)
            for row in rows:
                doc_id = row[0]
                text = row[1] or 'unknown'
                etype = row[2] or 'ENTITY'
                doc_entities[doc_id].append((text, etype))
        else:
            # Treat all as one document
            doc_entities = {0: [(r[0] or 'unknown', r[1] or 'ENTITY') for r in rows]}
        
        # Extract relationships
        total_edges = 0
        graph = get_graph()
        graph.nodes.clear()
        graph.edges.clear()
        graph.adjacency.clear()
        
        for doc_id, entities in doc_entities.items():
            edges = graph.extract_co_occurrence(entities)
            total_edges += edges
        
        stats = graph.get_stats()
        
        return {
            'documents_processed': len(doc_entities),
            'entities': len(rows),
            'nodes': stats['total_nodes'],
            'edges': stats['total_edges'],
            'node_types': stats['node_types']
        }
    
    def search_entity(self, query: str, limit: int = 10) -> List[Dict]:
        """Search for entities."""
        graph = get_graph()
        results = []
        query_lower = query.lower()
        
        for node_id, node in graph.nodes.items():
            if query_lower in node['label'].lower():
                node_copy = node.copy()
                node_copy['degree'] = len(graph.adjacency[node_id])
                results.append(node_copy)
        
        results.sort(key=lambda x: x['degree'], reverse=True)
        return results[:limit]
