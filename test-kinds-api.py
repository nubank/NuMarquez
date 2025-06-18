#!/usr/bin/env python3
"""
Test script for Data Lineage Kinds API
Demonstrates conversion from traditional Marquez lineage to kinds format
"""

import requests
import json
from typing import Dict, Any
from datetime import datetime

class LineageKindsApiTester:
    """Test client for the Data Lineage Kinds API"""
    
    def __init__(self, base_url: str = "http://localhost:5000"):
        self.traditional_base_url = f"{base_url}/api/v1"
        self.kinds_base_url = f"{base_url}/api/graphs/v1alpha1"
    
    def get_traditional_lineage(self, node_id: str, depth: int = 20) -> Dict[str, Any]:
        """Get lineage using the existing /lineage/direct endpoint"""
        url = f"{self.traditional_base_url}/lineage/direct"
        params = {"nodeId": node_id, "depth": depth}
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    def convert_to_kinds(self, node_id: str, depth: int = 20, target_kind: str = "LineageGraph") -> Dict[str, Any]:
        """Convert traditional lineage to kinds format using the conversion endpoint"""
        url = f"{self.kinds_base_url}/convert/traditional-to-kinds"
        
        payload = {
            "nodeId": node_id,
            "depth": depth,
            "targetKind": target_kind,
            "includeMetadata": True
        }
        
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()
    
    def get_lineage_graph_kind(self, name: str, depth: int = 20) -> Dict[str, Any]:
        """Get a LineageGraph kind directly"""
        url = f"{self.kinds_base_url}/lineage-graphs/{name}"
        params = {"depth": depth}
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    def list_lineage_graphs(self, label_selector: str = None, limit: int = 50) -> Dict[str, Any]:
        """List LineageGraph kinds with optional filtering"""
        url = f"{self.kinds_base_url}/lineage-graphs"
        params = {"limit": limit}
        
        if label_selector:
            params["labelSelector"] = label_selector
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

def run_test_scenarios():
    """Run various test scenarios to validate the kinds API"""
    tester = LineageKindsApiTester()
    
    # Test scenario 1: Basic conversion test
    print("=== Test Scenario 1: Basic Conversion ===")
    test_node_id = "dataset:my-namespace:customer-profile"
    
    try:
        # Get traditional lineage
        print(f"1. Getting traditional lineage for: {test_node_id}")
        traditional = tester.get_traditional_lineage(test_node_id, depth=3)
        print(f"   Traditional graph has {len(traditional.get('graph', []))} nodes")
        
        # Convert to kinds format
        print("2. Converting to LineageGraph kind...")
        conversion_result = tester.convert_to_kinds(test_node_id, depth=3)
        
        kinds_lineage = conversion_result.get('kinds', {})
        conversion_meta = conversion_result.get('conversionMetadata', {})
        
        print(f"   Conversion completed in {conversion_meta.get('conversionDuration', 'unknown')}")
        print(f"   Processed {conversion_meta.get('nodesProcessed', 0)} nodes")
        print(f"   LineageGraph kind created: {kinds_lineage.get('metadata', {}).get('name', 'unknown')}")
        
        # Validate kinds format
        validate_lineage_graph_kind(kinds_lineage)
        
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Request failed: {e}")
    except Exception as e:
        print(f"   ❌ Test failed: {e}")
    
    # Test scenario 2: Test with different data domains
    print("\n=== Test Scenario 2: Data Domain Filtering ===")
    
    try:
        # List lineage graphs for customer domain
        print("1. Listing LineageGraphs for customer domain...")
        customer_graphs = tester.list_lineage_graphs(
            label_selector="data-domain=customer,geo=BR", 
            limit=10
        )
        
        print(f"   Found {len(customer_graphs.get('items', []))} customer domain graphs")
        
        for item in customer_graphs.get('items', [])[:3]:  # Show first 3
            metadata = item.get('metadata', {})
            print(f"   - {metadata.get('name')}: depth={metadata.get('graphDepth')}")
    
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Request failed: {e}")
    
    # Test scenario 3: Governance metadata validation
    print("\n=== Test Scenario 3: Governance Metadata Validation ===")
    
    sample_kinds_lineage = create_sample_lineage_graph_kind()
    print("1. Validating sample LineageGraph kind structure...")
    
    try:
        validate_lineage_graph_kind(sample_kinds_lineage)
        print("   ✅ Sample LineageGraph kind validation passed")
        
        # Pretty print the example
        print("2. Sample LineageGraph kind structure:")
        print(json.dumps(sample_kinds_lineage, indent=2))
        
    except Exception as e:
        print(f"   ❌ Validation failed: {e}")

def validate_lineage_graph_kind(lineage_graph: Dict[str, Any]) -> None:
    """Validate that a LineageGraph kind follows the proper structure"""
    
    # Check required top-level fields
    required_fields = ['apiVersion', 'kind', 'metadata', 'spec']
    for field in required_fields:
        if field not in lineage_graph:
            raise ValueError(f"Missing required field: {field}")
    
    # Validate apiVersion and kind
    if lineage_graph['apiVersion'] != 'graphs/v1alpha1':
        raise ValueError(f"Invalid apiVersion: {lineage_graph['apiVersion']}")
    
    if lineage_graph['kind'] != 'LineageGraph':
        raise ValueError(f"Invalid kind: {lineage_graph['kind']}")
    
    # Validate metadata
    metadata = lineage_graph['metadata']
    if 'name' not in metadata:
        raise ValueError("Missing required metadata.name")
    
    if 'centralNode' in metadata:
        central_node = metadata['centralNode']
        if 'dataGovernance' not in central_node:
            raise ValueError("Missing centralNode.dataGovernance")
        
        governance = central_node['dataGovernance']
        if 'dataDomain' not in governance or 'dataSubdomain' not in governance:
            raise ValueError("Missing required governance fields")
    
    # Validate spec
    spec = lineage_graph['spec']
    if 'nodes' not in spec:
        raise ValueError("Missing spec.nodes")
    
    # Validate each node
    for i, node in enumerate(spec['nodes']):
        required_node_fields = ['nurn', 'name', 'type', 'dataGovernance', 'distanceFromTheCenter']
        for field in required_node_fields:
            if field not in node:
                raise ValueError(f"Missing required field in node {i}: {field}")

def create_sample_lineage_graph_kind() -> Dict[str, Any]:
    """Create a sample LineageGraph kind for testing"""
    
    return {
        "apiVersion": "graphs/v1alpha1",
        "kind": "LineageGraph",
        "metadata": {
            "name": "customer-profile-lineage",
            "graphDepth": 2,
            "centralNode": {
                "dataGovernance": {
                    "geo": "BR",
                    "dataDomain": "customer",
                    "dataSubdomain": "profile"
                },
                "nurn": "nurn:nu:data:metapod:dataset:customer/profile",
                "name": "customer/profile",
                "type": "dataset"
            },
            "labels": {
                "environment": "production",
                "team": "customer-data",
                "data-domain": "customer",
                "geo": "BR"
            },
            "annotations": {
                "cost-center": "engineering",
                "sla-tier": "gold",
                "marquez.source-endpoint": "/api/v1/lineage/direct",
                "conversion.timestamp": datetime.now().isoformat()
            },
            "createdAt": datetime.now().isoformat()
        },
        "spec": {
            "nodes": [
                {
                    "inEdges": ["customer/raw-events"],
                    "outEdges": ["customer/analytics", "customer/ml-features"],
                    "distanceFromTheCenter": 0,
                    "nurn": "nurn:nu:data:metapod:dataset:customer/profile",
                    "name": "customer/profile",
                    "type": "dataset",
                    "sourceSystem": "Itaipu",
                    "dataGovernance": {
                        "geo": "BR",
                        "dataDomain": "customer",
                        "dataSubdomain": "profile"
                    },
                    "description": "Customer profile dataset containing aggregated customer information",
                    "version": "v2.1",
                    "schema": [
                        {
                            "nurn": "nurn:nu:data:metapod:dataset:customer/profile:customer_id",
                            "attributeName": "customer_id",
                            "dataType": "string",
                            "description": "Unique customer identifier"
                        },
                        {
                            "nurn": "nurn:nu:data:metapod:dataset:customer/profile:profile_data",
                            "attributeName": "profile_data",
                            "dataType": "json",
                            "description": "Customer profile information"
                        }
                    ]
                },
                {
                    "inEdges": [],
                    "outEdges": ["customer/profile"],
                    "distanceFromTheCenter": 1,
                    "nurn": "nurn:nu:data:metapod:dataset:customer/raw-events",
                    "name": "customer/raw-events",
                    "type": "stream",
                    "sourceSystem": "Kafka",
                    "dataGovernance": {
                        "geo": "BR",
                        "dataDomain": "customer",
                        "dataSubdomain": "events"
                    },
                    "description": "Raw customer event stream"
                }
            ]
        }
    }

if __name__ == "__main__":
    print("🚀 Starting Data Lineage Kinds API Tests")
    print("=" * 50)
    
    run_test_scenarios()
    
    print("\n" + "=" * 50)
    print("✅ Test scenarios completed!")
    print("\nNext steps:")
    print("1. Implement the kinds conversion logic in your Java service")
    print("2. Add the new endpoints to your OpenLineageResource.java")
    print("3. Test with real data from your getDirectLineage endpoint")
    print("4. Validate governance metadata integration") 