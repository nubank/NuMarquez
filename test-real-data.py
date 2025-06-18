#!/usr/bin/env python3
"""
Test script to verify existing Marquez endpoints and get real lineage data
"""

import requests
import json
from typing import Dict, Any, Optional

class MarquezApiTester:
    """Test client for existing Marquez API endpoints"""
    
    def __init__(self, base_url: str = "http://localhost:5000"):
        self.base_url = base_url
        self.api_v1_url = f"{base_url}/api/v1"
    
    def check_server_health(self) -> bool:
        """Check if Marquez server is running"""
        try:
            response = requests.get(f"{self.api_v1_url}/namespaces", timeout=5)
            print(f"✅ Server is running - Status: {response.status_code}")
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f"❌ Server not accessible: {e}")
            return False
    
    def list_namespaces(self) -> Optional[Dict[str, Any]]:
        """List available namespaces to find real data"""
        try:
            response = requests.get(f"{self.api_v1_url}/namespaces")
            if response.status_code == 200:
                data = response.json()
                namespaces = data.get('namespaces', [])
                print(f"✅ Found {len(namespaces)} namespaces")
                for ns in namespaces[:3]:
                    print(f"   - {ns.get('name', 'unknown')}")
                return data
            else:
                print(f"❌ Failed to get namespaces: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"❌ Error getting namespaces: {e}")
            return None
    
    def list_datasets_in_namespace(self, namespace: str) -> Optional[Dict[str, Any]]:
        """List datasets in a specific namespace"""
        try:
            response = requests.get(f"{self.api_v1_url}/namespaces/{namespace}/datasets")
            if response.status_code == 200:
                data = response.json()
                datasets = data.get('datasets', [])
                print(f"✅ Found {len(datasets)} datasets in namespace '{namespace}'")
                return data
            else:
                print(f"❌ Failed to get datasets: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"❌ Error getting datasets: {e}")
            return None
    
    def get_direct_lineage(self, node_id: str, depth: int = 20) -> Optional[Dict[str, Any]]:
        """Test the existing getDirectLineage endpoint"""
        try:
            url = f"{self.api_v1_url}/lineage/direct"
            params = {"nodeId": node_id, "depth": depth}
            
            print(f"📡 Testing: GET {url}")
            print(f"   Parameters: {params}")
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Got direct lineage with {len(data.get('graph', []))} nodes")
                return data
            else:
                print(f"❌ Direct lineage failed: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Error getting direct lineage: {e}")
            return None

def test_with_real_data():
    """Test with real data from your Marquez instance"""
    
    print("🚀 Testing with Real Marquez Data")
    print("=" * 50)
    
    tester = MarquezApiTester()
    
    print("Step 1: Checking server...")
    if not tester.check_server_health():
        print("\n💡 To start Marquez:")
        print("   cd /Users/jonathan.moraes.gft/Projects/batata/NuMarquez")
        print("   docker-compose up -d")
        return
    
    print("\nStep 2: Finding real data...")
    namespaces_data = tester.list_namespaces()
    
    if not namespaces_data or not namespaces_data.get('namespaces'):
        print("❌ No namespaces found")
        return
    
    # Look for real data
    for namespace_info in namespaces_data.get('namespaces', [])[:3]:
        namespace_name = namespace_info.get('name')
        if not namespace_name:
            continue
            
        print(f"\nStep 3: Checking namespace '{namespace_name}'...")
        datasets_data = tester.list_datasets_in_namespace(namespace_name)
        
        if datasets_data and datasets_data.get('datasets'):
            datasets = datasets_data.get('datasets', [])
            print(f"📊 Datasets found:")
            
            for i, dataset in enumerate(datasets[:3]):
                dataset_name = dataset.get('name', 'unknown')
                print(f"   {i+1}. {dataset_name}")
            
            # Test with first dataset
            if datasets:
                test_dataset = datasets[0]
                dataset_name = test_dataset.get('name')
                node_id = f"dataset:{namespace_name}:{dataset_name}"
                
                print(f"\nStep 4: Testing lineage for '{node_id}'...")
                lineage_data = tester.get_direct_lineage(node_id, depth=3)
                
                if lineage_data:
                    print("\n🎉 SUCCESS! Here's what we found:")
                    print(f"   Real NodeId: {node_id}")
                    print(f"   Graph nodes: {len(lineage_data.get('graph', []))}")
                    
                    # Show node structure
                    for i, node in enumerate(lineage_data.get('graph', [])[:2]):
                        node_id_val = node.get('id', 'unknown')
                        node_type = node.get('type', 'unknown')
                        print(f"   Node {i+1}: {node_id_val} (type: {node_type})")
                    
                    # Create kinds conversion example
                    print(f"\n🔄 Here's how this would look as a LineageGraph kind:")
                    kinds_example = create_kinds_example(node_id, lineage_data, namespace_name, dataset_name)
                    print(json.dumps(kinds_example, indent=2)[:500] + "...")
                    
                    return True
                else:
                    print(f"❌ No lineage found for {node_id}")
    
    print("❌ No usable data found")
    return False

def create_kinds_example(node_id: str, lineage_data: Dict, namespace: str, dataset: str) -> Dict:
    """Create a sample LineageGraph kind from real data"""
    return {
        "apiVersion": "graphs/v1alpha1",
        "kind": "LineageGraph",
        "metadata": {
            "name": f"{namespace}-{dataset}-lineage".replace("_", "-"),
            "graphDepth": 3,
            "centralNode": {
                "dataGovernance": {
                    "geo": "DATA",
                    "dataDomain": namespace,
                    "dataSubdomain": "datasets"
                },
                "nurn": f"nurn:nu:data:metapod:dataset:{namespace}/{dataset}",
                "name": f"{namespace}/{dataset}",
                "type": "dataset"
            },
            "labels": {
                "namespace": namespace,
                "data-domain": namespace
            },
            "annotations": {
                "marquez.original-node-id": node_id,
                "marquez.source-endpoint": "/api/v1/lineage/direct"
            }
        },
        "spec": {
            "nodes": [
                {
                    "nurn": f"nurn:nu:data:metapod:{node.get('id', '').replace(':', '/')}",
                    "name": node.get('id', ''),
                    "type": "dataset" if node.get('type') == 'DATASET' else "job",
                    "dataGovernance": {
                        "geo": "DATA",
                        "dataDomain": namespace,
                        "dataSubdomain": "datasets"
                    },
                    "distanceFromTheCenter": i,
                    "inEdges": [],
                    "outEdges": []
                }
                for i, node in enumerate(lineage_data.get('graph', [])[:3])
            ]
        }
    }

if __name__ == "__main__":
    success = test_with_real_data()
    
    if success:
        print("\n✅ Ready to implement the kinds endpoints!")
        print("\nNext steps:")
        print("1. Add the LineageKindsResource to your project")
        print("2. Test the conversion with this real data")
        print("3. Implement proper governance metadata extraction")
    else:
        print("\n💡 To get test data in Marquez:")
        print("1. Check the examples/ directory for sample jobs")
        print("2. Run: docker-compose up -d to start with sample data")
        print("3. Or ingest some OpenLineage events manually") 