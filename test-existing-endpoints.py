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
            response = requests.get(f"{self.base_url}/api/v1", timeout=5)
            print(f"✅ Server is running - Status: {response.status_code}")
            return True
        except requests.exceptions.RequestException as e:
            print(f"❌ Server not accessible: {e}")
            return False
    
    def list_namespaces(self) -> Optional[Dict[str, Any]]:
        """List available namespaces to find real data"""
        try:
            response = requests.get(f"{self.api_v1_url}/namespaces")
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Found {len(data.get('namespaces', []))} namespaces")
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
                print(f"✅ Found {len(data.get('datasets', []))} datasets in namespace '{namespace}'")
                return data
            else:
                print(f"❌ Failed to get datasets for namespace '{namespace}': {response.status_code}")
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
                print(f"✅ Got lineage data with {len(data.get('graph', []))} nodes")
                return data
            else:
                print(f"❌ Failed to get lineage: {response.status_code}")
                if response.text:
                    print(f"   Response: {response.text[:200]}...")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Error getting lineage: {e}")
            return None
    
    def get_regular_lineage(self, node_id: str, depth: int = 20) -> Optional[Dict[str, Any]]:
        """Test the regular lineage endpoint as fallback"""
        try:
            url = f"{self.api_v1_url}/lineage"
            params = {"nodeId": node_id, "depth": depth}
            
            print(f"📡 Testing fallback: GET {url}")
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Got regular lineage data with {len(data.get('graph', []))} nodes")
                return data
            else:
                print(f"❌ Failed to get regular lineage: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Error getting regular lineage: {e}")
            return None

def discover_real_data():
    """Discover real data in the Marquez instance"""
    tester = MarquezApiTester()
    
    print("🔍 Step 1: Checking server health...")
    if not tester.check_server_health():
        print("\n💡 Make sure Marquez is running:")
        print("   cd /Users/jonathan.moraes.gft/Projects/batata/NuMarquez")
        print("   docker-compose up -d")
        print("   # or")
        print("   ./gradlew run")
        return None
    
    print("\n🔍 Step 2: Discovering available namespaces...")
    namespaces_data = tester.list_namespaces()
    
    if not namespaces_data or not namespaces_data.get('namespaces'):
        print("❌ No namespaces found. You may need to ingest some test data first.")
        return None
    
    # Try to find a namespace with data
    for namespace_info in namespaces_data.get('namespaces', [])[:3]:  # Check first 3
        namespace_name = namespace_info.get('name')
        if not namespace_name:
            continue
            
        print(f"\n🔍 Step 3: Checking datasets in namespace '{namespace_name}'...")
        datasets_data = tester.list_datasets_in_namespace(namespace_name)
        
        if datasets_data and datasets_data.get('datasets'):
            # Found a namespace with datasets
            datasets = datasets_data.get('datasets', [])
            print(f"📊 Available datasets in '{namespace_name}':")
            
            for i, dataset in enumerate(datasets[:5]):  # Show first 5
                dataset_name = dataset.get('name', 'unknown')
                created_at = dataset.get('createdAt', 'unknown')
                print(f"   {i+1}. {dataset_name} (created: {created_at})")
            
            # Test lineage for the first dataset
            if datasets:
                test_dataset = datasets[0]
                dataset_name = test_dataset.get('name')
                node_id = f"dataset:{namespace_name}:{dataset_name}"
                
                print(f"\n🧪 Step 4: Testing lineage for real dataset...")
                print(f"   NodeId: {node_id}")
                
                # Try direct lineage first
                lineage_data = tester.get_direct_lineage(node_id, depth=3)
                
                # If direct lineage fails, try regular lineage
                if not lineage_data:
                    print("   Trying regular lineage endpoint...")
                    lineage_data = tester.get_regular_lineage(node_id, depth=3)
                
                if lineage_data:
                    print("\n🎉 SUCCESS! Found real lineage data:")
                    print(f"   Graph nodes: {len(lineage_data.get('graph', []))}")
                    
                    # Show structure of first few nodes
                    for i, node in enumerate(lineage_data.get('graph', [])[:2]):
                        node_id_val = node.get('id', 'unknown')
                        node_type = node.get('type', 'unknown')
                        print(f"   Node {i+1}: {node_id_val} (type: {node_type})")
                    
                    return {
                        'namespace': namespace_name,
                        'dataset': dataset_name,
                        'nodeId': node_id,
                        'lineageData': lineage_data
                    }
                else:
                    print(f"❌ No lineage data found for {node_id}")
    
    return None

def create_kinds_format_example(real_data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert real lineage data to kinds format for demonstration"""
    
    lineage_data = real_data['lineageData']
    namespace = real_data['namespace']
    dataset = real_data['dataset']
    
    # Create a LineageGraph kind based on real data
    kinds_lineage = {
        "apiVersion": "graphs/v1alpha1",
        "kind": "LineageGraph",
        "metadata": {
            "name": f"{namespace}-{dataset}-lineage".replace("_", "-"),
            "graphDepth": 3,
            "centralNode": {
                "dataGovernance": {
                    "geo": "DATA",  # Would extract from real metadata
                    "dataDomain": namespace,
                    "dataSubdomain": "datasets"
                },
                "nurn": f"nurn:nu:data:metapod:dataset:{namespace}/{dataset}",
                "name": f"{namespace}/{dataset}",
                "type": "dataset"
            },
            "labels": {
                "namespace": namespace,
                "data-domain": namespace,
                "source": "marquez-conversion"
            },
            "annotations": {
                "marquez.source-endpoint": "/api/v1/lineage/direct",
                "conversion.timestamp": "2024-01-15T10:30:00Z",
                "original-node-count": str(len(lineage_data.get('graph', [])))
            }
        },
        "spec": {
            "nodes": []
        }
    }
    
    # Convert real nodes to kinds format
    for i, node in enumerate(lineage_data.get('graph', [])):
        node_spec = {
            "nurn": f"nurn:nu:data:metapod:{node.get('id', 'unknown').replace(':', '/')}",
            "name": node.get('id', f'node-{i}'),
            "type": "dataset" if node.get('type') == 'DATASET' else "job",
            "sourceSystem": "Marquez",
            "dataGovernance": {
                "geo": "DATA",
                "dataDomain": namespace,
                "dataSubdomain": "datasets"
            },
            "distanceFromTheCenter": i,  # Simplified
            "inEdges": [edge.get('origin', '') for edge in node.get('inEdges', [])],
            "outEdges": [edge.get('destination', '') for edge in node.get('outEdges', [])],
            "description": f"Converted from Marquez node: {node.get('id', 'unknown')}"
        }
        kinds_lineage["spec"]["nodes"].append(node_spec)
    
    return kinds_lineage

def run_real_data_test():
    """Run tests with real data from your Marquez instance"""
    
    print("🚀 Testing Data Lineage Kinds with Real Marquez Data")
    print("=" * 60)
    
    # Discover real data
    real_data = discover_real_data()
    
    if not real_data:
        print("\n❌ No real data found to test with")
        print("\n💡 To get test data, try:")
        print("1. Run some sample jobs to create lineage")
        print("2. Use the Marquez examples in the examples/ directory")
        print("3. Ingest some OpenLineage events")
        return
    
    print(f"\n✅ Found real data to work with!")
    print(f"   Namespace: {real_data['namespace']}")
    print(f"   Dataset: {real_data['dataset']}")
    print(f"   NodeId: {real_data['nodeId']}")
    
    # Create kinds format example
    print(f"\n🔄 Converting to LineageGraph kind format...")
    kinds_lineage = create_kinds_format_example(real_data)
    
    print(f"✅ Conversion successful!")
    print(f"   LineageGraph name: {kinds_lineage['metadata']['name']}")
    print(f"   Nodes converted: {len(kinds_lineage['spec']['nodes'])}")
    
    # Show the kinds format
    print(f"\n📋 LineageGraph Kind Structure:")
    print(json.dumps(kinds_lineage, indent=2))
    
    # Save to file for inspection
    output_file = "real-lineage-kinds-example.json"
    with open(output_file, 'w') as f:
        json.dump({
            'original_marquez_data': real_data['lineageData'],
            'converted_kinds_format': kinds_lineage
        }, f, indent=2)
    
    print(f"\n💾 Saved comparison to: {output_file}")
    
    print(f"\n🎯 Next Steps:")
    print(f"1. ✅ You now have real lineage data to work with")
    print(f"2. 🔧 Implement the Java endpoints using this real data structure")
    print(f"3. 🧪 Test the conversion logic with the actual node structure")
    print(f"4. 📝 Update the OpenAPI spec based on real data requirements")

if __name__ == "__main__":
    run_real_data_test() 