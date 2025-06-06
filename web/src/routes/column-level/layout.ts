import { ColumnLevelNodeData, ColumnLevelNodeKinds } from './nodes'
import { ColumnLineageGraph, ColumnLineageNode } from '../../types/api'
import { Edge, Node as ElkNode } from '../../../libs/graph'
import { Nullable } from '../../types/util/Nullable'
import { theme } from '../../helpers/theme'

/**
   Node of format dataset:food_delivery:public.categories:menu_id
   Node of format {type}:{namespace}:{table}:{column}
 */
export const parseColumnLineageNode = (node: string) => {
  const [type, namespace, dataset, column] = node.split(':')
  return { type, namespace, dataset, column }
}

/**
 * Recursively trace the `inEdges` and `outEdges` of the current node to find all connected column nodes
 * @param columnLineageGraph
 * @param currentColumn
 */
export const findConnectedNodes = (
  columnLineageGraph: ColumnLineageNode[],
  currentColumn: Nullable<string>
): ColumnLineageNode[] => {
  if (!currentColumn) return []
  const currentNode = columnLineageGraph.find((node) => node.id === currentColumn)
  if (!currentNode) return []
  const connectedNodes: ColumnLineageNode[] = []
  const visitedNodes: string[] = []
  const queue: ColumnLineageNode[] = [currentNode]

  while (queue.length) {
    const currentNode = queue.shift()
    if (!currentNode) continue
    if (visitedNodes.includes(currentNode.id)) continue
    visitedNodes.push(currentNode.id)
    connectedNodes.push(currentNode)
    // todo fix this broken in api edge.destination should be edge.origin
    queue.push(
      ...currentNode.inEdges
        .map((edge) => columnLineageGraph.find((n) => n.id === edge.destination))
        .filter((item): item is ColumnLineageNode => !!item)
    )
    queue.push(
      ...currentNode.outEdges
        .map((edge) => columnLineageGraph.find((n) => n.id === edge.destination))
        .filter((item): item is ColumnLineageNode => !!item)
    )
  }
  return connectedNodes
}

export const createElkNodes = (
  columnLineageGraph: ColumnLineageGraph,
  currentColumn: Nullable<string>
) => {
  const nodes: ElkNode<ColumnLevelNodeKinds, ColumnLevelNodeData>[] = []
  const edges: Edge[] = []

  const graph = columnLineageGraph.graph.filter((node) => !!node.data)

  // DEBUG: Log para verificar dados do backend
  console.log('=== DEBUG COLUMN LINEAGE ===')
  console.log(
    'Raw graph from backend:',
    graph.map((n) => `${n.data.namespace}:${n.data.dataset}`)
  )

  const connectedNodes = findConnectedNodes(graph, currentColumn)
  console.log(
    'Connected nodes:',
    connectedNodes.map((n) => `${n.data?.namespace}:${n.data?.dataset}`)
  )

  for (const node of graph) {
    const namespace = node.data.namespace
    const dataset = node.data.dataset
    const column = node.data.field

    // DEBUG: Log cada dataset sendo processado
    console.log(`Processing dataset: ${namespace}:${dataset}`)

    edges.push(
      ...node.outEdges.map((edge) => {
        return {
          id: `${edge.origin}:${edge.destination}`,
          sourceNodeId: edge.origin,
          targetNodeId: edge.destination,
          color:
            connectedNodes.includes(node) || connectedNodes.find((n) => n.id === edge.destination)
              ? theme.palette.primary.main
              : theme.palette.grey[400],
        }
      })
    )

    const datasetNode = nodes.find((n) => n.id === `datasetField:${namespace}:${dataset}`)
    if (!datasetNode) {
      // DEBUG: Log quando um novo dataset node é criado
      console.log(`Creating new dataset node: ${namespace}:${dataset}`)
      nodes.push({
        id: `datasetField:${namespace}:${dataset}`,
        kind: 'dataset',
        width: 800,
        data: {
          namespace,
          dataset,
        },
        children: [
          {
            id: node.id,
            height: 24,
            width: 200,
            kind: 'column',
            data: {
              column,
              namespace,
              dataset,
            },
          },
        ],
      })
    } else {
      // DEBUG: Log quando uma coluna é adicionada a um dataset existente
      console.log(`Adding column to existing dataset: ${namespace}:${dataset} - column: ${column}`)
      datasetNode.children?.push({
        id: node.id,
        width: 200,
        height: 24,
        kind: 'column',
        data: {
          column,
          namespace,
          dataset,
        },
      })
    }
  }

  console.log(
    'Final nodes created:',
    nodes.map((n) => (n.data ? `${n.data.namespace}:${n.data.dataset}` : 'no data'))
  )
  console.log('=== END DEBUG ===')

  return { nodes, edges }
}
