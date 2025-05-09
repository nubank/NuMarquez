import { API_URL } from '../../globals'
import { JobOrDataset } from '../../types/lineage'

import { generateNodeId } from '../../helpers/nodes'
import { genericFetchWrapper } from './index'

export const getColumnLineage = async (
  nodeType: JobOrDataset,
  namespace: string,
  name: string,
  depth: number,
  withDownstream: boolean
) => {
  const encodedNamespace = encodeURIComponent(namespace)
  const encodedName = encodeURIComponent(name)
  const nodeId = generateNodeId(nodeType, encodedNamespace, encodedName)
  const url = `${API_URL}/column-lineage?nodeId=${nodeId}&depth=${depth}&withDownstream=${withDownstream}`
  return genericFetchWrapper(url, { method: 'GET' }, 'fetchColumnLineage')
}
