// Copyright 2018-2023 contributors to the Marquez project
// SPDX-License-Identifier: Apache-2.0

import { API_URL } from '../../globals'
import { JobOrDataset } from '../../types/lineage'
import { generateNodeId } from '../../helpers/nodes'
import { genericFetchWrapper } from './index'

export const getLineage = async (
  nodeType: JobOrDataset,
  namespace: string,
  name: string,
  depth: number
) => {
  const encodedNamespace = encodeURIComponent(namespace)
  const encodedName = encodeURIComponent(name)
  const nodeId = generateNodeId(nodeType, encodedNamespace, encodedName)
  const url = `${API_URL}/lineage?nodeId=${nodeId}&depth=${depth}`
  return genericFetchWrapper(url, { method: 'GET' }, 'fetchLineage')
}

export const getFilteredLineage = async (
  nodeType: JobOrDataset,
  namespace: string,
  name: string,
  depth: number
) => {
  const encodedNamespace = encodeURIComponent(namespace)
  const encodedName = encodeURIComponent(name)
  const nodeId = generateNodeId(nodeType, encodedNamespace, encodedName)
  const url = `${API_URL}/lineage/direct?nodeId=${nodeId}&depth=${depth}` 
  return genericFetchWrapper(url, { method: 'GET' }, 'fetchFilteredLineage')
}
