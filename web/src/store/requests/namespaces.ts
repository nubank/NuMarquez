// Copyright 2018-2023 contributors to the Marquez project
// SPDX-License-Identifier: Apache-2.0

import { API_URL } from '../../globals'
import { genericFetchWrapper } from './index'

export const getNamespaces = async () => {
  const url = `${API_URL}/namespaces?limit=500`
  return genericFetchWrapper(url, { method: 'GET' }, 'fetchNamespaces')
}
