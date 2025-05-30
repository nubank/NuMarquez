// Copyright 2018-2023 contributors to the Marquez project
// SPDX-License-Identifier: Apache-2.0

import {
  FETCH_LINEAGE_END,
  FETCH_LINEAGE_START,
  FETCH_LINEAGE_SUCCESS,
  FETCH_FILTERED_LINEAGE_START,
  FETCH_FILTERED_LINEAGE_SUCCESS,
  FETCH_FILTERED_LINEAGE_ERROR,
  FETCH_FILTERED_LINEAGE_END,
  RESET_LINEAGE,
  SET_BOTTOM_BAR_HEIGHT,
  SET_LINEAGE_GRAPH_DEPTH,
  SET_SELECTED_NODE,
  SET_SHOW_FULL_GRAPH,
  SET_TAB_INDEX,
} from '../actionCreators/actionTypes'
import { HEADER_HEIGHT } from '../../helpers/theme'
import { LineageGraph } from '../../types/api'
import { Nullable } from '../../types/util/Nullable'
import { setBottomBarHeight, setLineageGraphDepth, setSelectedNode } from '../actionCreators'

export interface ILineageState {
  isFull: boolean
  lineage: LineageGraph
  filteredLineage: LineageGraph
  selectedNode: Nullable<string>
  bottomBarHeight: number
  depth: number
  isLoading: boolean
  tabIndex: number
  showFullGraph: boolean
}

const initialState: ILineageState = {
  lineage: { graph: [] },
  filteredLineage: { graph: [] },
  selectedNode: null,
  bottomBarHeight: (window.innerHeight - HEADER_HEIGHT) / 3,
  depth: 5,
  isLoading: false,
  isFull: false,
  tabIndex: 0,
  showFullGraph: true,
}

type ILineageActions = ReturnType<typeof setSelectedNode> &
  ReturnType<typeof setBottomBarHeight> &
  ReturnType<typeof setLineageGraphDepth>

const DRAG_BAR_HEIGHT = 8

export default (state = initialState, action: ILineageActions) => {
  switch (action.type) {
    case FETCH_LINEAGE_START:
      return { ...state, isLoading: true }
    case FETCH_LINEAGE_END:
      return { ...state, isLoading: false }
    case FETCH_LINEAGE_SUCCESS:
      return { ...state, lineage: action.payload }

    case FETCH_FILTERED_LINEAGE_START:
      return { ...state, isLoading: true }
    case FETCH_FILTERED_LINEAGE_SUCCESS:
      return { ...state, isLoading: false, filteredLineage: action.payload }
    case FETCH_FILTERED_LINEAGE_ERROR:
      return { ...state, isLoading: false }
    case FETCH_FILTERED_LINEAGE_END:
      return { ...state, isLoading: false }

    case SET_SELECTED_NODE:
      return { ...state, selectedNode: action.payload, tabIndex: state.tabIndex === 1 ? 1 : 0 }
    case SET_BOTTOM_BAR_HEIGHT:
      return {
        ...state,
        bottomBarHeight: Math.min(
          window.innerHeight - HEADER_HEIGHT - DRAG_BAR_HEIGHT,
          Math.max(2, action.payload)
        ),
      }
    case SET_TAB_INDEX:
      return {
        ...state,
        tabIndex: action.payload,
      }
    case SET_LINEAGE_GRAPH_DEPTH:
      return {
        ...state,
        depth: action.payload,
      }
    case SET_SHOW_FULL_GRAPH:
      return {
        ...state,
        showFullGraph: action.payload,
      }
    case RESET_LINEAGE: {
      return {
        ...state,
        lineage: { graph: [] },
        filteredLineage: { graph: [] },
      }
    }
    default:
      return state
  }
}
