import * as Redux from 'redux'
import { ActionBar } from './ActionBar'
import { ColumnLevelNodeData, ColumnLevelNodeKinds, columnLevelNodeRenderer } from './nodes'
import { ColumnLineageGraph } from '../../types/api'
import { Drawer } from '@mui/material'
import { Graph, ZoomPanControls } from '../../../libs/graph'
import { HEADER_HEIGHT, theme } from '../../helpers/theme'
import { IState } from '../../store/reducers'
import { ZoomControls } from './ZoomControls'
import { bindActionCreators } from 'redux'
import { connect } from 'react-redux'
import { createElkNodes } from './layout'
import { fetchColumnLineage } from '../../store/actionCreators'
import { useCallbackRef } from '../../helpers/hooks'
import { useParams, useSearchParams } from 'react-router-dom'
import Box from '@mui/material/Box'
import ColumnLevelDrawer from './ColumnLevelDrawer'
import ParentSize from '@visx/responsive/lib/components/ParentSize'
import React, { useEffect, useRef, useState } from 'react'
import { trackEvent } from '../../components/ga4'

interface StateProps {
  columnLineage: ColumnLineageGraph
}

interface DispatchProps {
  fetchColumnLineage: typeof fetchColumnLineage
}

type ColumnLevelProps = StateProps & DispatchProps

const zoomInFactor = 1.5
const zoomOutFactor = 1 / zoomInFactor

const ColumnLevel: React.FC<ColumnLevelProps> = ({
  fetchColumnLineage: fetchColumnLineage,
  columnLineage: columnLineage,
}: ColumnLevelProps) => {
  const { namespace, name } = useParams()
  const [searchParams, setSearchParams] = useSearchParams()

  const [depth, setDepth] = useState(Number(searchParams.get('depth')) || 2)
  const [withDownstream, setWithDownstream] = useState(
    searchParams.get('withDownstream') === 'true'
  )

  const graphControls = useRef<ZoomPanControls>()

  useEffect(() => {
    trackEvent('ColumnLevel', 'View Column-Level Lineage')
  }, [])

  useEffect(() => {
    if (name && namespace) {
      fetchColumnLineage('DATASET', namespace, name, depth, withDownstream)
    }
  }, [name, namespace, depth, withDownstream])

  if (!columnLineage) {
    return <div />
  }

  const handleScaleZoom = (inOrOut: 'in' | 'out') => {
    graphControls.current?.scaleZoom(inOrOut === 'in' ? zoomInFactor : zoomOutFactor)
  }

  const handleResetZoom = () => {
    graphControls.current?.fitContent()
  }

  const setGraphControls = useCallbackRef((zoomControls) => {
    graphControls.current = zoomControls
  })

  const { nodes, edges } = createElkNodes(columnLineage, searchParams.get('column'))

  useEffect(() => {
    setTimeout(() => {
      graphControls.current?.fitContent()
    }, 300)
  }, [nodes.length])

  return (
    <>
      <ActionBar
        fetchColumnLineage={fetchColumnLineage}
        depth={depth}
        setDepth={setDepth}
        withDownstream={withDownstream}
        setWithDownstream={setWithDownstream}
      />
      <Box height={`calc(100vh - ${HEADER_HEIGHT}px - 64px)`}>
        <Drawer
          anchor={'right'}
          open={!!searchParams.get('dataset')}
          onClose={() => {
            setSearchParams({})
            trackEvent('ColumnLevel', 'Close Drawer')
          }}
          PaperProps={{
            sx: {
              backgroundColor: theme.palette.background.default,
              backgroundImage: 'none',
              mt: `${HEADER_HEIGHT}px`,
              height: `calc(100vh - ${HEADER_HEIGHT}px)`,
            },
          }}
        >
          <Box>
            <ColumnLevelDrawer />
          </Box>
        </Drawer>
        <ZoomControls handleScaleZoom={handleScaleZoom} handleResetZoom={handleResetZoom} />
        <ParentSize>
          {(parent) => (
            <Graph<ColumnLevelNodeKinds, ColumnLevelNodeData>
              id='column-level-graph'
              backgroundColor={theme.palette.background.default}
              height={parent.height}
              width={parent.width}
              nodes={nodes}
              edges={edges}
              direction='right'
              nodeRenderers={columnLevelNodeRenderer}
              setZoomPanControls={setGraphControls}
            />
          )}
        </ParentSize>
      </Box>
    </>
  )
}

const mapStateToProps = (state: IState) => ({
  columnLineage: state.columnLineage.columnLineage,
})

const mapDispatchToProps = (dispatch: Redux.Dispatch) =>
  bindActionCreators(
    {
      fetchColumnLineage: fetchColumnLineage,
    },
    dispatch
  )

export default connect(mapStateToProps, mapDispatchToProps)(ColumnLevel)
