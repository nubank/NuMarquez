import React, { useEffect, useState, useCallback } from 'react'
import {
  Box,
  CircularProgress,
  Divider,
  FormControlLabel,
  IconButton,
  Switch,
  TextField,
} from '@mui/material'
import { ArrowBackIosRounded, Refresh } from '@mui/icons-material'
import { HEADER_HEIGHT, theme } from '../../helpers/theme'
import { fetchLineage, fetchFilteredLineage } from '../../store/actionCreators'
import { trackEvent } from '../../components/ga4'
import { truncateText } from '../../helpers/text'
import { useNavigate, useParams, useSearchParams } from 'react-router-dom'
import MQTooltip from '../../components/core/tooltip/MQTooltip'
import MqText from '../../components/core/text/MqText'

interface ActionBarProps {
  nodeType: 'DATASET' | 'JOB'
  fetchLineage: typeof fetchLineage
  fetchFilteredLineage: typeof fetchFilteredLineage
  depth: number
  setDepth: (depth: number) => void
  isCompact: boolean
  setIsCompact: (isCompact: boolean) => void
  isFull: boolean
  setIsFull: (isFull: boolean) => void
  isLoading: boolean
}

export const ActionBar = ({
  nodeType,
  fetchLineage,
  fetchFilteredLineage,
  depth,
  setDepth,
  isCompact,
  setIsCompact,
  isFull,
  setIsFull,
  isLoading,
}: ActionBarProps) => {
  const { namespace, name } = useParams()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()

  const [loading, setLoading] = useState(false)
  // const [openSnackbar, setOpenSnackbar] = useState(false)
  // const [snackbarMessage, setSnackbarMessage] = useState('')
  // const [maxDepthFull, setMaxDepthFull] = useState<number | null>(null)
  // const [maxDepthNonFull, setMaxDepthNonFull] = useState<number | null>(null)
  // const [prevObjectsCount, setPrevObjectsCount] = useState<number | null>(null)
  // const [prevDepth, setPrevDepth] = useState<number | null>(null)
  // const snackbarTimeoutRef = useRef<NodeJS.Timeout | null>(null)

  useEffect(() => {
    const resetLimitState = () => {
      // setMaxDepthFull(null)
      // setMaxDepthNonFull(null)
      // setPrevObjectsCount(null)
      // setPrevDepth(null)
    }

    const prevName = localStorage.getItem('prevName')
    if (prevName && prevName !== name) {
      localStorage.removeItem('maxDepthFull')
      localStorage.removeItem('maxDepthNonFull')
    }

    localStorage.setItem('prevName', name || '')

    resetLimitState()
  }, [name])

  useEffect(() => {
    const storedMaxDepthFull = localStorage.getItem('maxDepthFull')
    const storedMaxDepthNonFull = localStorage.getItem('maxDepthNonFull')

    // if (storedMaxDepthFull) {
    //   const parsedDepthFull = parseInt(storedMaxDepthFull)
    //   setMaxDepthFull(parsedDepthFull)
    // }

    // if (storedMaxDepthNonFull) {
    //   const parsedDepthNonFull = parseInt(storedMaxDepthNonFull)
    //   setMaxDepthNonFull(parsedDepthNonFull)
    // }

    // const currentMaxDepth = isFull ? maxDepthFull : maxDepthNonFull
    // if (currentMaxDepth !== null && depth > currentMaxDepth) {
    //   setDepth(currentMaxDepth)
    //   searchParams.set('depth', currentMaxDepth.toString())
    //   setSearchParams(searchParams)
    // }

    if (!searchParams.has('isCompact')) {
      searchParams.set('isCompact', 'true')
      setSearchParams(searchParams)
      setIsCompact(true)
    }
  }, [isFull])

  const handleBackClick = useCallback(() => {
    navigate(nodeType === 'JOB' ? '/jobs' : '/')
    trackEvent('ActionBar', 'Click Back Button', nodeType)
  }, [navigate, nodeType])

  const handleRefreshClick = useCallback(() => {
    if (namespace && name) {
      fetchLineage(nodeType, namespace, name, depth, true)
      trackEvent('ActionBar', 'Refresh Lineage', nodeType)
    }
  }, [namespace, name, nodeType, depth, fetchLineage])

  const handleDepthChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const requestedDepth = parseInt(e.target.value, 10) || 0

    if (!namespace || !name) {
      // setSnackbarMessage('Namespace or name is missing')
      // setOpenSnackbar(true)
      // if (snackbarTimeoutRef.current) clearTimeout(snackbarTimeoutRef.current)
      // snackbarTimeoutRef.current = setTimeout(() => {
      //   setOpenSnackbar(false)
      // }, 2000)
      return
    }

    if (isFull === false) {
      fetchFilteredLineage(nodeType, namespace, name, requestedDepth)

    } else {
      fetchLineage(nodeType, namespace, name, requestedDepth, true)

    }

    setDepth(requestedDepth)
    searchParams.set('depth', requestedDepth.toString())
    setSearchParams(searchParams)
  }

  // const handleCloseSnackbar = useCallback(() => {
  //   setOpenSnackbar(false)
  //   if (snackbarTimeoutRef.current) {
  //     clearTimeout(snackbarTimeoutRef.current)
  //     snackbarTimeoutRef.current = null
  //   }
  // }, [])

  const handleAllDependenciesToggle = useCallback(
    (checked: boolean) => {
      setIsFull(checked)
      searchParams.set('isFull', checked.toString())
      setSearchParams(searchParams)
    },
    [setIsFull, searchParams, setSearchParams]
  )

  const handleHideColumnNamesToggle = useCallback((checked: boolean) => {
    setIsCompact(checked)
    searchParams.set('isCompact', checked.toString())
    setSearchParams(searchParams)
    trackEvent('ActionBar', 'Toggle Hide Column Names', checked.toString())
  }, [setIsCompact, searchParams, setSearchParams])

  return (
    <Box
      sx={{
        borderBottomWidth: 2,
        borderTopWidth: 0,
        borderLeftWidth: 0,
        borderRightWidth: 0,
        borderStyle: 'dashed',
      }}
      display={'flex'}
      height={HEADER_HEIGHT - 1}
      justifyContent={'space-between'}
      alignItems={'center'}
      px={2}
      borderColor={theme.palette.secondary.main}
    >
      <Box display={'flex'} alignItems={'center'}>
        <MQTooltip title={`Back to ${nodeType === 'JOB' ? 'jobs' : 'datasets'}`}>
          <IconButton size={'small'} sx={{ mr: 2 }} onClick={handleBackClick}>
            <ArrowBackIosRounded fontSize={'small'} />
          </IconButton>
        </MQTooltip>
        <MqText heading>{nodeType === 'JOB' ? 'Jobs' : 'Datasets'}</MqText>
        <Divider orientation='vertical' flexItem sx={{ mx: 2 }} />
        <Box>
          <MqText subdued>Mode</MqText>
          <MqText font={'mono'}>Table Level</MqText>
        </Box>
        <Divider orientation='vertical' flexItem sx={{ mx: 2 }} />
        <Box>
          <MqText subdued>Namespace</MqText>
          <MqText font={'mono'}>
            {namespace ? truncateText(namespace, 50) : 'Unknown namespace name'}
          </MqText>
        </Box>
        <Divider orientation='vertical' flexItem sx={{ mx: 2 }} />
        <Box>
          <MqText subdued>Name</MqText>
          <MqText font={'mono'}>{name ? truncateText(name, 190) : 'Unknown dataset name'}</MqText>
        </Box>
      </Box>
      <Box display={'flex'} alignItems={'center'}>
        <MQTooltip title={'Refresh'}>
          <IconButton sx={{ mr: 2 }} color={'primary'} size={'small'} onClick={handleRefreshClick}>
            <Refresh fontSize={'small'} />
          </IconButton>
        </MQTooltip>
        <MQTooltip title={'Select the number of levels to display in the lineage'}>
          {loading || isLoading === true ? (
            <CircularProgress size={40} sx={{ width: '80px', mr: 2 }} />
          ) : (
            <TextField
              id='column-level-depth'
              type='number'
              inputProps={{ min: 0 }}
              label='Depth'
              variant='outlined'
              size='small'
              sx={{ width: '80px' }}
              value={depth}
              onChange={handleDepthChange}
              disabled={loading}
            />
          )}
        </MQTooltip>

        <Box display={'flex'} flexDirection={'column'} sx={{ marginLeft: 2 }}>
          <MQTooltip title={'Show all dependencies, including indirect ones'}>
            <FormControlLabel
              control={
                <Switch
                  size={'small'}
                  checked={isFull}
                  onChange={(_, checked) => handleAllDependenciesToggle(checked)}
                />
              }
              label={<MqText font={'mono'}>All dependencies</MqText>}
            />
          </MQTooltip>
          <MQTooltip title={'Hide column names for each dataset'}>
            <FormControlLabel
              control={
                <Switch
                  size={'small'}
                  checked={isCompact}
                  defaultChecked={searchParams.get('isCompact') === 'true'}
                  onChange={(_, checked) => handleHideColumnNamesToggle(checked)}
                />
              }
              label={<MqText font={'mono'}>Hide column names</MqText>}
            />
          </MQTooltip>
        </Box>
      </Box>
      {/* <Snackbar open={openSnackbar}  onClose={handleCloseSnackbar}>
        <Alert
          onClose={handleCloseSnackbar}
          severity='info'
          variant='filled'
          sx={{ width: '100%', backgroundColor: '#FFFFFF', color: '#191E26' }}
        >
          {snackbarMessage}
        </Alert>
      </Snackbar> */}
    </Box>
  )
}