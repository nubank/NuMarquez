// Copyright 2018-2023 contributors to the Marquez project
// SPDX-License-Identifier: Apache-2.0

import React from 'react'
import SVG from 'react-inlinesvg'

import { Link, useLocation } from 'react-router-dom'
import Box from '@mui/material/Box'

import { DRAWER_WIDTH, HEADER_HEIGHT } from '../../helpers/theme'
import { Divider, Drawer, createTheme } from '@mui/material'
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faCogs, faDatabase } from '@fortawesome/free-solid-svg-icons'
import MqIconButton from '../core/icon-button/MqIconButton'

// for i18n
import '../../i18n/config'
import { FormControl, MenuItem, Select } from '@mui/material'
import { MqInputNoIcon } from '../core/input-base/MqInputBase'
import { useTheme } from '@emotion/react'

import { Dashboard } from '@mui/icons-material'
import { PrivateRoute } from '../PrivateRoute'
import HelpCenterIcon from '@mui/icons-material/HelpCenter'
import SupportAgentIcon from '@mui/icons-material/SupportAgent'
import iconSearchArrow from '../../img/iconSearchArrow.svg'
import nu_logo from './logoNu.svg'

interface SidenavProps {}

const Sidenav: React.FC<SidenavProps> = () => {
  const i18next = require('i18next')
  const changeLanguage = (lng: string) => {
    i18next.changeLanguage(lng)
  }
  const theme = createTheme(useTheme())

  const location = useLocation()

  return (
    <PrivateRoute>
      <Drawer
        sx={{
          marginTop: `${HEADER_HEIGHT}px`,
          width: `${DRAWER_WIDTH}px`,
          flexShrink: 0,
          whiteSpace: 'nowrap',
          '& > :first-of-type': {
            borderRight: 'none',
          },
        }}
        PaperProps={{
          sx: {
            backgroundColor: theme.palette.background.default,
            backgroundImage: 'none',
          },
        }}
        variant='permanent'
      >
        <Box
          position={'relative'}
          width={DRAWER_WIDTH}
          display={'flex'}
          flexDirection={'column'}
          justifyContent={'space-between'}
          height={'100%'}
          pb={2}
          sx={{
            borderRight: `2px dashed ${theme.palette.secondary.main}`,
          }}
        >
          <Box display={'flex'} flexDirection={'column'} alignItems={'center'}>
            <Box
              display={'flex'}
              alignItems={'center'}
              justifyContent={'center'}
              height={HEADER_HEIGHT}
            >
              <Link to='/'>
                <img
                  src={nu_logo}
                  height={60}
                  alt='Nu Logo'
                  style={{ filter: 'invert(1)', marginTop: '10px' }}
                />
              </Link>
            </Box>
            <Divider sx={{ my: 1 }} />
            <MqIconButton
              to={'/'}
              id={'datasetsDrawerButton'}
              title={i18next.t('sidenav.datasets')}
              active={location.pathname === '/'}
            >
              <FontAwesomeIcon icon={faDatabase} fontSize={20} />
            </MqIconButton>
            <MqIconButton
              to={'/jobs'}
              id={'jobsDrawerButton'}
              title={i18next.t('sidenav.jobs')}
              active={location.pathname === '/jobs'}
            >
              <FontAwesomeIcon icon={faCogs} fontSize={20} />
            </MqIconButton>

            <MqIconButton
              id={'eventsButton'}
              to={'/events'}
              title={i18next.t('sidenav.events')}
              active={location.pathname === '/events'}
            >
              <SVG src={iconSearchArrow} width={'20px'} />
            </MqIconButton>
            <MqIconButton
              to={'/dashboard'}
              id={'homeDrawerButton'}
              title={i18next.t('sidenav.dataOps')}
              active={location.pathname === '/dashboard'}
            >
              <Dashboard />
            </MqIconButton>
          </Box>

          <Box
            sx={{
              marginTop: 'auto',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
            }}
          >
            <MqIconButton
              id={'documentationButton'}
              to={
                'https://nubank.atlassian.net/wiki/spaces/data/pages/264019349541/Nu+Data+Lineage'
              }
              target='_blank'
              title={i18next.t('sidenav.documentation')}
              active={location.pathname === '/documentation'}
            >
              <HelpCenterIcon sx={{ fontSize: 20 }} />
            </MqIconButton>
            <MqIconButton
              id={'supportButton'}
              to={
                'https://nubank.atlassian.net/servicedesk/customer/portal/49/group/1943/create/17910'
              }
              target='_blank'
              title={i18next.t('sidenav.support')}
              active={location.pathname === '/support'}
            >
              <SupportAgentIcon sx={{ fontSize: 20 }} />
            </MqIconButton>
          </Box>
          <FormControl
            variant='outlined'
            sx={{
              maxWidth: '100px',
            }}
          >
            <Box px={1}>
              <Select
                fullWidth
                value={i18next.resolvedLanguage}
                onChange={(event) => {
                  changeLanguage(event.target.value as string)
                  window.location.reload()
                }}
                input={<MqInputNoIcon />}
              >
                <MenuItem key={'en'} value={'en'}>
                  {'en'}
                </MenuItem>
                <MenuItem key={'es'} value={'es'}>
                  {'es'}
                </MenuItem>
                <MenuItem key={'fr'} value={'fr'}>
                  {'fr'}
                </MenuItem>
                <MenuItem key={'pl'} value={'pl'}>
                  {'pl'}
                </MenuItem>
              </Select>
            </Box>
          </FormControl>
        </Box>
      </Drawer>
    </PrivateRoute>
  )
}

export default Sidenav
