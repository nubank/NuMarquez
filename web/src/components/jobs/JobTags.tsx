// Copyright 2018-2024 contributors to the Marquez project
// SPDX-License-Identifier: Apache-2.0
import * as Redux from 'redux'
import {
  Autocomplete,
  AutocompleteChangeDetails,
  AutocompleteChangeReason,
  Checkbox,
  TextField,
} from '@mui/material'
import { Box, createTheme } from '@mui/material'
import { IState } from '../../store/reducers'
import { Tag } from '../../types/api'
import { addJobTag, addTags, deleteJobTag } from '../../store/actionCreators'
import { bindActionCreators } from 'redux'
import { connect, useSelector } from 'react-redux'
import { useTheme } from '@emotion/react'
import Button from '@mui/material/Button'
import CheckBoxIcon from '@mui/icons-material/CheckBox'
import CheckBoxOutlineBlankIcon from '@mui/icons-material/CheckBoxOutlineBlank'
import Chip from '@mui/material/Chip'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import LocalOfferIcon from '@mui/icons-material/LocalOffer'
import MQText from '../core/text/MqText'
import MQTooltip from '../core/tooltip/MQTooltip'
import React, { ChangeEvent, useState } from 'react'
import Snackbar from '@mui/material/Snackbar'
import { trackEvent } from '../ga4'

interface JobTagsProps {
  namespace: string
  jobName: string
  jobTags: string[]
}

interface DispatchProps {
  deleteJobTag: typeof deleteJobTag
  addJobTag: typeof addJobTag
  addTags: typeof addTags
}

type IProps = JobTagsProps & DispatchProps

const JobTags: React.FC<IProps> = (props) => {
  const { namespace, jobName, jobTags, deleteJobTag, addJobTag, addTags } = props

  const [listTag, setListTag] = useState('')
  const [openTagDesc, setOpenTagDesc] = useState(false)
  const [tagDescription, setTagDescription] = useState('')
  const [selectedTags, setSelectedTags] = useState<string[]>(jobTags)

  const handleButtonClick = () => {
    setOpenTagDesc(true)
  }

  const [snackbarOpen, setSnackbarOpen] = useState(false)
  const theme = createTheme(useTheme())

  const handleTagDescClose = () => {
    setOpenTagDesc(false)
    setListTag('')
    setTagDescription('')
  }

  const handleTagDescChange = (_event: ChangeEvent<HTMLInputElement>, value: string) => {
    const selectedTagData = tagData.find((tag) => tag.name === value)
    setListTag(value)
    setTagDescription(selectedTagData ? selectedTagData.description : '')
    trackEvent('JobTags', 'Edit Tag', value)
  }

  const handleDescriptionChange = (event: ChangeEvent<HTMLInputElement>) => {
    setTagDescription(event.target.value)
  }

  const tagData = useSelector((state: IState) =>
    state.tags.tags.sort((a, b) => a.name.localeCompare(b.name))
  )

  const handleTagChange = (
    _event: React.SyntheticEvent,
    _value: string[],
    _reason: AutocompleteChangeReason,
    details?: AutocompleteChangeDetails<string> | undefined
  ) => {
    if (details && _reason === 'removeOption') {
      const newTag = details.option
      const newSelectedTags = selectedTags.filter((tag) => newTag !== tag)
      setSelectedTags(newSelectedTags)
      deleteJobTag(namespace, jobName, newTag)
    } else if (details && !selectedTags.includes(details.option)) {
      const newTag = details.option
      const newSelectedTags = [...selectedTags, newTag]
      setSelectedTags(newSelectedTags)
      addJobTag(namespace, jobName, newTag)
    }
  }

  const handleDelete = (deletedTag: string) => {
    const newSelectedTags = selectedTags.filter((tag) => deletedTag !== tag)

    setSelectedTags(newSelectedTags)

    deleteJobTag(namespace, jobName, deletedTag)
    trackEvent('JobTags', 'Delete Tag', deletedTag)
  }

  const addTag = () => {
    addTags(listTag, tagDescription)
    setSnackbarOpen(true)
    setOpenTagDesc(false)
    setListTag('')
    setTagDescription('')
    trackEvent('JobTags', 'Add Tag', listTag)
  }

  const formatTags = (tags: string[], tag_desc: Tag[]) => {
    return tags.map((tag, index) => {
      const tagDescription = tag_desc.find((tagItem) => tagItem.name === tag)
      const tooltipTitle = tagDescription?.description || 'No Tag Description'
      return (
        <MQTooltip title={tooltipTitle} key={tag}>
          <Chip
            color={'primary'}
            variant='outlined'
            label={tag}
            size='small'
            onDelete={() => handleDelete(tag)}
            style={{
              display: 'row',
              marginLeft: index === 0 ? theme.spacing(0) : theme.spacing(1),
            }}
          />
        </MQTooltip>
      )
    })
  }

  return (
    <>
      <Snackbar
        open={snackbarOpen}
        autoHideDuration={1000}
        style={{ zIndex: theme.zIndex.snackbar }}
        onClose={() => setSnackbarOpen(false)}
        message={'Tag updated.'}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
      />
      <Box display={'flex'} alignItems='center' justifyContent='center' width={'100%'}>
        <MQTooltip title='Edit a Tag' key='edit-tag'>
          <Button
            variant='outlined'
            onClick={handleButtonClick}
            color='primary'
            sx={{ marginRight: '8px' }}
            style={{ paddingTop: '6.75px', paddingBottom: '6.75px' }}
            startIcon={<LocalOfferIcon />}
          >
            Edit Tag
          </Button>
        </MQTooltip>
        <Autocomplete
          multiple
          disableCloseOnSelect
          id='dataset-tags'
          sx={{ flex: 1, width: 'auto' }}
          limitTags={6}
          autoHighlight
          disableClearable
          disablePortal
          options={tagData.map((option) => option.name)}
          value={selectedTags}
          onChange={handleTagChange}
          renderTags={(value: string[]) => formatTags(value, tagData)}
          renderOption={(props, option, { selected }) => (
            <li {...props}>
              <Checkbox
                icon={<CheckBoxOutlineBlankIcon fontSize='small' />}
                checkedIcon={<CheckBoxIcon fontSize='small' />}
                style={{ marginRight: 4 }}
                checked={selected}
              />
              <div>
                <MQText bold>{option}</MQText>
                <MQText subdued overflowHidden>
                  {tagData.find((tagItem) => tagItem.name === option)?.description || ''}
                </MQText>
              </div>
            </li>
          )}
          renderInput={(params) => (
            <TextField
              variant={'outlined'}
              {...params}
              placeholder={selectedTags.length > 0 ? '' : 'Search Tags'}
              InputProps={{
                ...params.InputProps,
              }}
              InputLabelProps={{
                shrink: true,
              }}
              size='small'
            />
          )}
        />
      </Box>
      <Dialog
        PaperProps={{
          sx: { backgroundColor: theme.palette.background.default, backgroundImage: 'none' },
        }}
        open={openTagDesc}
        fullWidth
        maxWidth='sm'
        onKeyDown={(event) => {
          if (event.key === 'Escape') {
            handleTagDescClose()
          }
        }}
      >
        <DialogContent>
          <MQText label sx={{ fontSize: '1.25rem' }} bottomMargin>
            Select a Tag to change
          </MQText>
          <MQText label sx={{ fontSize: '0.85rem' }}>
            Tag
          </MQText>
          <Autocomplete
            options={tagData.map((option) => option.name)}
            autoSelect
            freeSolo
            fullWidth
            autoFocus
            forcePopupIcon
            onChange={handleTagDescChange}
            renderInput={(params) => (
              <TextField
                {...params}
                placeholder={'Search for a Tag...or enter a new one.'}
                autoFocus
                margin='dense'
                id='tag'
                variant='outlined'
                InputLabelProps={{
                  ...params.InputProps,
                  shrink: false,
                }}
              />
            )}
          />
          <MQText label sx={{ fontSize: '0.85rem' }} bottomMargin>
            Description
          </MQText>
          <TextField
            multiline
            id='tag-description'
            name='tag-description'
            fullWidth
            variant='outlined'
            placeholder={''}
            onChange={handleDescriptionChange}
            rows={6}
            value={tagDescription}
            InputProps={{
              style: { padding: '12px 16px' },
            }}
            InputLabelProps={{
              shrink: false,
            }}
          />
        </DialogContent>
        <DialogActions>
          <Button color='primary' onClick={addTag} disabled={listTag === ''}>
            Submit
          </Button>
          <Button color='primary' onClick={handleTagDescClose}>
            Cancel
          </Button>
        </DialogActions>
      </Dialog>
    </>
  )
}

const mapDispatchToProps = (dispatch: Redux.Dispatch) =>
  bindActionCreators(
    {
      deleteJobTag: deleteJobTag,
      addJobTag: addJobTag,
      addTags: addTags,
    },
    dispatch
  )

export default connect(null, mapDispatchToProps)(JobTags)
