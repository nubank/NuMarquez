[![CircleCI](https://circleci.com/gh/MarquezProject/marquez-python/tree/master.svg?style=shield)](https://circleci.com/gh/MarquezProject/marquez-python/tree/master) [![codecov](https://codecov.io/gh/MarquezProject/marquez-python/branch/master/graph/badge.svg)](https://codecov.io/gh/MarquezProject/marquez-python/branch/master) [![status](https://img.shields.io/badge/status-WIP-yellow.svg)](#status) [![license](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](https://raw.githubusercontent.com/MarquezProject/marquez-python/master/LICENSE) [![Known Vulnerabilities](https://snyk.io/test/github/MarquezProject/marquez-python/badge.svg)](https://snyk.io/test/github/MarquezProject/marquez-python)



# marquez-python
Marquez is an open source **metadata service** for the **collection**, **aggregation**, and **visualization** of a data ecosystem's metadata.
The marquez-python package is a library that interacts with a running instance of the Marquez server.

# Status
This project is under active development at WeWork. The latest published version can be found [here](https://pypi.org/project/marquez-python/). 

## Requirements.

Python 2.7 and 3.4+

## Installation & Usage
### pip install

If the python package is hosted on Github, you can install directly from Github

```sh
pip install git+https://github.com/GIT_USER_ID/GIT_REPO_ID.git
```
(you may need to run `pip` with root permission: `sudo pip install git+https://github.com/GIT_USER_ID/GIT_REPO_ID.git`)

Then import the package:
```python
import marquez_client 
```

### Setuptools

Install via [Setuptools](http://pypi.python.org/pypi/setuptools).

```sh
python setup.py install --user
```
(or `sudo python setup.py install` to install the package for all users)

Then import the package:
```python
import marquez_client
```

## Getting Started

Please follow the [installation procedure](#installation--usage) and then run the following:

Please be sure to set the environmental variables to connect with Marquez:
```
export MQZ_HOST='localhost'
export MQZ_PORT='8080'
```

```python
from __future__ import print_function
import time
import marquez_client
from marquez_client.rest import ApiException
from pprint import pprint

# create an instance of the API class
api_instance = marquez_client.DatasetsApi(marquez_client.ApiClient(configuration))
namespace = wework # str | The name of the namespace. (default to 'wework')

try:
    # List all datasets
    api_response = api_instance.namespaces_namespace_datasets_get(namespace)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling DatasetsApi->namespaces_namespace_datasets_get: %s\n" % e)

```

## Documentation for API Endpoints

All URIs are relative to *http://localhost:5000/api/v1*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*DatasetsApi* | [**namespaces_namespace_datasets_get**](docs/DatasetsApi.md#namespaces_namespace_datasets_get) | **GET** /namespaces/{namespace}/datasets | List all datasets
*JobsApi* | [**jobs_runs_id_abort_put**](docs/JobsApi.md#jobs_runs_id_abort_put) | **PUT** /jobs/runs/{id}/abort | Abort a job run
*JobsApi* | [**jobs_runs_id_complete_put**](docs/JobsApi.md#jobs_runs_id_complete_put) | **PUT** /jobs/runs/{id}/complete | Complete a job run
*JobsApi* | [**jobs_runs_id_fail_put**](docs/JobsApi.md#jobs_runs_id_fail_put) | **PUT** /jobs/runs/{id}/fail | Fail a job run
*JobsApi* | [**jobs_runs_id_get**](docs/JobsApi.md#jobs_runs_id_get) | **GET** /jobs/runs/{id} | Retrieve a job run
*JobsApi* | [**jobs_runs_id_outputs_get**](docs/JobsApi.md#jobs_runs_id_outputs_get) | **GET** /jobs/runs/{id}/outputs | List all job run outputs
*JobsApi* | [**jobs_runs_id_outputs_put**](docs/JobsApi.md#jobs_runs_id_outputs_put) | **PUT** /jobs/runs/{id}/outputs | Create multiple output datasets
*JobsApi* | [**jobs_runs_id_run_put**](docs/JobsApi.md#jobs_runs_id_run_put) | **PUT** /jobs/runs/{id}/run | Start a job run
*JobsApi* | [**namespaces_namespace_jobs_get**](docs/JobsApi.md#namespaces_namespace_jobs_get) | **GET** /namespaces/{namespace}/jobs | List all jobs
*JobsApi* | [**namespaces_namespace_jobs_job_get**](docs/JobsApi.md#namespaces_namespace_jobs_job_get) | **GET** /namespaces/{namespace}/jobs/{job} | Retrieve a job
*JobsApi* | [**namespaces_namespace_jobs_job_put**](docs/JobsApi.md#namespaces_namespace_jobs_job_put) | **PUT** /namespaces/{namespace}/jobs/{job} | Create a job
*JobsApi* | [**namespaces_namespace_jobs_job_runs_get**](docs/JobsApi.md#namespaces_namespace_jobs_job_runs_get) | **GET** /namespaces/{namespace}/jobs/{job}/runs | List all job runs
*JobsApi* | [**namespaces_namespace_jobs_job_runs_post**](docs/JobsApi.md#namespaces_namespace_jobs_job_runs_post) | **POST** /namespaces/{namespace}/jobs/{job}/runs | Create a job run
*JobsApi* | [**namespaces_namespace_jobs_job_versions_get**](docs/JobsApi.md#namespaces_namespace_jobs_job_versions_get) | **GET** /namespaces/{namespace}/jobs/{job}/versions | List all job versions
*NamespacesApi* | [**namespaces_get**](docs/NamespacesApi.md#namespaces_get) | **GET** /namespaces | List all namespaces
*NamespacesApi* | [**namespaces_namespace_get**](docs/NamespacesApi.md#namespaces_namespace_get) | **GET** /namespaces/{namespace} | Retrieve a namespace
*NamespacesApi* | [**namespaces_namespace_put**](docs/NamespacesApi.md#namespaces_namespace_put) | **PUT** /namespaces/{namespace} | Create a namespace


## Documentation For Models

 - [CreateJob](docs/CreateJob.md)
 - [CreateJobRun](docs/CreateJobRun.md)
 - [CreateNamespace](docs/CreateNamespace.md)
 - [DB](docs/DB.md)
 - [Dataset](docs/Dataset.md)
 - [Datasets](docs/Datasets.md)
 - [ICEBERG](docs/ICEBERG.md)
 - [Job](docs/Job.md)
 - [JobRun](docs/JobRun.md)
 - [JobRunId](docs/JobRunId.md)
 - [JobRunOutputs](docs/JobRunOutputs.md)
 - [JobRuns](docs/JobRuns.md)
 - [JobVersion](docs/JobVersion.md)
 - [JobVersions](docs/JobVersions.md)
 - [Jobs](docs/Jobs.md)
 - [Namespace](docs/Namespace.md)
 - [Namespaces](docs/Namespaces.md)


## Documentation For Authorization

 All endpoints do not require authorization.


## Author




