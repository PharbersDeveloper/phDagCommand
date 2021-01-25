# -*- coding: utf-8 -*-
from phcli.define_value import *

CLI_VERSION = "2020-11-11"

ASSUME_ROLE_ARN = 'YXJuOmF3cy1jbjppYW06OjQ0NDYwMzgwMzkwNDpyb2xlL1BoLUNsaS1NYXhBdXRv'
ASSUME_ROLE_EXTERNAL_ID = 'Ph-Cli-MaxAuto'

TEMPLATE_BUCKET = "ph-platform"
DAGS_S3_BUCKET = 's3fs-ph-airflow'
DAGS_S3_PREV_PATH = 'airflow/dags/'
DAGS_S3_PHJOBS_PATH = '/jobs/python/phcli/'

ENV_WORKSPACE_KEY = 'PH_WORKSPACE'
ENV_WORKSPACE_DEFAULT = '.'
ENV_CUR_PROJ_KEY = 'PH_CUR_PROJ'
ENV_CUR_PROJ_DEFAULT = '.'
ENV_CUR_IDE_KEY = 'PH_CUR_IDE'
ENV_CUR_IDE_DEFAULT = 'c9'
ENV_CUR_RUNTIME_KEY = 'PH_CUR_RUNTIME'
ENV_CUR_RUNTIME_DEFAULT = 'python3'

TEMPLATE_PHCONF_FILE = "/template/python/phcli/maxauto/phconf-20210104.yaml"
TEMPLATE_PHJOB_FILE_PY = "/template/python/phcli/maxauto/phjob-20210104.tmp"
TEMPLATE_PHJOB_FILE_R = "/template/python/phcli/maxauto/phjob-r-20210122.tmp"
TEMPLATE_PHMAIN_FILE_PY = "/template/python/phcli/maxauto/phmain-20210104.tmp"
TEMPLATE_PHMAIN_FILE_R = "/template/python/phcli/maxauto/phmain-r-20210122.tmp"

TEMPLATE_JUPYTER_PYTHON_FILE = '/template/python/phcli/maxauto/phJupyterPython-20210122.json'
TEMPLATE_JUPYTER_R_FILE = '/template/python/phcli/maxauto/phJupyterR-20210122.json'

TEMPLATE_PHDAG_FILE = "/template/python/phcli/maxauto/phdag-20210104.yaml"
TEMPLATE_PHGRAPHTEMP_FILE = "/template/python/phcli/maxauto/phgraphtemp-20210104.tmp"
TEMPLATE_PHDAGJOB_FILE = "/template/python/phcli/maxauto/phDagJob-20210125.tmp"

PRESET_MUST_ARGS = 'owner, run_id, job_id'
