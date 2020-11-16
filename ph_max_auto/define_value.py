# -*- coding: utf-8 -*-

CLI_VERSION = "2020-11-11"

ASSUME_ROLE_ARN = 'YXJuOmF3cy1jbjppYW06OjQ0NDYwMzgwMzkwNDpyb2xlL1BoLUNsaS1NYXhBdXRv'
ASSUME_ROLE_EXTERNAL_ID = 'Ph-Cli-MaxAuto'

TEMPLATE_BUCKET = "ph-platform"
DAGS_S3_BUCKET = 's3fs-ph-airflow'
DAGS_S3_PREV_PATH = 'airflow/dags/'
DAGS_S3_PHJOBS_PATH = 'airflow/dags/phjobs/'

ENV_WORKSPACE_KEY = 'PH_WORKSPACE'
ENV_WORKSPACE_DEFAULT = '.'
ENV_CUR_PROJ_KEY = 'PH_CUR_PROJ'
ENV_CUR_PROJ_DEFAULT = '.'

TEMPLATE_PHCONF_FILE = "/template/python/phcli/maxauto/phconf-20201116.yaml"
TEMPLATE_PHDAG_FILE = "/template/python/phcli/maxauto/phdag-20201110.yaml"
TEMPLATE_PHGRAPHTEMP_FILE = "/template/python/phcli/maxauto/phgraphtemp-20201110.tmp"
TEMPLATE_PHDAGJOB_FILE = "/template/python/phcli/maxauto/phDagJob-20201110.tmp"


TEMPLATE_PHJOB_FILE_PY = "/template/python/phcli/maxauto/phjob-20201116.tmp"
TEMPLATE_PHJOB_FILE_R = "/template/python/phcli/maxauto/phjob.r.tmp"

TEMPLATE_PHMAIN_FILE_PY = "/template/python/phcli/maxauto/phmain-20201116.tmp"
TEMPLATE_PHMAIN_FILE_R = "/template/python/phcli/maxauto/phmain.r.tmp"
