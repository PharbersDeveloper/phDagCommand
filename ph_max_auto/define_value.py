# -*- coding: utf-8 -*-

CLI_VERSION = "2020-08-10"

ASSUME_ROLE_ARN = 'YXJuOmF3cy1jbjppYW06OjQ0NDYwMzgwMzkwNDpyb2xlL1BoLUNsaS1NYXhBdXRv'
ASSUME_ROLE_EXTERNAL_ID = 'Ph-Cli-MaxAuto'

TEMPLATE_BUCKET = "ph-platform"
DAGS_S3_BUCKET = 's3fs-ph-airflow'
DAGS_S3_PREV_PATH = 'airflow/dags/'
DAGS_S3_PHJOBS_PATH = 'airflow/dags/phjobs/'

ENV_WORKSPACE_KEY = 'PH_WORKSPACE'
ENV_WORKSPACE_DEFAULT = '.'
ENV_CUR_PROJ_KEY = 'PH_CUR_PROJ'
ENV_CUR_PROJ_DEFAULT = 'BP_Max_AutoJob'

TEMPLATE_PHJOB_FILE = "/template/python/phcli/maxauto/phjob.tmp"
TEMPLATE_PHCONF_FILE = "/template/python/phcli/maxauto/phconf.yaml"
TEMPLATE_PHMAIN_FILE = "/template/python/phcli/maxauto/phmain.tmp"
TEMPLATE_PHDAG_FILE = "/template/python/phcli/maxauto/phdag.yaml"
TEMPLATE_PHGRAPHTEMP_FILE = "/template/python/phcli/maxauto/phgraphtemp-20200820.tmp"
TEMPLATE_PHDAGJOB_FILE = "/template/python/phcli/maxauto/phDagJob-20200820.tmp"

