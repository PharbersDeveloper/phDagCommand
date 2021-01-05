import subprocess

from .ph_rt_base import PhRTBase
from phcli.ph_max_auto import define_value as dv
from phcli.ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig


class PhRTR(PhRTBase):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def create(self):
        pass
