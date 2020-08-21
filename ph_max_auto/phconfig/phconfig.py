# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the YAML Config for Pharbers jobs
"""
import yaml

from ph_errs.ph_err import exception_function_not_implement
from ph_max_auto.phdagspec.phdagspec import PhYAMLDAGSpec
from ph_max_auto.phspec.phspec import PhYAMLSpec
from ph_max_auto.phmetadata.phmetadata import PhYAMLMetadata


class PhYAMLConfig(object):
    def __init__(self, path, name="/phconf.yaml"):
        self.path = path
        self.name = name
        self.apiVersion = ""
        self.kind = ""
        self.metadata = ""
        self.spec = ""

    def dict2obj(self, dt):
        self.__dict__.update(dt)

    def load_yaml(self, stream=''):
        if not stream:
            stream = open(self.path + self.name)

        y = yaml.safe_load(stream)
        self.dict2obj(y)
        if self.kind == "PhJob":
            self.metadata = PhYAMLMetadata(self.metadata)
            self.spec = PhYAMLSpec(self.spec)
        elif self.kind == "PhDag":
            self.metadata = PhYAMLMetadata(self.metadata)
            self.spec = PhYAMLDAGSpec(self.spec)
        else:
            raise exception_function_not_implement
