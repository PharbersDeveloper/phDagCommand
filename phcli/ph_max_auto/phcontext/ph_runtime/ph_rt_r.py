from .ph_rt_base import PhRTBase


class PhRTR(PhRTBase):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def create(self):
        pass
