"""
    Script to add system data.
"""

from model.DW import DW
from settings import settings

dw = DW()
cfg = settings.get_config()

for system in cfg['datasources'].keys():
    row = {}
    row['system_name'] = system
    dw.system_dimension.insert(row)
