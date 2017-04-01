"""
This module loads application settings.
"""

import yaml
import os


def __apply_overrides(original, other):
    """
    Recurses over the settings dictionary and applies updates it with the
    values from the local settings override file.

    :param original: application settings dict
    :param other: local settings dict
    """
    for item in other:
        if isinstance(other[item], dict):
            if item in original.keys():
                __apply_overrides(original[item], other[item])
        else:
            original[item] = other[item]


def get_config(local_overrides=True):
    """
    This method loads the application settings into a dictionary. Unless
    local_overrides is set to false, user-defined settings will override
    application settings.

    :param local_overrides: do not override app settings with user settings
    :return: a dictionary containing application settings
    """

    FLIST = ['./ingestion_settings.yml']

    cfg = {}

    for category in FLIST:

        with open(os.path.join(os.path.dirname(__file__), category), 'r') as fh:
            cfg.update(yaml.safe_load(fh))

    if local_overrides:

        with open(os.path.join(os.path.dirname(__file__), 'local_settings.yml'), 'r') as fh:
            local_overrides = yaml.safe_load(fh)

        __apply_overrides(cfg, local_overrides)

    return cfg
