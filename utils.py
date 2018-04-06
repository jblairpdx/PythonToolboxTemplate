"""Mostly self-contained functions for geoprocessing in Python toolboxes."""
import logging
import os
import random
import string
import sys
import uuid

if sys.version_info.major <= 3:
    basestring = str


##TODO: Create ConfigManager class, with load_config & update_config as methods.
##TODO: Prettyprint json for config output.
##TODO: OrderedDict for config manager internal.
##TODO: Wrap exception for bad json in load?
##TODO: Decorator method for applying config values in getParameterInfo?
##TODO: Decorator method for adding self.config to tool on init?
def load_config(config_path):
    """Load configuration from file.

    Args:
        config_path (str): Path for the config file.

    Returns:
        dict: Configuration settings if file exists; otherwise empty dictionary.

    """
    import json
    if os.path.exists(config_path):
        with open(config_path) as _file:
            config = json.load(_file)
    else:
        config = {}
    return config


def update_config(tool_name, parameters, config_path=None):
    """Updates tool configuration data in toolbox configuration file.

    Args:
        tool_name (object): Instance of the tool class.
        config_path (str): Path for the config file. If config_path is None, no file
            will be updated.

    Returns:
        dict: Updated copy of configuration settings.

    """
    import json
    _config = load_config(config_path) if config_path else {}
    old_config = _config.get(tool_name, {})
    new_config = {param.name: param.valueAsText for param in parameters}
    if old_config != new_config:
        _config[tool_name] = new_config
        if config_path:
            with open(config_path, 'w') as _file:
                json.dump(_config, _file)
    return new_config


def leveled_logger(logger, level_repr=None):
    """Return function to log into logger at the given level.

    Args:
        logger (logging.Logger): Logger to log to.
        level_repr: Representation of the logging level.

    Returns:
        function.

    """
    def _logger(msg, *args, **kwargs):
        return logger.log(log_level(level_repr), msg, *args, **kwargs)
    return _logger


def log_level(level_repr=None):
    """Return integer for logging module level.

    Args:
        level_repr: Representation of the logging level.

    Returns:
        int: Logging module level.

    """
    level = {None: 0, 'debug': logging.DEBUG, 'info': logging.INFO,
             'warning': logging.WARNING, 'error': logging.ERROR,
             'critical': logging.CRITICAL}
    if level_repr in level.values():
        result = level_repr
    elif level_repr is None:
        result = level[level_repr]
    elif isinstance(level_repr, basestring):
        result = level[level_repr.lower()]
    else:
        raise RuntimeError("level_repr invalid.")
    return result


def unique_ids(data_type=uuid.UUID, string_length=4):
    """Generator for unique IDs.

    Args:
        data_type: Type object to create unique IDs as.
        string_length (int): Length to make unique IDs of type string.
            Ignored if data_type is not a stringtype.

    Yields:
        Unique ID.

    """
    if data_type in (float, int):
        # Skip 0 (problematic - some processing functions use 0 for null).
        unique_id = data_type(1)
        while True:
            yield unique_id
            unique_id += 1
    elif data_type in (uuid.UUID,):
        while True:
            yield uuid.uuid4()
    elif data_type in (str,):
        seed = string.ascii_letters + string.digits
        used_ids = set()
        while True:
            unique_id = ''.join(random.choice(seed) for _ in range(string_length))
            if unique_id in used_ids:
                continue
            yield unique_id
    else:
        raise NotImplementedError(
            "Unique IDs for {} type not implemented.".format(data_type)
            )


def unique_name(prefix='', suffix='', unique_length=4, allow_initial_digit=True):
    """Generate unique name.

    Args:
        prefix (str): String to insert before the unique part of the name.
        suffix (str): String to append after the unique part of the name.
        unique_length (int): Number of unique characters to generate.
        allow_initial_number (bool): Flag indicating whether to let the
            initial character be a number. Defaults to True.

    Returns:
        str: Unique name.

    """
    name = prefix + next(unique_ids(str, unique_length)) + suffix
    if not allow_initial_digit and name[0].isdigit():
        name = unique_name(prefix, suffix, unique_length, allow_initial_digit)
    return name


def unique_path(prefix='', suffix='', unique_length=4, workspace_path='in_memory'):
    """Create unique temporary dataset path.

    Args:
        prefix (str): String to insert before the unique part of the name.
        suffix (str): String to append after the unique part of the name.
        unique_length (int): Number of unique characters to generate.
        workspace_path (str): Path of workspace to create the dataset in.

    Returns:
        str: Path of the created dataset.

    """
    name = unique_name(prefix, suffix, unique_length, allow_initial_digit=False)
    return os.path.join(workspace_path, name)
