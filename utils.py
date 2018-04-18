"""Mostly self-contained functions for geoprocessing in Python toolboxes."""
from collections import Iterable
import inspect
import logging
import math
import os
import random
import string
import sys
import uuid

if sys.version_info.major >= 3:
    basestring = str


def clean_whitespace(value, clear_empty_string=True):
    """Return value with whitespace stripped & deduplicated.

    Args:
        value (str): Value to clean.
        clear_empty_string (bool): Convert empty string results to NoneTypes if True.

    Returns
        str, NoneType: Cleaned value.

    """
    if value is not None:
        value = value.strip()
        for character in string.whitespace:
            while character*2 in value:
                value = value.replace(character*2, character)
    if clear_empty_string and not value:
        value = None
    return value


def contain(obj, nonetypes_as_empty=True):
    """Generate contained items if a collection, otherwise generate object.

    Args:
        obj: Any object, collection or otherwise.
        nontypes_as_empty (bool): True if NoneTypes treated as an empty
            collection, otherwise False.

    Yields:
        obj or its contents.

    """
    if nonetypes_as_empty and obj is None:
        return
    if inspect.isgeneratorfunction(obj):
        obj = obj()
    if isinstance(obj, Iterable) and not isinstance(obj, basestring):
        for i in obj:
            yield i
    else:
        yield obj


def describe_attribute_change(attribute_key, new_attribute_value, **kwargs):
    """Return description of an attribute change (useful for logging).

    Args:
        attribute_key (str): Name of the attribute.
        new_attribute_value: New value of the attribute.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        feature_id_key (str): Name of the feature ID attribute.
        feature_id_value: Value of the feature ID.
        old_attribute_value: Old value of the attribute.

    Returns:
        str: Change description.

    """
    desc = "Changed {}=".format(attribute_key)
    if 'old_attribute_value' in kwargs:
        desc += "{old_attribute_value!r} --> ".format(**kwargs)
    desc += "{!r}".format(new_attribute_value)
    if 'feature_id_key' in kwargs:
        kwargs.setdefault('feature_id_value')
        desc += " for {feature_id_key}={feature_id_value!r}".format(**kwargs)
    desc += "."
    return desc


def get_bearing(coord0, coord1):
    """Find directional bearing or angle of input coordinates.

    Args:
        coord0 (iter): Two-part iterable of an X- & Y-value for the first point.
        coord0 (iter): Two-part iterable of an X- & Y-value for the second point.

    Returns:
        float: Directional bearing for the coordinates.

    """
    if coord0 == coord1:
        raise ValueError("Coordinates are the same point.")
    x_0, y_0 = coord0
    x_1, y_1 = coord1
    run = x_1 - x_0
    rise = y_1 - y_0
    try:
        theta_angle = math.degrees(math.atan(abs(run / rise)))
    except ZeroDivisionError:
        theta_angle = None
    if theta_angle is None:
        # Bearing is either 90 or 270 (cannot divide by zero for equation).
        bearing = 90 if run > 0 else 270
    else:
        # Top-right quadrant (0-89.x).
        if run >= 0 and rise > 0:
            bearing = theta_angle
        # Lower-right quadrant (90.x-180).
        if run >= 0 and rise < 0:
            bearing = 180 - theta_angle
        # Lower-left quadrant (180-269.x). Don't care about 180 overlap.
        if run <= 0 and rise < 0:
            bearing = 180 + theta_angle
        # Top-left quadrant (270.x-359.x). Do care about 360 overlap 0.
        if run < 0 and rise > 0:
            bearing = 360 - theta_angle
    return bearing


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
