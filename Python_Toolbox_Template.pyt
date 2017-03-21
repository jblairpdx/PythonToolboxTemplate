# -*- coding=utf-8 -*-
"""##TODO: Docstring."""
##TODO: Standard lib imports.
import logging

##TODO: Third-party imports.

##TODO: Local imports.
import arcpy


LOG = logging.getLogger(__name__)

PARAMETER_ATTRIBUTES = {
    'example_parameter': {
        # Match parameter name to dictionary key.
        'name': 'example_parameter',
        'displayName': "Example Parameter",
        # Direction: 'Input' or 'Output'.
        'direction': 'Input',
        # datatype: http://desktop.arcgis.com/en/arcmap/latest/analyze/creating-tools/defining-parameter-data-types-in-a-python-toolbox.htm
        'datatype': 'GPBoolean',
        # parameterType: 'Required', 'Optional', or 'Derived'.
        'parameterType': 'Required',
        # emabled: True or False.
        'enabled': True,
        # category (optional). Note having one will collapse category on open.
        'category': None,
        'multiValue': False,
        # Value type must be Python type match for datatype.
        'value': True,
        # symbology (optional): Path to layer file for drawing output.
        'symbology': None,
        },
    }


class Toolbox(object):  # pylint: disable=too-few-public-methods
    """Define the toolbox.

    Toolbox class is required for constructing and ArcGIS Python toolbox.
    The name of toolbox is the basename of this file.
    """

    def __init__(self):
        self.label = "##TODO: Toolbox label."
        # Alias is toolbox namespace when attached to ArcPy (arcpy.{alias}).
        # Attach using arcpy.AddToolbox().
        self.alias = '##TODO: Toolbox alias.'
        # List of tool classes associated with this toolbox.
        # self.tools must be list (not other iterable).
        self.tools = [
            # Add tools here by their class name to make visible in toolbox.
            ToolExample,
            ]


class ToolExample(object):
    """Example of an individual tool in an ArcGIS Python toolbox."""

    def __init__(self):
        # Label is how tool is named within toolbox.
        self.label = "##TODO: Label."
        # Category is name of sub-toolset tool will be in (optional).
        self.category = None
        # Description is longer text for tool, shown in side panel.
        self.description = """
            ##TODO: Description.
            """
        # Sets whether the tool controls ArcGIS  focus while running.
        self.canRunInBackground = False  # pylint: disable=invalid-name
        # Recommended: collect parameter attributes here, to have a default
        # reference in instance.
        self.parameter_attributes = (
            PARAMETER_ATTRIBUTES['example_parameter'],
            )

    def getParameterInfo(self):  # pylint: disable=invalid-name,no-self-use
        """Load parameters into toolbox."""
        # Create the parameters in a separate place (allows reusability),
        # then add them here. Recommended: use parameter_from_attributes
        # to allow initial definition to be a dictionary/attribute map.
        # Return value must be list (not other iterable).
        parameters = [parameter_from_attributes(attributes)
                      for attributes in self.parameter_attributes]
        return parameters

    def isLicensed(self):  # pylint: disable=invalid-name,no-self-use
        """Set whether tool is licensed to execute."""
        # If tool needs extra licensing, checking here will prevent execution.
        return True

    def updateMessages(self, parameters):  # pylint: disable=invalid-name,no-self-use
        """Modify messages created by internal validation for each parameter.

        This method is called after internal validation.
        """
        # No update requirements at this time.
        pass

    def updateParameters(self, parameters):  # pylint: disable=invalid-name,no-self-use
        """Modify parameters before internal validation is performed.

        This method is called whenever a parameter has been changed.
        """
        # Follow the below format for checking for changes.
        # Same code can be used for updateMessages.
        # Remove code if not needed.
        parameter_map = {parameter.name: parameter for parameter in parameters}
        if parameter_changed(parameter_map['a_parameter']):
            # Do something.
            pass

    def execute(self, parameters, messages):  # pylint: disable=no-self-use
        """Procedural code of the tool."""
        # Set up logger-like object, logs to both ArPy and file's logger.
        log = ArcLogger(loggers=[LOG])
        # value_map contains dictionary with parameter name/value key/values.
        value_map = parameter_value_map(parameters)
        log.info("TODO: Steps of the tool here.")


# Tool-specific helpers.

##TODO: Put objects specific to tool(s) only in this toolbox here.

# Helpers.

##TODO: Put more generic objects here.

class ArcLogger(object):
    """Faux-logger for logging to ArcPy/ArcGIS messaging system."""

    arc_function = {
        logging.NOTSET: (lambda msg: None),
        # No debug level in Arc messaging system 👎.
        logging.DEBUG: (lambda msg: None),
        logging.INFO: arcpy.AddMessage,
        logging.WARNING: arcpy.AddWarning,
        logging.ERROR: arcpy.AddError,
        # No debug level in Arc messaging system 👎. Map to error level.
        logging.CRITICAL: arcpy.AddError,
        }

    def __init__(self, loggers=None):
        """Instance initialization."""
        self.loggers = loggers if loggers else []

    def debug(self, msg):
        """Log message with level DEBUG."""
        self.log(logging.DEBUG, msg)

    def info(self, msg):
        """Log message with level INFO."""
        self.log(logging.INFO, msg)

    def warning(self, msg):
        """Log message with level WARNING."""
        self.log(logging.WARNING, msg)

    def error(self, msg):
        """Log message with level ERROR."""
        self.log(logging.ERROR, msg)

    def critical(self, msg):
        """Log message with level CRITICAL."""
        self.log(logging.CRITICAL, msg)

    def log(self, lvl, msg):
        """Log message with level lvl."""
        self.arc_function[lvl](msg)
        for logger in self.loggers:
            logger.log(lvl, msg)


def parameter_changed(parameter):
    """Return True if parameter is in a pre-validation changed state."""
    return all((parameter.altered, not parameter.hasBeenValidated))


def parameter_from_attributes(attribute_map):
    """Create ArcPy parameter object using an attribute mapping.

    Note that this doesn't check if the attribute exists in the default
    parameter instance. This means that you can attempt to set a new
    attribute, but the result will depend on how the class implements setattr
    (usually this will just attach the new attribute).
    """
    parameter = arcpy.Parameter()
    for attribute_name, attribute_value in attribute_map.items():
        # Apply filter later.
        if attribute_name.startswith('filter.'):
            continue
        else:
            setattr(parameter, attribute_name, attribute_value)
    # Filter attributes don't stick using setattr.
    if 'filter.type' in attribute_map:
        parameter.filter.type = attribute_map['filter.type']
    if 'filter.list' in attribute_map:
        parameter.filter.list = attribute_map['filter.list']
    return parameter


def parameter_value(parameter):
    """Return value of parameter."""
    def handle_value_object(value_object):
        """Return actual value from value object.

        Some values embedded in 'value object' (.value.value), others aren't.
        """
        return getattr(value_object, 'value', value_object)
    if not parameter.multiValue:
        result = handle_value_object(parameter.value)
    # Multivalue parameters place their values in .values (.value. holds a
    # ValueTable object).
    else:
        result = [handle_value_object(value) for value in parameter.values]
    return result


def parameter_value_map(parameters):
    """Create value map from ArcPy parameter objects."""
    return {parameter.name: parameter_value(parameter)
            for parameter in parameters}
