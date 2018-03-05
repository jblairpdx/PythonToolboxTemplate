"""##TODO: Docstring."""
##TODO: Standard lib imports.
import logging

##TODO: Third-party imports.

##TODO: Local imports.
import arcpy


LOG = logging.getLogger(__name__)


class Toolbox(object):  # pylint: disable=too-few-public-methods
    """Define the toolbox.

    Toolbox class is required for constructing an ArcGIS Python toolbox.
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
        """Initialize instance."""
        self.label = "##TODO: Label."
        """str: How tool is named within toolbox."""
        self.category = None
        """str, NoneType: Name of sub-toolset tool will be in (optional)."""
        self.description = ""  ##TODO: Description.
        """str: Longer text describing tool, shown in side panel."""
        self.canRunInBackground = False  # pylint: disable=invalid-name
        """bool: Flag for whether tool controls ArcGIS focus while running."""

    def getParameterInfo(self):  # pylint: disable=invalid-name,no-self-use
        """Load parameters into toolbox.

        Recommended: Use `parameter_create` to allow initial
        definition to be a dictionary attribute map.

        Returns:
            list of arcpy.Parameter: Tool parameters.

        """
        parameters = [
            parameter_create(
                ##TODO: Parameter attributes.
                {'name': 'example_parameter',
                 'displayName': "Example Parameter",
                 'direction': 'Input',
                 'datatype': 'GPVariant',
                 'parameterType': 'Required',
                 'enabled': True,
                 'category': None,
                 'multiValue': False,
                 'value': True,
                 'symbology': None},
                ),
            ]
        return parameters

    def isLicensed(self):  # pylint: disable=invalid-name,no-self-use
        """Set whether tool is licensed to execute.

        If tool needs extra licensing, returning False prevents execution.

        Returns:
            bool: True if licensed, False otherwise.

        """
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
    """Check whether parameter is in a pre-validation changed state.

    Args:
        arcpy.Parameter: Parameter to check.

    Returns:
        bool: True if changed, False otherwise.

    """
    return all((parameter.altered, not parameter.hasBeenValidated))


##TODO: Implement/test for 'defaultEnvironmentName' (add to docstring).
##TODO: Implement/test for 'parameterDependencies' (add to docstring).
##TODO: Implement/test for 'filters' (add to docstring).
##TODO: Implement/test for 'values' (add to docstring).
def parameter_create(attribute_values):
    """Create ArcPy parameter object using an attribute mapping.

    Note that this doesn't check if the attribute exists in the default
    parameter instance. This means that you can attempt to set a new
    attribute, but the result will depend on how the class implements setattr
    (usually this will just attach the new attribute).

    Args:
        attribute_values (dict): Mapping of attribute names to values.
            {
                # (str): Internal reference name.
                'name': name,
                # (str): Label as shown in tool's dialog.
                'displayName': displayName,
                # (str): Direction of the parameter ('Input'/'Output').
                'direction': direction,
                # (str) Parameter data type.
                # https://desktop.arcgis.com/en/arcmap/latest/analyze/creating-tools/defining-parameter-data-types-in-a-python-toolbox.htm
                'datatype': datatype,
                # (str): Parameter type ('Required'/'Optional'/'Derived').
                'parameterType': parameterType,
                # (bool): Flag to set parameter as enabled or disabled.
                'enabled': enabled,
                # (str, NoneType): Category to include parameter in.
                # Naming category will hide tool in collapsed category on
                # open. Set to None for tool to be at top-level.
                'category': category,
                # (str, NoneType): Path to layer file used for drawing output.
                # Set to None to omit symbology.
                'symbology': symbology,
                # (bool): Flag to set whether parameter is multi-valued.
                'multiValue': multiValue,
                # (object): Data value of the parameter. Object's type must
                # be Python equivalent of parameter 'datatype'.
                'value': value,
                # (list of list): data types & names for value table columns.
                # Ex:  [['GPFeatureLayer', 'Features'], ['GPLong', 'Ranks']]
                'columns': columns,
                # (str): Type of filter.
                'filter.type': filter_type,
                # (list): Collection of possible values.
                'filter.list': filter_list,
            }

    Returns:
        arcpy.Parameter: Parameter derived from the attributes.

    """
    default = {'name': unique_name(), 'displayName': None,
               'direction': 'Input', 'datatype': 'GPVariant',
               'parameterType': 'Optional', 'enabled': True, 'category': None,
               'symbology': None, 'multiValue': False, 'value': None}
    parameter = arcpy.Parameter()
    for attr, value in attribute_values.items():
        # Apply filter later.
        if attr.startswith('filter.'):
            continue
        else:
            setattr(parameter, attr, value)
    # Set defaults for initial attributes not in attribute_values.
    for attr, value in default.items():
        if attr in attribute_values:
            continue
        setattr(parameter, attr, value)
    # Filter attributes don't stick using setattr.
    if 'filter.type' in attribute_values:
        parameter.filter.type = attribute_values['filter.type']
    if 'filter.list' in attribute_values:
        parameter.filter.list = attribute_values['filter.list']
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
