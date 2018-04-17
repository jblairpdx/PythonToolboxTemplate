"""##TODO: Docstring.

Here is where you should explain the contents/purpose of the toolbox.

"""
import logging
import os
import random
import string
import sys
import uuid

import arcpy

if sys.version_info.major >= 3:
    basestring = str


LOG = logging.getLogger(__name__)

# _CONFIG_PATH = __file__[:-4] + '.config.json'
# """str: Path for toolbox configuration file."""
META = {
    'label': os.path.splitext(os.path.basename(__file__))[0].replace('_', ' '),
    'config_path': os.path.splitext(__file__)[0] + '.config.json',
}
"""dict: Toolbox metadata."""


class Toolbox(object):
    """Defines the toolbox.

    Toolbox class is required for constructing an ArcGIS Python toolbox.
    The name of toolbox is the basename of this file.

    Use arcpy.ImportToolbox to attach the toolbox. After attaching,
    reference the tools like `arcpy.{toolbox-alias}.{tool-class-name}`

    """

    def __init__(self):
        """Initialize instance."""
        self.label = META['label']
        """str: Label for the toolbox. Only shows up in toolbox properties."""
        self.alias = '##TODO: Toolbox alias.'
        """str: toolbox namespace when attached to ArcPy."""
        self.tools = [
            ToolExample,
        ]
        """list: Tool classes associated with this toolbox."""


class ToolExample(object):
    """Example of an individual tool in an ArcGIS Python toolbox."""

    def __init__(self):
        """Initialize instance."""
        self.label = "##TODO: Label."
        """str: How tool is named within toolbox."""
        self.category = None
        """str, NoneType: Name of sub-toolset tool will be in (optional)."""
        self.description = "##TODO: Description."
        """str: Longer text describing tool, shown in side panel."""
        self.canRunInBackground = False
        """bool: Flag for whether tool controls ArcGIS focus while running."""
        # self.config = load_config(_CONFIG_PATH).get(self.__class__.__name__, dict())
        # """dict: Tool configuration settings."""

    def getParameterInfo(self):
        """Load parameters into toolbox.

        Recommended: Use `create_parameter` to allow initial
        definition to be a dictionary attribute map.

        Returns:
            list of arcpy.Parameter: Tool parameters.

        """
        parameters = [
            create_parameter(
                {'name': 'example_parameter',
                 'displayName': "Example Parameter (see create_parameter docstring)",
                 'datatype': 'GPVariant',
                 'value': 'EXAMPLE VALUE',},
            ),
            create_parameter(
                {'name': 'overwrite',
                 'displayName': 'Overwrite Output',
                 'datatype': 'GPBoolean',
                 'category': 'Settings', 'value': False}
            ),
            # create_parameter(
            #     {'name': 'save_config_file_on_run',
            #      'displayName': 'Save to Configuration File on Run',
            #      'datatype': 'GPBoolean',
            #      'category': 'Settings', 'value': False}
            # ),
        ]
        # # Apply config values.
        # for parameter in parameters:
        #     if parameter.name not in {'save_config_file'}:
        #         parameter.value = self.config.get(parameter.name, parameter.value)
        return parameters

    def isLicensed(self):
        """Set whether tool is licensed to execute.

        If tool needs extra licensing, returning False prevents execution.

        Returns:
            bool: True if licensed, False otherwise.

        """
        return True

    def updateMessages(self, parameters):
        """Modify messages created by internal validation for each parameter.

        This method is called after internal validation.

        Args:
            parameters (list of arcpy.Parameter): Tool parameters.

        """
        parameter = {param.name: param for param in parameters}
        ##TODO: See if example_parameter's message needs updating.
        if parameter_changed(parameter['example_parameter']):
            pass

    def updateParameters(self, parameters):
        """Modify parameters before internal validation is performed.

        This method is called whenever a parameter has been changed.

        Args:
            parameters (list of arcpy.Parameter): Tool parameters.

        """
        parameter = {param.name: param for param in parameters}
        if parameter_changed(parameter['overwrite']):
            arcpy.env.overwriteOutput = parameter['overwrite'].value
        ##TODO: See if example_parameter's properties need updating.
        if parameter_changed(parameter['example_parameter']):
            pass

    def execute(self, parameters, messages):
        """Execute tool procedure.

        Args:
            parameters (list of arcpy.Parameter): Tool parameters.
            messages (geoprocessing messages object): Tool messages.

        """
        ##TODO: Use value to access parameter values by name (can also add values).
        value = parameter_value_map(parameters)
        # if value['save_config_file_on_run']:
        #     self.config = update_config(self.__class__.__name__, parameters,
        #                                 _CONFIG_PATH)
        # Uncomment for-loop to have info about parameter values in messages.
        # for param in parameters:
        #     messages.AddWarningMessage(param.name + " - " + param.datatype)
        #     messages.AddWarningMessage(value[param.name])
        ##TODO: Execution code.


# Tool-specific objects.

##TODO: Put objects specific to tool(s) only in this toolbox here.


# Utility objects.

##TODO: Put utils here.


# General toolbox objects.

##TODO: Add to create_parameter docstring: 'filters', 'defaultEnvironmentName', 'parameterDependencies'.
def create_parameter(attribute_values):
    """Create ArcPy parameter object using an attribute mapping.

    Note that this doesn't check if the attribute exists in the default
    parameter instance. This means that you can attempt to set a new
    attribute, but the result will depend on how the class implements setattr
    (usually this will just attach the new attribute).

    Args:
        attribute_values (dict): Mapping of attribute names to values.
            {
                # (str): Internal reference name (required).
                'name': name,
                # (str): Label as shown in tool's dialog.
                'displayName': displayName,
                # (str): Direction of the parameter ('Input'/'Output').
                'direction': direction,
                # (str) Parameter data type. See: https://desktop.arcgis.com/en/arcmap/latest/analyze/creating-tools/defining-parameter-data-types-in-a-python-toolbox.htm
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
                # (list of list): data types & names for value table columns.
                # Ex:  [['GPFeatureLayer', 'Features'], ['GPLong', 'Ranks']]
                'columns': columns,
                # (object): Data value of the parameter. Object's type must
                # be Python equivalent of parameter 'datatype'.
                'value': value,
                # (str): Type of filter.
                'filter.type': filter_type,
                # (list): Collection of possible values.
                'filter.list': filter_list,
            }

    Returns:
        arcpy.Parameter: Parameter derived from the attributes.

    """
    default = {'displayName': None, 'direction': 'Input', 'datatype': 'GPVariant',
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


def parameter_changed(parameter):
    """Check whether parameter is in a pre-validation changed state.

    Args:
        arcpy.Parameter: Parameter to check.

    Returns:
        bool: True if changed, False otherwise.

    """
    return all((parameter.altered, not parameter.hasBeenValidated))


def parameter_value(parameter):
    """Get current parameter value.

    If value attribute references a geoprocessing value object, will use this
    function recursively to get the actual value.

    Args:
        parameter (arcpy.Parameter): Parameter to check.

    Returns:
        Current parameter value.

    """
    if hasattr(parameter, 'values'):
        if parameter.values is None:
            value = None
        elif parameter.datatype == 'Value Table':
            value = []
            for row in parameter.values:
                subval = tuple(val if type(val).__name__ != 'geoprocessing value object'
                               else parameter_value(val) for val in row)
                value.append(subval)
            value = tuple(value)
        else:
            value = tuple(val if type(val).__name__ != 'geoprocessing value object'
                          else parameter_value(val) for val in parameter.values)
    else:
        if parameter.value is None:
            value = None
        elif type(parameter.value).__name__ == 'geoprocessing value object':
            value = parameter_value(parameter.value)
        else:
            value = parameter.value
    return value


def parameter_value_map(parameters):
    """Create value map from parameter.

    Args:
        parameters (list of arcpy.Parameter): Tool parameters.

    Returns:
        dict: {parameter-name: parameter-value}

    """
    return {parameter.name: parameter_value(parameter) for parameter in parameters}
