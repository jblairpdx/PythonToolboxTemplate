"""##TODO: Docstring.

Here is where you should explain the contents/purpose of the toolbox.
"""
import json
import logging
import os
import sys

import arcpy

if sys.version_info.major >= 3:
    basestring = str
    """Defining a basestring type instance for Py3+."""


LOG = logging.getLogger(__name__)
"""logging.Logger: Toolbox-level logger."""
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

    def getParameterInfo(self):
        """Load parameters into toolbox.

        Recommended: Use `create_parameter` to allow initial
        definition to be a dictionary attribute map.

        Returns:
            list of arcpy.Parameter: Tool parameters.
        """
        parameters = [
            ##TODO: See create_parameter docs for info about args.
            create_parameter(name='example_parameter',
                             displayName='Example Parameter',
                             datatype='GPVariant',
                             value='EXAMPLE VALUE'),
            create_parameter(name='overwrite',
                             displayName='Overwrite Output',
                             datatype='GPBoolean',
                             category='Settings',
                             value=False),
            create_parameter(name='save_config_file_on_run',
                             displayName='Save to Configuration File on Run',
                             datatype='GPBoolean',
                             category='Settings',
                             value=False),
        ]
        # Apply config values.
        config = load_config(META['config_path'], self.__class__.__name__)
        if config:
            for parameter in parameters:
                if parameter.name not in {'save_config_file'}:
                    parameter.value = config.get(parameter.name, parameter.value)
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
        if value.get('save_config_file_on_run'):
            update_config(META['config_path'], self.__class__.__name__, parameters)
        # Uncomment loop to have info about parameter values in messages.
        # for param in parameters:
        #     messages.AddWarningMessage(param.name + " - " + param.datatype)
        #     messages.AddWarningMessage(value[param.name])
        ##TODO: Execution code.


# Tool-specific objects.

##TODO: Put objects specific to tool(s) only in this toolbox here.


# Geoprocessing objects.

##TODO: Put geoprocessing objects here.


# Utility objects.

##TODO: Put utility objects here.


# General toolbox objects.

def load_config(config_path, tool_name=None):
    """Load configuration from toolbox configuration file.

    Args:
        config_path (str): Path for the config file.
        tool_name (str): Name of the tool class. If None, will load entire contents of
            config file.

    Returns:
        dict: Configuration settings if file exists; otherwise empty dictionary.
    """
    if os.path.exists(config_path):
        with open(config_path) as _file:
            all_config = json.load(_file)
            config = all_config.get(tool_name, {}) if tool_name else all_config
    else:
        config = {}
    return config


def update_config(config_path, tool_name, parameters):
    """Updates tool configuration data in toolbox configuration file.

    Args:
        config_path (str): Path for the config file.
        tool_name (str): Name of the tool class.
        parameters (iter of arcpy.Parameter): Collection of parameter objects to save
            values of in the config file.
    """
    all_config = load_config(config_path)
    old_config = all_config.get(tool_name, {})
    new_config = {param.name: param.valueAsText for param in parameters}
    if old_config != new_config:
        all_config[tool_name] = new_config
        with open(config_path, 'w') as config_file:
            json.dump(all_config, config_file)


def create_parameter(name, **kwargs):
    """Create ArcPy parameter object using an attribute mapping.

    Note that this doesn't check if the attribute exists in the default
    parameter instance. This means that you can attempt to set a new
    attribute, but the result will depend on how the class implements setattr
    (usually this will just attach the new attribute).

    Args:
        name (str): Internal reference name for parameter (required).

    Keyword Args:
        displayName (str): Label as shown in tool's dialog. Default is parameter name.
        direction (str): Direction of the parameter: Input or Output. Default is Input.
        datatype (str): Parameter data type. Default is GPVariant. See
            https://desktop.arcgis.com/en/arcmap/latest/analyze/creating-tools/defining-parameter-data-types-in-a-python-toolbox.htm
        parameterType (str): Parameter type: Optional, Required, or Derived. Default is
            Optional.
        enabled (bool): Flag to set parameter as enabled or disabled. Default is True.
        category (str, NoneType): Category to include parameter in. Naming a category
            will hide tool in collapsed category on open. Set to None (the default) for
            tool to be at top-level.
        symbology (str, NoneType): Path to layer file used for drawing output. Set to
            None (the default) to omit symbology.
        multiValue (bool): Flag to set whether parameter is multi-valued. Default is
            False.
        value (object): Data value of the parameter. Object's type must be the Python
            equivalent of parameter 'datatype'. Default is None.
        columns (list of list): Ordered collection of data type/name pairs for value
            table columns. Ex: `[['GPFeatureLayer', 'Features'], ['GPLong', 'Ranks']]`
        filter_type (str): Type of filter to apply: ValueList, Range, FeatureClass,
            File, Field, or Workspace.
        filter_list (list): Collection of possible values allowed by the filter type.
            Default is an empty list.
        parameterDependencies (list): Collection other parameter's names that this
            parameter's value depends upon.

    Returns:
        arcpy.Parameter: Parameter derived from the attributes.
    """
    kwargs.setdefault('displayName', name)
    kwargs.setdefault('direction', 'Input')
    kwargs.setdefault('datatype', 'GPVariant')
    kwargs.setdefault('parameterType', 'Optional')
    kwargs.setdefault('enabled', True)
    kwargs.setdefault('category')
    kwargs.setdefault('symbology')
    kwargs.setdefault('multiValue', False)
    kwargs.setdefault('value')
    # DO NOT SET DEFAULT: kwargs.setdefault('filter_list', [])
    # DO NOT SET DEFAULT: kwargs.setdefault('parameterDependencies', [])
    parameter = arcpy.Parameter(name)
    for attr, value in kwargs.items():
        # Apply filter properties later.
        if attr.startswith('filter_'):
            continue
        else:
            setattr(parameter, attr, value)
    for key in ['filter_type', 'filter_list']:
        if key in kwargs:
            setattr(parameter.filter, key.replace('filter_', ''), kwargs[key])
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
