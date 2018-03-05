"""##TODO: Docstring."""
import logging
import os
import uuid

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
                 'value': 'EXAMPLE VALUE',
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

        Args:
            parameters (list of arcpy.Parameter): Tool parameters.

        """
        # parameter = {param.name: param for param in parameters}
        # if parameter_changed(parameter['example_parameter']):
        #     do_message_updates()
        pass

    def updateParameters(self, parameters):  # pylint: disable=invalid-name,no-self-use
        """Modify parameters before internal validation is performed.

        This method is called whenever a parameter has been changed.

        Args:
            parameters (list of arcpy.Parameter): Tool parameters.

        """
        # parameter = {param.name: param for param in parameters}
        # if parameter_changed(parameter['example_parameter']):
        #     do_parameter_updates
        pass

    def execute(self, parameters, messages):  # pylint: disable=no-self-use
        """Execute tool procedure.

        Args:
            parameters (list of arcpy.Parameter): Tool parameters.
            messages (geoprocessing messages object): Tool messages.

        """
        # Use value to access parameter values by name. Can also add values.
        value = parameter_value_map(parameters)
        # Uncomment for-loop to have info about parameters in messages.
        # for param in parameters:
        #     messages.AddWarningMessage(param.name + " - " + param.datatype)
        #     messages.AddWarningMessage(value[param.name])
        ##TODO: Execution code.


# Tool-specific helpers.

##TODO: Put objects specific to tool(s) only in this toolbox here.

# Helpers.

##TODO: Put more generic objects here.

# General toolbox functions.

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
                subval = tuple(
                    val if type(val).__name__ != 'geoprocessing value object'
                    else parameter_value(val)
                    for val in row
                    )
                value.append(subval)
            value = tuple(value)
        else:
            value = tuple(
                val if type(val).__name__ != 'geoprocessing value object'
                else parameter_value(val)
                for val in parameter.values
                )
    else:
        if parameter.value is None:
            value = None
        elif type(parameter.value).__name__ == 'geoprocessing value object':
            value = parameter_value(parameter.value)
        else:
            value = parameter.value
    return value


def parameter_value_map(parameters):
    """Create value map from parameters.

    Args:
        parameters (list of arcpy.Parameter): Tool parameters.

    Returns:
        dict: {parameter-name: parameter-value}

    """
    return {parameter.name: parameter_value(parameter)
            for parameter in parameters}


# Generic functions.

def unique_dataset_path(prefix='', suffix='', unique_length=4,
                        workspace_path='in_memory'):
    """Create unique temporary dataset path.

    Args:
        prefix (str): String to insert before the unique part of the name.
        suffix (str): String to append after the unique part of the name.
        unique_length (int): Number of unique characters to generate.
        workspace_path (str): Path of workspace to create the dataset in.

    Returns:
        str: Path of the created dataset.

    """
    name = unique_name(prefix, suffix, unique_length,
                       allow_initial_digit=False)
    return os.path.join(workspace_path, name)


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
            unique_id = ''.join(random.choice(seed)
                                for _ in range(string_length))
            if unique_id in used_ids:
                continue
            yield unique_id
    else:
        raise NotImplementedError(
            "Unique IDs for {} type not implemented.".format(data_type)
            )


def unique_name(prefix='', suffix='', unique_length=4,
                allow_initial_digit=True):
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
