# -*- coding=utf-8 -*-
"""##TODO: Docstring."""
import logging
import os
import uuid

import arcpy


LOG = logging.getLogger(__name__)

PARAMETER_ATTRIBUTES = {
    'parameter_example': {
        'name': 'parameter_example',
        'displayName': "Example Parameter",
        'direction': 'Input',  # Input or Output.
        'datatype': 'GPBoolean',  # Ref: http://desktop.arcgis.com/en/arcmap/latest/analyze/creating-tools/defining-parameter-data-types-in-a-python-toolbox.htm
        'parameterType': 'Required',  # Required, Optional, or Derived.
        'enabled': True,
        'category': None,  # Optional.
        'multiValue': False,
        'value': False,  # Initial value on run.
        'symbology': None  # Path to layer file for drawing output. Optional.
        },
    }


class Toolbox(object):
    """Define the toolbox.

    Toolbox class is required for constructing and ArcGIS Python toolbox.
    The name of toolbox is the basename of this file.
    """

    def __init__(self):
        self.label = "##TODO: Toolbox label."
        # Sets namespace of toolbox when attached to ArcPy (arcpy.{alias}).
        # Attach using arcpy.AddToolbox().
        self.alias = '##TODO: Toolbox alias.'
        # List of tool classes associated with this toolbox.
        self.tools = [
            # Add tools here by their class name to make visible in toolbox.
            ToolExample,
            ]


class ToolExample(object):
    """Example of an individual tool in an ArcGIS Python toolbox."""

    def __init__(self):
        # Sets how the tool is named within the toolbox.
        self.label = "##TODO: Label."
        # Sets name of toolset tool will be placed in (optional).
        self.category = None
        # Sets longer description of the tool, shown in the side panel.
        self.description = "##TODO: Description."
        # Sets whether the tool controls ArcGIS while running or not.
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Load parameters into toolbox."""
        # Create the parameters in a separate place (allows reusability),
        # then add them here. Recommended: use parameter_from_attributes
        # to allow initial definition to be a dictionary/attribute map.
        return [parameter_from_attributes(PARAMETER_ATTRIBUTES[name])
                for name in ['parameter_example']]

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        # If tool needs extra licensing, checking here will prevent execution.
        return True

    def updateParameters(self, parameters):
        """Modify parameters before internal validation is performed.

        This method is called whenever a parameter has been changed.
        """
        # Follow the below format for checking for changes. Remove if unused.
        parameter_map = {parameter.name: parameter for parameter in parameters}
        if all([parameter_map['a_parameter'].altered,
                not parameter_map['a_parameter'].hasBeenValidated]):
            # Do something.
            pass
        return

    def updateMessages(self, parameters):
        """Modify messages created by internal validation for each parameter.

        This method is called after internal validation.
        """
        return

    def execute(self, parameters, messages):
        """Procedural code of the tool."""
        # value_map contains dictionary with parameter name/value key/values.
        value_map = parameter_value_map(parameters)
        # Do the steps of the tool.
        messages.addMessage("Can do messages, too.")
        return


# Functions & generators.

def parameter_from_attributes(attribute_map):
    """Create ArcPy parameter object using an attribute mapping.

    Note that this doesn't check if the attribute exists in the default
    parameter instance. This means that you can attempt to set a new
    attribute, but the result will depend on how the class implements setattr
    (usually this will just attach the new attribute).
    """
    parameter = arcpy.Parameter()
    for attribute_name, attribute_value in attribute_map.items():
        # Filter list doesn't stick using setattr.
        if attribute_name == 'filter.list':
            parameter.filter.list = attribute_value
        else:
            setattr(parameter, attribute_name, attribute_value)
    return parameter


def parameter_value(parameter):
    """Return value of parameter."""
    def handle_value_object(value_object):
        """Return actual value from value object.

        Some values embedded in 'value object' (.value.value), others aren't.
        """
        return getattr(value_object, 'value', value_object)
    if not parameter.multiValue:
        return handle_value_object(parameter.value)
    # Multivalue parameters place their values in .values (.value. holds a
    # ValueTable object).
    else:
        return [handle_value_object(value) for value in parameter.values]


def parameter_value_map(parameters):
    """Create value map from ArcPy parameter objects."""
    return {parameter_value(parameter) for parameter in parameters}


def unique_ids(data_type=uuid.UUID, string_length=4):
    """Generator for unique IDs."""
    if data_type in (float, int):
        unique_id = data_type()
        while True:
            yield unique_id
            unique_id += 1
    elif data_type in [uuid.UUID]:
        while True:
            yield uuid.uuid4()
    elif data_type in [str]:
        used_ids = set()
        while True:
            unique_id = str(uuid.uuid4())[:string_length]
            while unique_id in used_ids:
                unique_id = str(uuid.uuid4())[:string_length]
            yield unique_id
    else:
        raise NotImplementedError(
            "Unique IDs for {} type not implemented.".format(data_type))


def unique_name(prefix='', suffix='', unique_length=4):
    """Generate unique name."""
    return '{}{}{}'.format(
        prefix, next(unique_ids(str, unique_length)), suffix)


def unique_temp_dataset_path(prefix='', suffix='', unique_length=4,
                             workspace='in_memory'):
    """Create unique temporary dataset path."""
    return os.path.join(workspace, unique_name(prefix, suffix, unique_length))
