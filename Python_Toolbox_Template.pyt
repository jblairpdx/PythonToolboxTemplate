# -*- coding=utf-8 -*-
"""##TODO: Docstring."""
import logging
import os
import uuid

import arcpy


LOG = logging.getLogger(__name__)

PARAMETER_ATTRIBUTES = (
    {'name': 'parameter_example',
     'displayName': "Example Parameter",
     'direction': 'Input',  # Input or Output.
     'datatype': 'GPBoolean',  # Ref: http://desktop.arcgis.com/en/arcmap/latest/analyze/creating-tools/defining-parameter-data-types-in-a-python-toolbox.htm
     'parameterType': 'Required',  # Required, Optional, or Derived.
     'enabled': True,
     'category': None,  # Optional.
     'multiValue': False,
     'value': False,  # Initial value on run.
     'symbology': None,  # Path to layer file for drawing output. Optional.
     'meta_tags' = ['tool_example']},  # Tag collection for referencing related objects in-code.
    )


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
        return [parameter_from_attributes(attributes)
                for attributes in PARAMETER_ATTRIBUTES]

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
        if parameter_changed(parameter_map['a_parameter']):
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

def attributes_as_dicts(dataset_path, field_names=None, **kwargs):
    """Generator for dictionaries of feature attributes.

    Args:
        dataset_path (str): Path of dataset.
        field_names (iter): Iterable of field names.
    Kwargs:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Yields:
        dict.
    """
    for kwarg_default in [('dataset_where_sql', None),
                          ('spatial_reference_id', None)]:
        kwargs.setdefault(*kwarg_default)
    with arcpy.da.SearchCursor(
        in_table=dataset_path, field_names=field_names if field_names else '*',
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=spatial_reference_as_arc(
            kwargs['spatial_reference_id']
            )
        ) as cursor:
        for feature in cursor:
            yield dict(zip(cursor.fields, feature))


def attributes_as_iters(dataset_path, field_names=None, **kwargs):
    """Generator for iterables of feature attributes.

    Args:
        dataset_path (str): Path of dataset.
        field_names (iter): Iterable of field names.
    Kwargs:
        iter_type (object): Python iterable type to yield.
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_id (int): EPSG code indicating the spatial reference
            output geometry will be in.
    Yields:
        iter.
    """
    for kwarg_default in [('dataset_where_sql', None), ('iter_type', tuple),
                          ('spatial_reference_id', None)]:
        kwargs.setdefault(*kwarg_default)
    with arcpy.da.SearchCursor(
        in_table=dataset_path, field_names=field_names if field_names else '*',
        where_clause=kwargs['dataset_where_sql'],
        spatial_reference=spatial_reference_as_arc(
            kwargs['spatial_reference_id']
            )
        ) as cursor:
        for feature in cursor:
            yield kwargs['iter_type'](feature)


def dataset_metadata(dataset_path):
    """Return dictionary of dataset metadata.

    Args:
        dataset_path (str): Path of dataset.
    Returns:
        dict.
    """
    describe_object = arcpy.Describe(dataset_path)
    meta = {
        'name': getattr(describe_object, 'name'),
        'path': getattr(describe_object, 'catalogPath'),
        'data_type': getattr(describe_object, 'dataType'),
        'workspace_path': getattr(describe_object, 'path'),
        # Do not use getattr! Tables can not have OIDs.
        'is_table': hasattr(describe_object, 'hasOID'),
        'is_versioned': getattr(describe_object, 'isVersioned', False),
        'oid_field_name': getattr(describe_object, 'OIDFieldName', None),
        'is_spatial': hasattr(describe_object, 'shapeType'),
        'geometry_type': getattr(describe_object, 'shapeType', None),
        'geometry_field_name': getattr(describe_object, 'shapeFieldName', None),
        'field_names': [], 'fields': [],
        'user_field_names': [], 'user_fields': [],
        }
    for field in getattr(describe_object, 'fields', ()):
        meta['field_names'].append(field.name)
        meta['fields'].append(field_as_metadata(field))
        if all([field.name != meta['oid_field_name'],
                '{}.'.format(meta['geometry_field_name']) not in field.name]):
            meta['user_field_names'].append(field.name)
            meta['user_fields'].append(field_as_metadata(field))
    if hasattr(describe_object, 'spatialReference'):
        meta['arc_spatial_reference'] = getattr(describe_object,
                                                'spatialReference')
        meta['spatial_reference_id'] = getattr(meta['arc_spatial_reference'],
                                               'factoryCode')
    else:
        meta['arc_spatial_reference'] = None
        meta['spatial_reference_id'] = None
    return meta


def field_as_metadata(field_object):
    """Return dictionary of metadata from an ArcPy field object."""
    meta = {
        'name': getattr(field_object, 'name'),
        'alias_name': getattr(field_object, 'aliasName'),
        'base_name': getattr(field_object, 'baseName'),
        'type': getattr(field_object, 'type').lower(),
        'length': getattr(field_object, 'length'),
        'precision': getattr(field_object, 'precision'),
        'scale': getattr(field_object, 'scale'),
        }
    return meta


def insert_features_from_dicts(dataset_path, insert_features, field_names):
    """Insert features from a collection of dictionaries.

    Args:
        dataset_path (str): Path of dataset.
        insert_features (iter): Iterable containing dictionaries representing
            features.
        field_names (iter): Iterable of field names to insert.
    Returns:
        str.
    """
    LOG.info("Start: Insert features from dictionaries into %s.", dataset_path)
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    # Regenerate as iters.
    insert_features = (
        [feat[name] if name in feat else None for name in field_names]
        for feat in insert_features
        )
    with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
        for row in insert_features:
            cursor.insertRow(row)
    LOG.info("End: Insert.")
    return dataset_path


def insert_features_from_iters(dataset_path, insert_features, field_names):
    """Insert features from a collection of iterables.

    Args:
        dataset_path (str): Path of dataset.
        insert_features (iter): Iterable containing iterables representing
            features.
        field_names (iter): Iterable of field names to insert.
    Returns:
        str.
    """
    LOG.info("Start: Insert features from iterables into %s.", dataset_path)
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    with arcpy.da.InsertCursor(dataset_path, field_names) as cursor:
        for row in insert_features:
            cursor.insertRow(row)
    LOG.info("End: Insert.")
    return dataset_path


def parameter_changed(parameter):
    """Return True if parameter is in a pre-validation changed state."""
    return all([parameter.altered, not parameter.hasBeenValidated])


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
        return handle_value_object(parameter.value)
    # Multivalue parameters place their values in .values (.value. holds a
    # ValueTable object).
    else:
        return [handle_value_object(value) for value in parameter.values]


def parameter_value_map(parameters):
    """Create value map from ArcPy parameter objects."""
    return {parameter.name: parameter_value(parameter)
            for parameter in parameters}


def spatial_reference_as_arc(spatial_reference):
    """Return ArcPy spatial reference object from a Python reference.

    Args:
        spatial_reference (int): Spatial reference ID.
                          (str): Path of reference dataset/file.
                          (arcpy.Geometry): Reference geometry object.
    Returns:
        arcpy.SpatialReference.
    """
    if spatial_reference is None:
        arc_object = None
    elif isinstance(spatial_reference, int):
        arc_object = arcpy.SpatialReference(spatial_reference)
    elif isinstance(spatial_reference, arcpy.Geometry):
        arc_object = getattr(spatial_reference, 'spatialReference')
    else:
        arc_object = getattr(arcpy.Describe(spatial_reference),
                             'spatialReference')
    return arc_object


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
