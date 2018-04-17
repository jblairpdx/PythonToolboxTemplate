"""Mostly self-contained functions for geoprocessing in Python toolboxes."""
from collections import Counter, defaultdict
import copy
import datetime
import inspect
from itertools import chain
import logging
import uuid

import arcpy

from utils import (
    contain,
    leveled_logger,
    unique_ids,
    unique_name,
)


LOG = logging.getLogger(__name__)


class DatasetView(object):
    """Context manager for an ArcGIS dataset view (feature layer/table view).

    Attributes:
        name (str): Name of the view.
        dataset_path (str): Path of the dataset.
        dataset_meta (dict): Metadata dictionary for the dataset.
        is_spatial (bool): Flag indicating if the view is spatial.

    """

    def __init__(self, dataset_path, dataset_where_sql=None, view_name=None,
                 force_nonspatial=False):
        """Initialize instance.

        Args:
            dataset_path (str): Path of the dataset.
            dataset_where_sql (str): SQL where-clause for dataset
                subselection.
            view_name (str): Name of the view to create.
            force_nonspatial (bool): Flag that forces a nonspatial view.

        """
        self.name = view_name if view_name else unique_name('view')
        self.dataset_path = dataset_path
        self.dataset_meta = dataset_metadata(dataset_path)
        self.is_spatial = all((self.dataset_meta['is_spatial'], not force_nonspatial))
        self._where_sql = dataset_where_sql

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.discard()

    @property
    def count(self):
        """int: Number of features in the view."""
        return int(arcpy.management.GetCount(self.name).getOutput(0))

    @property
    def exists(self):
        """bool: Flag indicating the view currently exists."""
        return arcpy.Exists(self.name)

    @property
    def where_sql(self):
        """str: SQL where-clause property of dataset view subselection.

        Setting this property will change the view's dataset subselection.

        """
        return self._where_sql

    @where_sql.setter
    def where_sql(self, value):
        if self.exists:
            arcpy.management.SelectLayerByAttribute(
                in_layer_or_view=self.name, selection_type='new_selection',
                where_clause=value
                )
        self._where_sql = value

    @where_sql.deleter
    def where_sql(self):
        if self.exists:
            arcpy.management.SelectLayerByAttribute(in_layer_or_view=self.name,
                                                    selection_type='clear_selection')
        self._where_sql = None

    def as_chunks(self, chunk_size):
        """Generate 'chunks' of the view's data as new DatasetView.

        Yields DatasetView with context management, i.e. view will be discarded
        when generator moves to next chunk-view.

        Args:
            chunk_size (int): Number of features in each chunk-view.

        Yields:
            DatasetView.

        """
        # ArcPy where clauses cannot use 'between'.
        chunk_where_sql_template = ("{oid_field_name} >= {from_oid}"
                                    " and {oid_field_name} <= {to_oid}")
        if self.where_sql:
            chunk_where_sql_template += " and ({})".format(self.where_sql)
        # Get iterable of all object IDs in dataset.
        with arcpy.da.SearchCursor(in_table=self.dataset_path,
                                   field_names=('oid@',),
                                   where_clause=self.where_sql) as cursor:
            # Sorting is important: allows selection by ID range.
            oids = sorted(oid for oid, in cursor)
        while oids:
            chunk_where_sql = chunk_where_sql_template.format(
                oid_field_name=self.dataset_meta['oid_field_name'],
                from_oid=min(oids), to_oid=max(oids[:chunk_size])
                )
            with DatasetView(self.name, chunk_where_sql) as chunk_view:
                yield chunk_view
            # Remove chunk from set.
            oids = oids[chunk_size:]

    def create(self):
        """Create view."""
        function = (arcpy.management.MakeFeatureLayer if self.is_spatial
                    else arcpy.management.MakeTableView)
        function(self.dataset_path, self.name, where_clause=self.where_sql,
                 workspace=self.dataset_meta['workspace_path'])
        return self.exists

    def discard(self):
        """Discard view."""
        if self.exists:
            arcpy.management.Delete(self.name)
        return not self.exists


class Editor(object):
    """Context manager for editing features.

    Attributes:
        workspace_path (str):  Path for the editing workspace

    """

    def __init__(self, workspace_path, use_edit_session=True):
        """Initialize instance.

        Args:
            workspace_path (str): Path for the editing workspace.
            use_edit_session (bool): Flag directing edits to be made in an
                edit session. Default is True.

        """
        self._editor = (arcpy.da.Editor(workspace_path) if use_edit_session else None)
        self.workspace_path = workspace_path

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop(save_changes=False if exception_type else True)

    @property
    def active(self):
        """bool: Flag indicating whether edit session is active."""
        if self._editor:
            _active = self._editor.isEditing
        else:
            _active = False
        return _active

    def start(self):
        """Start an active edit session.

        Returns:
            bool: Indicator that session is active.

        """
        if self._editor and not self._editor.isEditing:
            self._editor.startEditing(with_undo=True, multiuser_mode=True)
            self._editor.startOperation()
        return self.active

    def stop(self, save_changes=True):
        """Stop an active edit session.

        Args:
            save_changes (bool): Flag indicating whether edits should be
                saved.

        Returns:
            bool: Indicator that session is not active.

        """
        if self._editor and self._editor.isEditing:
            if save_changes:
                self._editor.stopOperation()
            else:
                self._editor.abortOperation()
            self._editor.stopEditing(save_changes)
        return not self.active


def _field_object_metadata(field_object):
    """Return dictionary of metadata from ArcPy field object."""
    meta = {
        'arc_object': field_object,
        'name': getattr(field_object, 'name'),
        'alias_name': getattr(field_object, 'aliasName'),
        'base_name': getattr(field_object, 'baseName'),
        'type': getattr(field_object, 'type').lower(),
        'length': getattr(field_object, 'length'),
        'precision': getattr(field_object, 'precision'),
        'scale': getattr(field_object, 'scale'),
    }
    return meta


def attributes_as_dicts(dataset_path, field_names=None, **kwargs):
    """Generator for dictionaries of feature attributes.

    Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collection of field names. Names will be the keys in the
            dictionary mapping to their values. If value is None, all attributes fields
            will be used.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the output geometry's spatial
            reference will be derived.

    Yields:
        dict: Mapping of feature attribute field names to values.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('spatial_reference_item')
    if field_names is None:
        meta = {'dataset': dataset_metadata(dataset_path)}
        keys = {'field': tuple(key.lower() for key
                               in meta['dataset']['field_names_tokenized'])}
    else:
        keys = {'field': tuple(contain(field_names))}
    sref = spatial_reference(kwargs['spatial_reference_item'])
    cursor = arcpy.da.SearchCursor(in_table=dataset_path, field_names=keys,
                                   where_clause=kwargs['dataset_where_sql'],
                                   spatial_reference=sref)
    with cursor:
        for feature in cursor:
            yield dict(zip(cursor.fields, feature))


def attributes_as_iters(dataset_path, field_names, **kwargs):
    """Generator for iterables of feature attributes.

    Use ArcPy cursor token names for object IDs and geometry objects/properties.

    Args:
        dataset_path (str): Path of the dataset.
        field_names (iter): Collection of field names. The order of the names in the
            collection will determine where its value will fall in the yielded item.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the output geometry's spatial
            reference will be derived.
        iter_type: Iterable type to yield. Default is tuple.

    Yields:
        iter: Collection of attribute values.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('spatial_reference_item')
    kwargs.setdefault('iter_type', tuple)
    keys = {'field': tuple(contain(field_names))}
    sref = spatial_reference(kwargs['spatial_reference_item'])
    cursor = arcpy.da.SearchCursor(in_table=dataset_path, field_names=keys['field'],
                                   where_clause=kwargs['dataset_where_sql'],
                                   spatial_reference=sref)
    with cursor:
        for feature in cursor:
            yield kwargs['iter_type'](feature)


def coordinate_node_map(dataset_path, from_id_field_name, to_id_field_name,
                        id_field_name='oid@', **kwargs):
    """Return dictionary mapping of coordinates to node-info dictionaries.

    Note:
        From & to IDs must be the same attribute type.

    Args:
        dataset_path (str): Path of the dataset.
        from_id_field_name (str): Name of the from-ID field.
        to_id_field_name (str): Name of the to-ID field.
        id_field_name (str): Name of the ID field. Default is 'oid@'.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        update_nodes (bool): Flag to indicate whether to update nodes based on feature
            geometries. Default is False.

    Returns:
        dict: Mapping of coordinate tuples to node-info dictionaries.
            {(x, y): {'node_id': <id>, 'ids': {'from': set(), 'to': set()}}}

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('update_nodes', False)
    def _node_feature_count(node):
        """Return feature count for node from info map."""
        return len(node['ids']['from'].union(node['ids']['to']))
    def _update_coord_node_map(coord_node, node_id_metadata):
        """Return updated coordinate node info map."""
        coord_node = copy.deepcopy(coord_node)
        used_ids = {node['node_id'] for node in coord_node.values()
                    if node['node_id'] is not None}
        unused_ids = (
            _id for _id in unique_ids(python_type(node_id_metadata['type']),
                                      node_id_metadata['length'])
            if _id not in used_ids
        )
        id_coords = {}
        for coord, node in coord_node.items():
            # Assign IDs where missing.
            if node['node_id'] is None:
                node['node_id'] = next(unused_ids)
            # If ID duplicate, re-ID node with least features.
            elif node['node_id'] in id_coords:
                other_coord = id_coords[node['node_id']]
                other_node = coord_node[other_coord]
                new_node_id = next(unused_ids)
                if _node_feature_count(node) > _node_feature_count(other_node):
                    other_node['node_id'] = new_node_id  # Does update coord_node!
                    id_coords[new_node_id] = id_coords.pop(node['node_id'])
                else:
                    node['node_id'] = new_node_id  # Does update coord_node!
            id_coords[node['node_id']] = coord
        return coord_node
    keys = {'field': (id_field_name, from_id_field_name, to_id_field_name, 'shape@')}
    coord_node = {}
    g_features = attributes_as_iters(
        dataset_path, keys['field'], dataset_where_sql=kwargs['dataset_where_sql'],
    )
    for feature_id, from_node_id, to_node_id, geom in g_features:
        for end, node_id, point in [('from', from_node_id, geom.firstPoint),
                                    ('to', to_node_id, geom.lastPoint)]:
            coord = (point.X, point.Y)
            if coord not in coord_node:
                # Create new coordinate-node.
                coord_node[coord] = {'node_id': node_id, 'ids': defaultdict(set)}
            coord_node[coord]['node_id'] = (
                # Assign new ID if current is missing.
                node_id if coord_node[coord]['node_id'] is None
                # Assign new ID if lower than current.
                else min(coord_node[coord]['node_id'], node_id)
            )
            # Add feature ID to end-ID set.
            coord_node[coord]['ids'][end].add(feature_id)
    if kwargs['update_nodes']:
        field_meta = {'node_id': field_metadata(dataset_path, from_id_field_name)}
        coord_node = _update_coord_node_map(coord_node, field_meta['node_id'])
    return coord_node


def dataset_feature_count(dataset_path, **kwargs):
    """Return number of features in dataset.

    Requires:
        DatasetView

    Args:
        dataset_path (str): Path of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

   Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.

    Returns:
        int: Number of features counted.

    """
    view = DatasetView(dataset_path, **kwargs)
    with view:
        return view.count


def dataset_metadata(dataset_path):
    """Return dictionary of dataset metadata.

    Requires:
        _field_object_metadata

    Args:
        dataset_path (str): Path of the dataset.

    Returns:
        dict: Metadata for dataset.

    """
    arc_object = arcpy.Describe(dataset_path)
    meta = {
        'arc_object': arc_object,
        'name': getattr(arc_object, 'name'),
        'path': getattr(arc_object, 'catalogPath'),
        'data_type': getattr(arc_object, 'dataType'),
        'workspace_path': getattr(arc_object, 'path'),
        # Do not use getattr! Tables sometimes don't have OIDs.
        'is_table': hasattr(arc_object, 'hasOID'),
        'is_versioned': getattr(arc_object, 'isVersioned', False),
        'oid_field_name': getattr(arc_object, 'OIDFieldName', None),
        'is_spatial': hasattr(arc_object, 'shapeType'),
        'geometry_type': getattr(arc_object, 'shapeType', None),
        'geom_type': getattr(arc_object, 'shapeType', None),
        'geometry_field_name': getattr(arc_object, 'shapeFieldName', None),
        'geom_field_name': getattr(arc_object, 'shapeFieldName', None),
    }
    meta['field_token'] = {}
    if meta['oid_field_name']:
        meta['field_token'][meta['oid_field_name']] = 'oid@'
    if meta['geom_field_name']:
        meta['field_token'].update({
            meta['geom_field_name']: 'shape@',
            meta['geom_field_name'] + '_Area': 'shape@area',
            meta['geom_field_name'] + '_Length': 'shape@length',
            meta['geom_field_name'] + '.STArea()': 'shape@area',
            meta['geom_field_name'] + '.STLength()': 'shape@length',
        })
    meta['field_names'] = tuple(field.name for field
                                in getattr(arc_object, 'fields', ()))
    meta['field_names_tokenized'] = tuple(meta['field_token'].get(name, name)
                                          for name in meta['field_names'])
    meta['fields'] = tuple(_field_object_metadata(field) for field
                           in getattr(arc_object, 'fields', ()))
    meta['user_field_names'] = tuple(
        name for name in meta['field_names']
        if name != meta['oid_field_name']
        and '{}.'.format(meta['geometry_field_name']) not in name
    )
    meta['user_fields'] = tuple(
        field for field in meta['fields']
        if field['name'] != meta['oid_field_name']
        and '{}.'.format(meta['geometry_field_name']) not in field['name']
    )
    if hasattr(arc_object, 'spatialReference'):
        meta['spatial_reference'] = getattr(arc_object, 'spatialReference')
        meta['spatial_reference_id'] = getattr(meta['spatial_reference'], 'factoryCode')
    else:
        meta['spatial_reference'] = None
        meta['spatial_reference_id'] = None
    return meta


def features_delete(dataset_path, **kwargs):
    """Delete features in the dataset.

    Requires:
        DatasetView
        Editor
        dataset_feature_count
        dataset_metadata

    Args:
        dataset_path (str): Path of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    if kwargs['dataset_where_sql']:
        log("Start: Delete features from %s where `%s`.",
            dataset_path, kwargs['dataset_where_sql'])
    else:
        log("Start: Delete features from %s.", dataset_path)
    meta = {'dataset': dataset_metadata(dataset_path)}
    truncate_error_codes = (
        # "Only supports Geodatabase tables and feature classes."
        'ERROR 000187',
        # "Operation not supported on a versioned table."
        'ERROR 001259',
        # "Operation not supported on table {table name}."
        'ERROR 001260',
        # Operation not supported on a feature class in a controller dataset.
        'ERROR 001395',
    )
    # Can use (faster) truncate when no sub-selection or edit session.
    run_truncate = (kwargs['dataset_where_sql'] is None
                    and kwargs['use_edit_session'] is False)
    feature_count = Counter()
    if run_truncate:
        feature_count['deleted'] = dataset_feature_count(dataset_path)
        feature_count['unchanged'] = 0
        try:
            arcpy.management.TruncateTable(in_table=dataset_path)
        except arcpy.ExecuteError:
            # Avoid arcpy.GetReturnCode(); error code position inconsistent.
            # Search messages for 'ERROR ######' instead.
            if any(code in arcpy.GetMessages()
                   for code in truncate_error_codes):
                LOG.debug("Truncate unsupported; will try deleting rows.")
                run_truncate = False
            else:
                raise
    if not run_truncate:
        view = {'dataset': DatasetView(dataset_path, kwargs['dataset_where_sql'])}
        session = Editor(meta['dataset']['workspace_path'], kwargs['use_edit_session'])
        with view['dataset'], session:
            feature_count['deleted'] = view['dataset'].count
            arcpy.management.DeleteRows(in_rows=view['dataset'].name)
        feature_count['unchanged'] = dataset_feature_count(dataset_path)
    for key in ('deleted', 'unchanged'):
        log("%s features %s.", feature_count[key], key)
    log("End: Delete.")
    return feature_count


def field_metadata(dataset_path, field_name):
    """Return dictionary of field metadata.

    Note:
        Field name is case-insensitive.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.

    Returns:
        dict: Metadata for field.

    """
    try:
        field_object = arcpy.ListFields(dataset=dataset_path, wild_card=field_name)[0]
    except IndexError:
        raise AttributeError("Field {} not present on {}".format(field_name,
                                                                 dataset_path))
    meta = _field_object_metadata(field_object)
    return meta


def id_attributes_map(dataset_path, id_field_names, field_names, **kwargs):
    """Return dictionary mapping of field attribute for each feature ID.

    Note:
        There is no guarantee that the ID field(s) are unique.
        Use ArcPy cursor token names for object IDs and geometry objects/
        properties.

    Args:
        dataset_path (str): Path of the dataset.
        id_field_names (iter, str): Name(s) of the ID field(s).
        field_names (iter, str): Name(s) of the field(s).
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        spatial_reference_item: Item from which the output geometry's spatial
            reference will be derived.

    Returns:
        dict: Mapping of feature ID to feature attribute(s).

    """
    field_names = tuple(contain(field_names))
    id_field_names = tuple(contain(id_field_names))
    sref = spatial_reference(kwargs.get('spatial_reference_item'))
    cursor = arcpy.da.SearchCursor(dataset_path,
                                   field_names=id_field_names + field_names,
                                   where_clause=kwargs.get('dataset_where_sql'),
                                   spatial_reference=sref)
    with cursor:
        result = {}
        for row in cursor:
            map_id = row[:len(id_field_names)]
            map_value = row[len(id_field_names):]
            if len(id_field_names) == 1:
                map_id = map_id[0]
            if len(field_names) == 1:
                map_value = map_value[0]
            result[map_id] = map_value
    return result


def id_node_map(dataset_path, from_id_field_name, to_id_field_name,
                id_field_name='oid@', **kwargs):
    """Return dictionary mapping of feature ID to from- & to-node IDs.

    From & to IDs must be the same attribute type.

    Args:
        dataset_path (str): Path of the dataset.
        from_id_field_name (str): Name of the from-ID field.
        to_id_field_name (str): Name of the to-ID field.
        id_field_name (str): Name of the ID field. Default is 'oid@'.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        field_names_as_keys (bool): Flag to indicate use of dataset's node ID field
            names as the ID field names in the map. Default is False.
        update_nodes (bool): Flag to indicate whether to update the nodes based on the
            feature geometries. Default is False.

    Returns:
        dict: Mapping of feature IDs to node-end ID dictionaries.
            `{feature_id: {'from': from_node_id, 'to': to_node_id}}`

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('field_names_as_keys', False)
    kwargs.setdefault('update_nodes', False)
    field_meta = {'from': field_metadata(dataset_path, from_id_field_name),
                  'to': field_metadata(dataset_path, to_id_field_name)}
    if field_meta['from']['type'] != field_meta['to']['type']:
        raise ValueError("Fields %s & %s must be of same type.")
    key = {'id': id_field_name, 'from': from_id_field_name, 'to': to_id_field_name}
    end_key = {'from': from_id_field_name if kwargs['field_names_as_keys'] else 'from',
               'to': to_id_field_name if kwargs['field_names_as_keys'] else 'to'}
    id_nodes = defaultdict(dict)
    if kwargs['update_nodes']:
        coord_node_info = coordinate_node_map(dataset_path, from_id_field_name,
                                              to_id_field_name, id_field_name, **kwargs)
        for node in coord_node_info.values():
            for end, key in end_key.items():
                for feat_id in node['ids'][end]:
                    id_nodes[feat_id][key] = node['node_id']
    # If not updating nodes, don't need to bother with geometry/coordinates.
    else:
        g_id_nodes = attributes_as_iters(
            dataset_path, field_names=(key['id'], from_id_field_name, to_id_field_name),
            dataset_where_sql=kwargs['dataset_where_sql'],
        )
        for feat_id, from_node_id, to_node_id in g_id_nodes:
            id_nodes[feat_id][end_key['from']] = from_node_id
            id_nodes[feat_id][end_key['to']] = to_node_id
    return id_nodes


def insert_features_from_dicts(dataset_path, insert_features, field_names, **kwargs):
    """Insert features into dataset from dictionaries.

    Args:
        dataset_path (str): Path of the dataset.
        insert_features (iter of dict): Collection of dictionaries
            representing features.
        field_names (iter): Collection of field names/keys to insert.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Insert features into %s from dictionaries.", dataset_path)
    keys = {'row': tuple(contain(field_names))}
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    iters = ((feature[key] for key in keys['row']) for feature in insert_features)
    feature_count = insert_features_from_iters(
        dataset_path, iters, field_names,
        use_edit_session=kwargs['use_edit_session'], log_level=None,
    )
    log("%s features inserted.", feature_count['inserted'])
    log("End: Insert.")
    return feature_count


def insert_features_from_iters(dataset_path, insert_features, field_names, **kwargs):
    """Insert features into dataset from iterables.

    Args:
        dataset_path (str): Path of the dataset.
        insert_features (iter of iter): Collection of iterables representing
            features.
        field_names (iter): Collection of field names to insert. These must
            match the order of their attributes in the insert_features items.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Insert features into %s from iterables.", dataset_path)
    meta = {'dataset': dataset_metadata(dataset_path)}
    keys = {'row': tuple(contain(field_names))}
    if inspect.isgeneratorfunction(insert_features):
        insert_features = insert_features()
    session = Editor(meta['dataset']['workspace_path'], kwargs['use_edit_session'])
    cursor = arcpy.da.InsertCursor(dataset_path, field_names=keys['row'])
    feature_count = Counter()
    with session, cursor:
        for row in insert_features:
            cursor.insertRow(tuple(row))
            feature_count['inserted'] += 1
    log("%s features inserted.", feature_count['inserted'])
    log("End: Insert.")
    return feature_count


def insert_features_from_path(dataset_path, insert_dataset_path, field_names=None,
                              **kwargs):
    """Insert features into dataset from another dataset.

    Args:
        dataset_path (str): Path of the dataset.
        insert_dataset_path (str): Path of dataset to insert features from.
        field_names (iter): Collection of field names to insert. Listed field must be
            present in both datasets. If field_names is None, all fields will be
            inserted.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        insert_where_sql (str): SQL where-clause for insert-dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Path of the dataset updated.

    """
    kwargs.setdefault('insert_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Insert features into %s from %s.", dataset_path, insert_dataset_path)
    meta = {'dataset': dataset_metadata(dataset_path),
            'insert': dataset_metadata(insert_dataset_path)}
    if field_names is None:
        keys = set.intersection(
            *(set(name.lower() for name in _meta['field_names_tokenized'])
              for _meta in meta.values())
        )
    else:
        keys = set(name.lower() for name in contain(field_names))
    # OIDs & area/length "fields" have no business being part of an update.
    # Geometry itself is handled separately in append function.
    for _meta in meta.values():
        for key in chain(*_meta['field_token'].items()):
            keys.discard(key)
    append_kwargs = {'inputs': unique_name('view'), 'target': dataset_path,
                     'schema_type': 'no_test', 'field_mapping': arcpy.FieldMappings()}
    # Create field maps.
    # ArcGIS Pro's no-test append is case-sensitive (verified 1.0-1.1.1).
    # Avoid this problem by using field mapping.
    # BUG-000090970 - ArcGIS Pro 'No test' field mapping in Append tool does
    # not auto-map to the same field name if naming convention differs.
    for key in keys:
        field_map = arcpy.FieldMap()
        field_map.addInputField(insert_dataset_path, key)
        append_kwargs['field_mapping'].addFieldMap(field_map)
    view = DatasetView(insert_dataset_path, kwargs['insert_where_sql'],
                       view_name=append_kwargs['inputs'],
                       # Must be nonspatial to append to nonspatial table.
                       force_nonspatial=(not meta['dataset']['is_spatial']))
    session = Editor(meta['dataset']['workspace_path'], kwargs['use_edit_session'])
    with view, session:
        arcpy.management.Append(**append_kwargs)
        feature_count = Counter({'inserted': view.count})
    log("%s features inserted.", feature_count['inserted'])
    log("End: Insert.")
    return feature_count


def python_type(type_description):
    """Return object representing the Python type.

    Args:
        type_description (str): Arc-style type description/code.

    Returns:
        Python object representing the type.

    """
    instance = {
        'date': datetime.datetime,
        'double': float, 'single': float,
        'integer': int, 'long': int, 'short': int, 'smallinteger': int,
        'geometry': arcpy.Geometry,
        'guid': uuid.UUID,
        'string': str, 'text': str,
    }
    return instance[type_description.lower()]


def spatial_reference(item):
    """Return ArcPy spatial reference object from a Python reference.

    Args:
        item (int): Spatial reference ID.
             (str): Path of reference dataset/file.
             (arcpy.Geometry): Reference geometry object.
             (arcpy.SpatialReference): Spatial reference object.

    Returns:
        arcpy.SpatialReference.

    """
    if item is None:
        arc_object = None
    elif isinstance(item, arcpy.SpatialReference):
        arc_object = item
    elif isinstance(item, int):
        arc_object = arcpy.SpatialReference(item)
    elif isinstance(item, arcpy.Geometry):
        arc_object = getattr(item, 'spatialReference')
    else:
        arc_object = arcpy.SpatialReference(
            getattr(getattr(arcpy.Describe(item), 'spatialReference'), 'factoryCode')
        )
    return arc_object


def update_attributes_by_function(dataset_path, field_name, function, **kwargs):
    """Update attribute values by passing them to a function.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        function (types.FunctionType): Function to get values from.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        arg_field_names (iter): Iterable of the field names whose values will
            be the method arguments (not including the primary field).
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        field_as_first_arg (bool): Flag to indicate the field value will be
            the first argument for the method. Defaults to True.
        kwarg_field_names (iter): Iterable of the field names whose names &
            values will be the method keyword arguments.
        log_level (str): Level to log the function at. Defaults to 'info'.
        use_edit_session (bool): Flag to perform updates in an edit session.
            Default is False.

    Returns:
        str: Name of the field updated.

    """
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by function %s.",
        field_name, dataset_path, function)
    field_names = {'args': tuple(kwargs.get('arg_field_names', ())),
                   'kwargs': tuple(kwargs.get('kwarg_field_names', ()))}
    field_names['row'] = ((field_name,) + field_names['args'] + field_names['kwargs'])
    args_idx = len(field_names['args']) + 1
    session = Editor(dataset_metadata(dataset_path)['workspace_path'],
                     kwargs.get('use_edit_session', False))
    cursor = arcpy.da.UpdateCursor(dataset_path, field_names['row'],
                                   kwargs.get('dataset_where_sql'))
    with session, cursor:
        for row in cursor:
            func_args = (row[0:args_idx] if kwargs.get('field_as_first_arg', True)
                         else row[1:args_idx])
            func_kwargs = dict(zip(field_names['kwargs'], row[args_idx:]))
            new_value = function(*func_args, **func_kwargs)
            if row[0] != new_value:
                try:
                    cursor.updateRow([new_value] + row[1:])
                except RuntimeError:
                    LOG.error("Offending value is %s", new_value)
                    raise RuntimeError
    log("End: Update.")
    return field_name


def update_attributes_by_mapping(dataset_path, field_name, mapping, key_field_names,
                                 **kwargs):
    """Update attribute values by finding them in a mapping.

    Note:
        Wraps update_by_function.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        mapping (object): Mapping to get values from.
        key_field_names (iter): Name of the fields whose values will be the mapping's
            keys.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        default_value: Value to return from mapping if key value on feature not
            present. Defaults to None.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        str: Name of the field updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('default_value')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by mapping with key(s) in %s.",
        field_name, dataset_path, key_field_names)
    keys = tuple(contain(key_field_names))
    session = Editor(dataset_metadata(dataset_path)['workspace_path'],
                     kwargs['use_edit_session'])
    cursor = arcpy.da.UpdateCursor(dataset_path, (field_name,)+keys,
                                   kwargs['dataset_where_sql'])
    with session, cursor:
        for row in cursor:
            old_value = row[0]
            key = row[1] if len(keys) == 1 else tuple(row[1:])
            new_value = mapping.get(key, kwargs['default_value'])
            if old_value != new_value:
                try:
                    cursor.updateRow([new_value] + row[1:])
                except RuntimeError:
                    LOG.error("Offending value is %s", new_value)
                    raise RuntimeError
    log("End: Update.")
    return field_name


def update_attributes_by_node_ids(dataset_path, from_id_field_name, to_id_field_name,
                                  **kwargs):
    """Update attribute values by passing them to a function.

    Args:
        dataset_path (str): Path of the dataset.
        from_id_field_name (str): Name of the from-ID field.
        to_id_field_name (str): Name of the to-ID field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Defaults to 'info'.

    Returns:
        tuple: Names (str) of the fields updated.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', False)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by node IDs.",
        (from_id_field_name, to_id_field_name), dataset_path)
    oid_nodes = id_node_map(dataset_path, from_id_field_name, to_id_field_name,
                            field_names_as_keys=True, update_nodes=True)
    session = Editor(dataset_metadata(dataset_path)['workspace_path'],
                     kwargs['use_edit_session'])
    cursor = arcpy.da.UpdateCursor(
        dataset_path, field_names=('oid@', from_id_field_name, to_id_field_name),
        where_clause=kwargs['dataset_where_sql'],
    )
    with session, cursor:
        for row in cursor:
            oid = row[0]
            new_row = (oid, oid_nodes[oid][from_id_field_name],
                       oid_nodes[oid][to_id_field_name])
            if row != new_row:
                try:
                    cursor.updateRow(new_row)
                except RuntimeError:
                    LOG.error("Offending values one of %s, %s", new_row[1], new_row[2])
                    raise RuntimeError
    log("End: Update.")
    return (from_id_field_name, to_id_field_name)


def update_attributes_by_unique_id(dataset_path, field_name, **kwargs):
    """Update attribute values by assigning a unique ID.

    Existing IDs are preserved, if unique.

    Args:
        dataset_path (str): Path of the dataset.
        field_name (str): Name of the field.
        **kwargs: Arbitrary keyword arguments. See below.

    Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.
        use_edit_session (bool): Flag to perform updates in an edit session. Default is
            False.
        log_level (str): Level to log the function at. Default is 'info'.

    Returns:
        dict: Mapping of new IDs to existing old IDs.

    """
    kwargs.setdefault('dataset_where_sql')
    kwargs.setdefault('use_edit_session', True)
    log = leveled_logger(LOG, kwargs.get('log_level', 'info'))
    log("Start: Update attributes in %s on %s by assigning unique IDs.",
        field_name, dataset_path)
    meta = {'field': field_metadata(dataset_path, field_name)}
    def _corrected_id(current_id, unique_id_pool, used_ids, ignore_nonetype=False):
        """Return corrected ID to ensure uniqueness."""
        if any((ignore_nonetype and current_id is None, current_id not in used_ids)):
            corrected_id = current_id
        else:
            corrected_id = next(unique_id_pool)
            while corrected_id in used_ids:
                corrected_id = next(unique_id_pool)
        return corrected_id
    unique_id_pool = unique_ids(data_type=python_type(meta['field']['type']),
                                string_length=meta['field'].get('length', 16))
    oid_id = id_attributes_map(dataset_path, id_field_names='oid@',
                               field_names=field_name)
    used_ids = set()
    new_old_id = {}
    # Ensure current IDs are unique.
    for oid, current_id in oid_id.items():
        _id = _corrected_id(current_id, unique_id_pool, used_ids, ignore_nonetype=True)
        if _id != current_id:
            new_old_id[_id] = oid_id[oid]
            oid_id[oid] = _id
        used_ids.add(_id)
    # Take care of unassigned IDs now that we know all the used IDs.
    for oid, current_id in oid_id.items():
        if current_id is None:
            _id = _corrected_id(current_id, unique_id_pool, used_ids,
                                ignore_nonetype=False)
        oid_id[oid] = _id
        used_ids.add(_id)
    update_attributes_by_mapping(dataset_path, field_name,
                                 mapping=oid_id, key_field_names='oid@',
                                 dataset_where_sql=kwargs.get('dataset_where_sql'),
                                 use_edit_session=kwargs.get('use_edit_session', False),
                                 log_level=None)
    log("End: Update.")
    return new_old_id
