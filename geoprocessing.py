"""Mostly self-contained functions for geoprocessing in Python toolboxes."""
from collections import Counter
import logging
import arcpy

from utils import (
    leveled_logger,
    unique_name,
)


LOG = logging.getLogger(__name__)


class DatasetView(object):
    """Context manager for an ArcGIS dataset view (feature layer/table view).

    Requires:
        dataset_metadata

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
