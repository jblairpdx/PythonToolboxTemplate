"""Self-contained functions for geoprocessing in Python toolboxes."""
import random
import string

import arcpy


def feature_count(dataset_path, **kwargs):
    """Return number of features in dataset.

    Args:
        dataset_path (str): Path of the dataset.
        **kwargs: Arbitrary keyword arguments. See below.

   Keyword Args:
        dataset_where_sql (str): SQL where-clause for dataset subselection.

    Returns:
        int: Number of features counted.
    """
    kwargs.setdefault('dataset_where_sql')
    view_name = 'view_' + ''.join(random.choice(string.ascii_letters)
                                  for _ in range(4))
    arcpy.management.MakeTableView(dataset_path, view_name,
                                   kwargs['dataset_where_sql'])
    return int(arcpy.management.GetCount(view_name).getOutput(0))
