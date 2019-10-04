# logging
import logging

logger = logging.getLogger(__name__)

# Fickle beast handlers for Earth Engine
try:
    import ee
    ee.Initialize()
    _HAVE_EE = True
except Exception:
    _HAVE_EE = False
    logger.warning("Failed to load the Earth Engine API. "
                   "Check your installation. Will continue "
                   "to load but without the EE functionality.")

from gee_asset_manager.batch_remover import delete
from gee_asset_manager.batch_uploader import upload
from gee_asset_manager.config import setup_logging

def ee_ingest(*args, **kwargs):
    """punt"""
    upload(user=kwargs.user,
           source_path=kwargs.get('filename',None),
           destination_path=kwargs.get('asset_id', None),
            metadata_path=None,
            multipart_upload=None,
            nodata_value=None,
            bucket_name=None,
            band_names=None)

class EeAsset(object):
    pass


class EeImageCollection(object):
    pass


def _ee_extract(*args):
    """ EE raster extraction handler
    :param str asset:
    :param str feature_collection
    :return:
    """
    if not _HAVE_EE:
        raise AttributeError("Requested Earth Engine functionality, "
                             "but we failed to load and initialize the ee"
                             "package.")

def _ee_rebuild_crs(*args):
    raise NotImplementedError
