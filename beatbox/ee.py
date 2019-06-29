# logging
import logging
logging.basicConfig(level=logging.WARNING)
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