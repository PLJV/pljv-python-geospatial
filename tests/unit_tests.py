import unittest

from copy import copy, deepcopy

# GeoJSON test string randomly pulled out of a browser
_GEOJSON_TEST_STR: str = '"{"type":"FeatureCollection","features":[{"type":"Feature",' '"geometry":{"type":"Polygon","coordinates":[[[-98.55578125,39.' "006979909158126],[-98.61345947265625,38.910871918919376],[-98.58" "599365234375,38.87239223120202],[-98.4843701171875,38.874530538501" "47],[-98.38,38.9001852078871],[-98.43767822265625,38.95146674797671]" ",[-98.5502880859375,38.88949688823828],[-98.53380859375,38.9685523" '560863]]]},"properties":{"fid":0}}]}"'


class TestVectorBaseMethods(unittest.TestCase):
    def test_empty_base_class(self):
        try:
            from beatbox import Vector
        except ImportError:
            self.assertRaises(ImportError)
        try:
            self.vector = Vector()
        except Exception:
            self.assertRaises(
                BaseException("Unable to generate an empty Vector base class")
            )

    def test_copy(self):
        _copy = copy(self.vector)

    def test_deep_copy(self):
        _copy = deepcopy(self.vector)

    def test_read_geojson_string(self):
        try:
            from beatbox import Vector
        except ImportError:
            self.assertRaises(ImportError)
        # args[0] / string=
        _test = Vector(_GEOJSON_TEST_STR)
        _test = Vector(string=_GEOJSON_TEST_STR)

    def test_read_shapefile(self):
        pass

    def test_read_geojson_file(self):
        pass

    def test_iter(self):
        pass

    def test_write_shapefile(self):
        pass

    def test_write_geojson_file(self):
        pass

    def test_external_geometries_assignment(self):
        pass

    def test_external_crs_assignment(self):
        pass

    def test_to_geopandasdataframe(self):
        pass

    def test_to_shapely_geometries(self):
        pass

    def test_to_geojson_string(self):
        pass

    def test_to_ee_feature_collection(self):
        pass


if __name__ == "__main__":
    unittest.main()
