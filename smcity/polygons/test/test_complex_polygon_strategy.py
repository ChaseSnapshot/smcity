''' Unit tests for the ComplexPolygonStrategy class. '''

from smcity.models.test.mock_data import MockData
from smcity.polygons.complex_polygon_strategy import ComplexPolygonStrategy

class TestComplexPolygonStrategy:
    ''' Unit tests for the ComplexPolygonStrategy class. '''

    def test_get_bounding_box(self):
        ''' Tests the get_bounding_box function. '''
        points = [[(-5, -5), (-1, 0), (-5, 5), (0, 1), (5, 5), (1, 0), (5, -5), (0, -1)]]
        polygon = ComplexPolygonStrategy(points) 

        bounding_box = polygon.get_bounding_box()

        assert bounding_box['min_lat'] == -5, bounding_box['min_lat']
        assert bounding_box['min_lon'] == -5, bounding_box['min_lon']
        assert bounding_box['max_lat'] == 5, bounding_box['max_lat']
        assert bounding_box['max_lon'] == 5, bounding_box['max_lon']

    def test_is_filtered(self):
        ''' Tests the is_filtered function. '''
        points = [[(-2, -2), (-2, 2), (-1, 2), (-1, 0), (1, 0), (1, 2), (2, 2), (2, -2)]]
        polygon = ComplexPolygonStrategy(points)
        
        data = MockData({'location' : (-5, -1)})
        assert polygon.is_filtered(data) == True
        
        data = MockData({'location' : (-5, 1)})
        assert polygon.is_filtered(data) == True

        data = MockData({'location' : (0, 5)})
        assert polygon.is_filtered(data) == True

        data = MockData({'location' : (-1.5, 0)})
        assert polygon.is_filtered(data) == False
    
        data = MockData({'location' : (-1.5, 0.5)})
        assert polygon.is_filtered(data) == False

        data = MockData({'location' : (1.5, 0.5)})
        assert polygon.is_filtered(data) == False
