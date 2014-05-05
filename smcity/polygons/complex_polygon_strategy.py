''' Polygon strategy that facilitates performing analytics on complex polygons. '''

from sympy.geometry.point import Point
from sympy.geometry.polygon import Polygon

from smcity.polygons.polygon_strategy import PolygonStrategy

class ComplexPolygonStrategy(PolygonStrategy):
    ''' Polygon strategy that facilitates performing analytics on complex polygons. '''

    def __init__(self, sub_polygons):
        '''
        Constructor.

        @param Ordered sequence of points describing the polygon.
        @paramType List of (x, y) tuples
        @returns n/a
        '''
        assert len(sub_polygons) > 0, len(sub_polygons)

        self.bounding_box       = None # Lazily calculate bounding box
        self.sub_polygon_points = []
        self.sympy_sub_polygons = []

        for sub_polygon in sub_polygons: # For each raw sub-polygon
            if sub_polygon[0] != sub_polygon[-1]: # If the sub-polygon is not a closed loop
                sub_polygon.append(sub_polygon[0]) # Close the loop

            self.sub_polygon_points.append(sub_polygon)
            self.sympy_sub_polygons.append(Polygon(*(sub_polygon[:-1])))

    def get_bounding_box(self):
        ''' {@inheritDocs} '''
        if self.bounding_box is not None:
            return self.bounding_box

        min_lon = 9999999
        min_lat = 9999999
        max_lon = -9999999
        max_lat = -9999999
        for sub_polygon in self.sub_polygon_points: # For each sub-polygon
            for (lon, lat) in sub_polygon:
                if lon < min_lon:
                    min_lon = lon
                if lon > max_lon:
                    max_lon = lon
                if lat < min_lat:
                    min_lat = lat
                if lat > max_lat:
                    max_lat = lat

        self.bounding_box = {
            'min_lat' : min_lat,
            'min_lon' : min_lon, 
            'max_lat' : max_lat,
            'max_lon' : max_lon
        }

        return self.bounding_box

    def get_outline(self):
        ''' {@inheritDocs} '''
        return self.sub_polygon_points

    def is_filtered(self, data):
        ''' {@inheritDocs} '''
        lon, lat = data.get_location()

        # Calculate how many times our test ray passes through the polygons
        for sub_polygon in self.sympy_sub_polygons:
            intersect_ray = Polygon((lon, lat), (lon + 999999, lat))
            intersections = intersect_ray.intersection(sub_polygon)
            is_inside_polygon = False
 
            for intersection in intersections: # Inspect the intersection geometry
                if type(intersection) is Point:
                    print "Point!"
                    is_inside_polygon = not is_inside_polygon
                else:
                    print "Line!"

            if is_inside_polygon: # If the point is inside this sub-polygon
                return False # Do not filter the point

        return True # Not in any of the sub-polygons so filter it

class ComplexPolygonStrategyFactory:
    ''' Factory class that handles constructing complex polygons. '''

    def from_geojson(self, geojson_polygon):
        ''' 
        Constructs a ComplexPolygon from the provided GeoJSON polygon.

        @param geojson_polygon GeoJSON polygon to extract state from
        @paramType geojson.Polygon
        @returns Equivalent ComplexPolygon
        @returnType ComplexPolygon
        '''
        coordinates = geojson_polygon['coordinates']
        
        if geojson_polygon['type'] == "Polygon":
            if len(coordinates): # If there are holes in this polygon, ignore them
                coordinates = coordinates[0]

            # Convert the coordinates from lists to tuples
            coordinate_tuples = []
            for coordinate in coordinates:
                coordinate_tuples.append(tuple(coordinate))

            return ComplexPolygonStrategy([coordinate_tuples])
        elif geojson_polygon['type'] == "MultiPolygon":
            sub_polygons = []

            for sub_polygon in coordinates:
                sub_polygon = sub_polygon[0] # Drop any holes in the polygon

                # Convert the coordinates from lists to tuples
                coordinate_tuples = []
                for coordinate in sub_polygon:
                    coordinate_tuples.append(tuple(coordinate))

                sub_polygons.append(coordinate_tuples)

            return ComplexPolygonStrategy(sub_polygons)
        else:
            raise Exception("Unknown feature type '%s'!" % geojson_polygon['type'])
