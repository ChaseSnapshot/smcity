import geojson
import pickle

from smcity.polygons.complex_polygon_strategy import ComplexPolygonStrategyFactory

print "Loading districts file..."
districts_geojson = geojson.loads(open('manual_tests/ohio_districts_low_res.geojson').read())

print "Parsing districts..."
districts = []
for district_geojson in districts_geojson['features']:
    print "Parsing district " + str(len(districts) + 1) + "/" + str(len(districts_geojson['features']))
    districts.append(ComplexPolygonStrategyFactory().from_geojson(district_geojson['geometry']))

print "Pickling districts..."
pickled_district  = pickle.dump(districts, open("ohio_districts_low_res.pickled", "wb"))
