#!/usr/bin/python

import datetime
import geojson
import logging
import logging.config

from ConfigParser import ConfigParser
from uuid import uuid4

# Set up the logging configuration
logging.config.fileConfig('config/qa_smcity.conf')
logging.getLogger('boto').setLevel(logging.INFO)

from smcity.analytics.worker import Worker
from smcity.models.aws.aws_data import AwsDataFactory
from smcity.models.test.mock_result_queue import MockResultQueue
from smcity.models.test.mock_task_queue import MockTaskQueue
from smcity.polygons.complex_polygon_strategy import ComplexPolygonStrategyFactory
from smcity.transformers.geojson_transformer import GeoJsonTransformer

# Load the config settings
config = ConfigParser()
configFile = open('config/qa_smcity.conf')
config.readfp(configFile)
configFile.close()

# Load the GeoJSON description of Franklin county
franklin_geojson = geojson.loads(open('manual_tests/franklin.geojson').read())
franklin_county = ComplexPolygonStrategyFactory().from_geojson(franklin_geojson['features'][0]['geometry'])

# Set up the components
data_factory = AwsDataFactory(config)
result_queue = MockResultQueue()
task_queue   = MockTaskQueue()
worker       = Worker(config, result_queue, task_queue, data_factory)
transformer  = GeoJsonTransformer(data_factory)

# Generate the data set of interest
set_id    = str(uuid4())
age_limit = datetime.datetime.now()
age_limit += datetime.timedelta(hours=5) - datetime.timedelta(minutes=15)
age_limit = age_limit.timetuple()
bounding_box = franklin_county.get_bounding_box()
keywords = ['robbery', 'gun', 'steal', 'crime', 'shooter', 'robber', 'weapon', 'shoota']
print "Bounding Box:", bounding_box
print "Set ID:", set_id
print "Time:", age_limit
kwargs = {
    'num_segments' : 4,
    'in_data_set_id' : 'global',
    'out_data_set_id' : set_id,
    'min_timestamp' : age_limit,
    'complex_filters' : [franklin_county],
    'min_lat' : bounding_box['min_lat'],
    'max_lat' : bounding_box['max_lat'],
    'min_lon' : bounding_box['min_lon'],
    'max_lon' : bounding_box['max_lon']
#    'keywords' : keywords
}
print "Filtering data..."
worker.filter_data_parallel(**kwargs)
print "Done filtering data..."

# Generate the GeoJSON display
print "Generating GeoJSON display..."
geojson = transformer.plot_points(set_id)
geojson = transformer.plot_polygon(franklin_county, geojson)

print "Writing out GeoJSON display..."
outfile = open('franklin_county_this_hour.geojson', 'w')
outfile.write(geojson)
outfile.close()
