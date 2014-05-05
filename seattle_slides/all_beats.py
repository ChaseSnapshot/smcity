#!/usr/bin/python

import datetime
import geojson
import logging
import logging.config

from ConfigParser import ConfigParser
from uuid import uuid4

print "Setting up the logging configuration..."
logging.config.fileConfig('config/seattle_smcity.conf')
logging.getLogger('boto').setLevel(logging.INFO)

from smcity.analytics.worker import Worker
from smcity.models.aws.aws_data import AwsDataFactory
from smcity.models.test.mock_result_queue import MockResultQueue
from smcity.models.test.mock_task_queue import MockTaskQueue
from smcity.polygons.complex_polygon_strategy import ComplexPolygonStrategyFactory
from smcity.transformers.geojson_transformer import GeoJsonTransformer

print "Loading the config settings..."
config = ConfigParser()
configFile = open('config/seattle_smcity.conf')
config.readfp(configFile)
configFile.close()

# Load the GeoJSON description of the police beats
police_beats_geojson = geojson.loads(open('seattle_slides/seattle_police_beats.geojson').read())['features']

# Set up the components
data_factory = AwsDataFactory(config)
result_queue = MockResultQueue()
task_queue   = MockTaskQueue()
worker       = Worker(config, result_queue, task_queue, data_factory)
transformer  = GeoJsonTransformer(data_factory)
geojson      = None

for iter in range(len(police_beats_geojson)):
    police_beat = ComplexPolygonStrategyFactory().from_geojson(police_beats_geojson[iter]['geometry'])

    # Generate the data set of interest
    set_id    = str(uuid4())
    age_limit = datetime.datetime.now()
    age_limit -= datetime.timedelta(hours=1)
    age_limit = age_limit.timetuple()
    bounding_box = police_beat.get_bounding_box()
    keywords = ['Mayday', 'parade']
    print "Bounding Box:", bounding_box
    print "Set ID:", set_id
    print "Time:", age_limit
    kwargs = {
        'num_segments' : 4,
        'in_data_set_id' : 'global',
        'out_data_set_id' : set_id,
        'min_timestamp' : age_limit,
        'complex_filters' : [police_beat],
        'min_lat' : bounding_box['min_lat'],
        'max_lat' : bounding_box['max_lat'],
        'min_lon' : bounding_box['min_lon'],
        'max_lon' : bounding_box['max_lon']#,
#        'keywords' : keywords
    }
    print "Filtering data..."
    worker.filter_data_parallel(**kwargs)
    print "Done filtering data..."

    print "Generating GeoJSON display..."
    if geojson is None:
        geojson = transformer.plot_points(set_id)
    else:
        geojson = transformer.plot_points(set_id, geojson)
    geojson = transformer.plot_polygon(police_beat, geojson)

print "Writing out GeoJSON display..."
outfile = open('all_beats_mayday_tweets.geojson', 'w')
outfile.write(geojson)
outfile.close()
