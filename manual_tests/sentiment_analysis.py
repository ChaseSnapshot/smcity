#!/usr/bin/python

import datetime
import geojson
import logging
import logging.config
import pickle
import time

from ConfigParser import ConfigParser
from pattern.en import sentiment
from uuid import uuid4

print "Setting up the logging configuration..."
logging.config.fileConfig('config/qa_smcity.conf')
logging.getLogger('boto').setLevel(logging.INFO)

from smcity.analytics.worker import Worker
from smcity.models.aws.aws_data import AwsDataFactory
from smcity.models.test.mock_result_queue import MockResultQueue
from smcity.models.test.mock_task_queue import MockTaskQueue
from smcity.polygons.complex_polygon_strategy import ComplexPolygonStrategyFactory
from smcity.transformers.geojson_transformer import GeoJsonTransformer

print "Loading the config settings..."
config = ConfigParser()
configFile = open('config/qa_smcity.conf')
config.readfp(configFile)
configFile.close()

print "Depickling the congressional districts..."
districts = pickle.load(open("manual_tests/ohio_districts_low_res.pickled", "rb"))
district_ids = []
geojson = None

# Set up the components
data_factory = AwsDataFactory(config)
result_queue = MockResultQueue()
task_queue   = MockTaskQueue()
worker       = Worker(config, result_queue, task_queue, data_factory)
transformer  = GeoJsonTransformer(data_factory)

# Extract the data points inside the time frame and geographic area of interest
age_limit    = datetime.datetime.now()
age_limit   += datetime.timedelta(hours=5) - datetime.timedelta(hours=1)
age_limit    = age_limit.timetuple()

for iter in range(len(districts)):
    print "District", (iter+1)

    district_ids.append(uuid4())
    bounding_box = districts[iter].get_bounding_box()
    key_words = ['OBAMA', 'POTUS', 'BARACK', 'PRESIDENT', 'PREZ']
    kwargs = {
        'num_segments' : 10,
        'in_data_set_id' : 'global',
        'out_data_set_id' : str(district_ids[iter]),
        'min_timestamp' : age_limit,
        'complex_filters' : [districts[iter]],
        'min_lat' : bounding_box['min_lat'],
        'max_lat' : bounding_box['max_lat'],
        'min_lon' : bounding_box['min_lon'],
        'max_lon' : bounding_box['max_lon']
    }

    print "Set ID:", district_ids[iter]
    print "Bounding Box:", bounding_box
    print "Time:", age_limit

    print "Filtering data..."
    start_time = time.time()
    worker.filter_data_parallel(**kwargs)
    run_time = time.time() - start_time
    print "Done filtering data! Took " + str(run_time) + " seconds!"

    avg_polarity = 0.0
    num_points = 0 
    for data in data_factory.get_data_set(str(district_ids[iter])):
         polarity, objectivity = sentiment(data.get_content())
         
         avg_polarity += polarity
         num_points += 1
    if num_points > 0:
        avg_polarity /= num_points
    print "Avg Polarity:", avg_polarity
    print "# Points:", num_points

    fill_color = '000000'
    if (avg_polarity > 0):
        polar_char = int(avg_polarity * 16)
        if polar_char <= 9:
            polar_char = str(polar_char)
        else:
            polar_char = chr(64+polar_char-9)
        
        fill_color = '0000' + polar_char + polar_char
    elif (avg_polarity < 0):
        polar_char = int(-avg_polarity * 16)
        if polar_char <= 9:
            polar_char = str(polar_char)
        else:
            polar_char = chr(64+polar_char-9)
 
        fill_color = polar_char + polar_char + '0000'

    fill_opacity = 1
    if avg_polarity == 0:
        fill_opacity = 0

    print "Fill Color:", fill_color
    print "Generating GeoJSON display..."
    if geojson is not None:
        geojson = transformer.plot_points(str(district_ids[iter]), geojson)
    else: 
        geojson = transformer.plot_points(str(district_ids[iter]))
    geojson = transformer.plot_polygon(districts[iter], geojson, properties={
        'num_points' : num_points,
        'polarity' : avg_polarity,
        'fill' : fill_color,
        'fill-opacity' : fill_opacity
    })

print "Writing out GeoJSON display..."
outfile = open('districts_sentiment.geojson', 'w')
outfile.write(geojson)
outfile.close()
