''' Interface for generating GeoJSON displays of data sets. '''

import geojson

from geojson import Feature, FeatureCollection, Point, MultiPolygon

from smcity.transformers.display_transformer import DisplayTransformer

class GeoJsonTransformer(DisplayTransformer):
    ''' Interface for generating GeoJSON displays of data sets. '''

    def __init__(self, data_factory):
        '''
        Constructor.

        @param data_factory Interface for retrieving data sets
        @paramType DataFactory
        @returns n/a
        '''
        assert data_factory is not None

        self.data_factory = data_factory

    def plot_points(self, data_set_id, encoded_feature_collection=None):
        ''' {@inheritDocs} '''
        datas    = self.data_factory.get_data_set(set_id=data_set_id) # Fetch the data of the data sets
        features = []

        for data in datas:
            point      = Point(data.get_location())
            properties = {'content' : data.get_content()}
            
            features.append(Feature(geometry=point, properties=properties))

        feature_collection = None
        if encoded_feature_collection is not None: # If a feature set to add to was provided
            feature_collection = geojson.loads(encoded_feature_collection)
            feature_collection['features'].extend(features)
        else:
            feature_collection = FeatureCollection(features)
            
        return geojson.dumps(feature_collection)

    def plot_polygon(self, polygon_strategy, encoded_display=None, properties=None):
        ''' {@inheritDocs} '''
        outline = polygon_strategy.get_outline()
        polygon = MultiPolygon([outline])

        feature_collection = None
        if encoded_display is not None: # If a feature set to add to was provided
            feature_collection = geojson.loads(encoded_display)
            feature_collection['features'].append(Feature(geometry=polygon, properties=properties))
        else:
            feature_collection = FeatureCollection([Feature(geometry=polygon, properties=properties)])

        return geojson.dumps(feature_collection)

