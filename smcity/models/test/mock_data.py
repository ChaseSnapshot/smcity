''' Mock implementations of the Data and DataFactory classes. '''

from smcity.models.data import Data, DataFactory

class MockData:
    ''' Mock implementation of the data class. '''

    def __init__(self, data):
        self.data = data

    def get_content(self):
        return self.data['content']

    def get_datum_id(self):
        return self.data['id']

    def get_location(self):
        return self.data['location']

    def get_set_id(self):
        return self.data['set_id']

    def get_timestamp(self):
        return self.data['timestamp']

    def get_type(self):
        return self.data['type']

class MockDataFactory:
    ''' Mock implementation of the DataFactory class. '''

    def create_data(self, content, id, location, set_id, timestamp, type):
        ''' {@inheritDocs} '''
        self.created_data = {
            'content' : content,
            'id' : id,
            'location' : location,
            'set_id' : set_id,
            'timestamp' : timestamp,
            'type' : type
        }

    def copy_data(self, set_id, data):
        ''' {@inheritDocs} '''
        self.copied_data        = data
        self.copied_data_set_id = set_id

    def filter_global_data(self, min_timestamp=None, max_timestamp=None,
                                 min_lat=None, max_lat=None,
                                 min_lon=None, max_lon=None,
                                 segment_id=0, num_segments=1,
                                 type=None):
        ''' {@inheritDocs} '''
        return self.data

    def get_data_set(self, set_id):
        ''' {@inheritDocs} '''
        return self.data
