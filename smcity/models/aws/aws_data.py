''' Data model and factory implementations that are backed by Amazon Web Service's DynamoDB2 NoSQL database '''

from boto.dynamodb2.table import Table
from time import strftime, strptime

from smcity.misc.errors import CreateError, ReadError
from smcity.misc.logger import Logger
from smcity.models.data import Data, DataFactory

logger = Logger(__name__)

class AwsData(Data):
    ''' AWS specific implementation of the Data model. '''

    def __init__(self, record):
        '''
        Constructor.

        @param record Database record corresponding to this peice of Data.
        @paramType dictionary(dynamodb2.Item)
        @returns n/a
        '''
        assert record is not None

        self.record = record

    def get_content(self):
        ''' {@inheritDocs} '''
        return self.record['content']

    def get_datum_id(self):
        ''' {@inheritDocs} '''
        return self.record['datum_id']

    def get_location(self):
        ''' {@inheritDocs} '''
        lat = float(self.record['lat']) / 10000000
        lon = float(self.record['lon']) / 10000000

        return (lon, lat)

    def get_set_id(self):
        ''' {@inheritDocs} '''
        return self.record['set_id']

    def get_timestamp(self):
        ''' {@inheritDocs} '''
        return strptime(self.record['timestamp'], '%Y-%m-%d %H:%M:%S')

    def get_type(self):
        ''' {@inheritDocs} '''
        return self.record['type']

class AwsDataFactory(DataFactory):

    def __init__(self, config):
        '''
        Constructor.

        @param config Configuration settings. Expected definition:

        Section: database
        Key:     data_table
        Type:    string
        Desc:    Name of the Data model table
        @paramType ConfigParser
        @returns n/a
        '''
        self.global_table = Table(config.get('database', 'global_data_table'))
        self.set_table = Table(config.get('database', 'set_data_table'))

    def create_data(self, content, datum_id, location, set_id, timestamp, type):
        ''' {@inheritDocs} '''
        assert content is not None
        assert datum_id is not None
        assert -180 <= location[0] and location[0] < 180, location[0]
        assert -90 <= location[1] and location[1] < 90, location[1]
        assert set_id is not None
        assert timestamp is not None
        assert type is not None
        
        # Normalize the values
        lat_norm       = int(location[1] * 10000000)
        lon_norm       = int(location[0] * 10000000)
        timestamp_norm = strftime('%Y-%m-%d %H:%M:%S', timestamp)

        # Create the database record
        data = {
            'content' : content,
            'datum_id' : datum_id,
            'lat' : lat_norm,
            'lat_copy' : lat_norm,
            'lon' : lon_norm,
            'lon_copy' : lon_norm,
            'set_id' : set_id,
            'timestamp' : timestamp_norm,
            'timestamp_copy' : timestamp_norm,
            'type' : type
        }

        result = False 
        if set_id == 'global': # If this is a global data point
            result = self.global_table.put_item(data=data)
        else: # If this is a set data point
            result = self.set_table.put_item(data=data)

        # If we failed to create the database record
        if result is False:
            raise CreateError("Failed to create the Data(" + str(data) + ")!")
        
    def copy_data(self, set_id, datas):
        ''' {@inheritDocs} '''
        assert set_id is not None

        with self.set_table.batch_write() as batch:
            for data in datas:
                batch.put_item(data = {
                    'content' : data.get_content(), 
                    'datum_id' : data.get_datum_id(),
                    'lat' : data.record['lat'],
                    'lat_copy' : data.record['lat_copy'],
                    'lon' : data.record['lon'],
                    'lon_copy' : data.record['lon_copy'],
                    'set_id' : set_id,
                    'timestamp' : data.record['timestamp'],
                    'timestamp_copy' : data.record['timestamp_copy'],
                    'type' : data.record['type']
                })

    def filter_global_data(self, min_timestamp=None, max_timestamp=None,
                                 min_lat=None, max_lat=None,
                                 min_lon=None, max_lon=None,
                                 segment_id=0, num_segments=1,
                                 type=None
                                 ):
        ''' {@inheritDocs} '''
        kwargs = {}
        if min_timestamp is not None:
            kwargs['timestamp__gte'] = strftime('%Y-%m-%d %H:%M:%S', min_timestamp)
        if max_timestamp is not None:
            kwargs['timestamp_copy__lte'] = strftime('%Y-%m-%d %H:%M:%S', max_timestamp)
        if min_lat is not None:
            kwargs['lat__gte'] = int(min_lat * 10000000)
        if max_lat is not None:
            kwargs['lat_copy__lte'] = int(max_lat * 10000000)
        if min_lon is not None:
            kwargs['lon__gte'] = int(min_lon * 10000000)
        if max_lon is not None:
            kwargs['lon_copy__lte'] = int(max_lon * 10000000)
        if type is not None: 
            kwargs['type__eq'] = type
        kwargs['set_id__eq'] = 'global'
        kwargs['segment'] = segment_id
        kwargs['total_segments'] = num_segments

        logger.debug("Scan Args: %s", kwargs)

        return AwsDataIterator(self.global_table.scan(**kwargs))

    def get_data_set(self, set_id):
        ''' {@inheritDocs} '''
        return AwsDataIterator(self.set_table.query(set_id__eq=set_id))

class AwsDataIterator():
    ''' AWS specific implementation of the Data result set iterator. '''

    def __init__(self, result_set):
        '''
        Constructor.

        @param result_set DynamoDB2 ResultSet to wrap.
        @param boto.dynamodb2.ResultSet
        @returns n/a
        '''
        assert result_set is not None, "result_set must not be None!"

        self.result_set = result_set

    def __iter__(self):
        return self

    def next(self):
        return AwsData(self.result_set.next())
