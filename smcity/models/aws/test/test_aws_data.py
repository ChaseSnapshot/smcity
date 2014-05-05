''' Unit tests for the AwsDataFactory class. '''

import datetime
import logging

from boto.dynamodb2.table import Table
from ConfigParser import ConfigParser
from time import strptime

from smcity.models.aws.aws_data import AwsData, AwsDataFactory

logger = logging.getLogger(__name__)

class TestAwsDataFactory:
    ''' Unit tests for the AwsDataFactory class. '''

    def setup(self):
        ''' Set up before each test '''
        logger.info('Setting up the test data table...')
        self.global_table = Table('test_global_data')
        self.set_table = Table('test_set_data')        
 
        logger.info('Setting up the DataFactory instance...')
        config = ConfigParser()
        config.add_section('database')
        config.set('database', 'global_data_table', 'test_global_data')
        config.set('database', 'set_data_table', 'test_set_data')
        
        self.data_factory = AwsDataFactory(config)

        logger.info('Emptying the contents of the test data table...')
        for data in self.global_table.scan():
            data.delete()
        for data in self.set_table.scan():
            data.delete()
    
    def test_create_global_data(self):
        ''' Tests creating global data. '''
        content   = 'content'
        datum_id  = 'datum_id'
        lat       = 5.5
        lon       = 6.6
        set_id    = 'global'
        timestamp = strptime('2014-01-02 01:02:03', '%Y-%m-%d %H:%M:%S')
        type      = 'type'

        self.data_factory.create_data(content, datum_id, (lon, lat), set_id, timestamp, type)
         
        raw_record = self.global_table.get_item(datum_id=datum_id) # Check the created global data record
        assert raw_record is not None
        
        record     = AwsData(raw_record)
        assert record.get_content() == content, record.get_content()
        assert record.get_datum_id() == datum_id, record.get_datum_id()
        assert record.get_location() == (lon, lat), record.get_location()
        assert record.get_set_id() == set_id, record.get_set_id()
        assert record.get_timestamp() == timestamp, record.get_timestamp()
        assert record.get_type() == type, record.get_type()

        for record in self.set_table.scan(): # Make sure there weren't any set data records created
            assert False, "Should not have record in set table!"

    def test_create_set_data(self):
        ''' Tests creating non-global data. '''
        content   = 'content'
        datum_id  = 'id'
        lat       = 5.5
        lon       = 6.6
        set_id    = 'my_set'
        timestamp = strptime('2014-01-02 01:02:03', '%Y-%m-%d %H:%M:%S')
        type      = 'type'

        self.data_factory.create_data(content, datum_id, (lon, lat), set_id, timestamp, type)
        
        # Check the created set data record
        raw_record = self.set_table.get_item(set_id=set_id, datum_id=datum_id)
        assert raw_record is not None

        record = AwsData(raw_record)
        assert record.get_content() == content, record.get_content()
        assert record.get_datum_id() == datum_id, record.get_datum_id()
        assert record.get_location() == (lon, lat), record.get_location()
        assert record.get_set_id() == set_id, record.get_set_id()
        assert record.get_timestamp() == timestamp, record.get_timestamp()
        assert record.get_type() == 'type', record.get_type()
        
        for record in self.global_table.scan(): # Make sure there weren't any global data records created
            assert False, "Should not have record in set table!"

    def test_copy_data(self):
        ''' Tests the copy_data function. '''
        record1 = AwsData({
            'content' : 'content1',
            'datum_id' : 'id_1',
            'lat' : '10000000',
            'lat_copy' : '10000000',
            'lon' : '20000000',
            'lon_copy' : '20000000',
            'set_id' : 'set_id_1', 
            'timestamp' : 'timestamp1',
            'timestamp_copy' : 'timestamp1',
            'type' : 'type1'
        })
        record2 = AwsData({
            'content' : 'content2',
            'datum_id' : 'id_2',
            'lat' : '30000000',
            'lat_copy' : '300000000',
            'lon' : '40000000',
            'lon_copy' : '40000000',
            'set_id' : 'set_id_2',
            'timestamp' : 'timestamp2',
            'timestamp_copy' : 'timestamp2',
            'type' : 'type2'
        })

        self.data_factory.copy_data('set_id_3', [record1, record2])
         
        record = self.set_table.get_item(set_id='set_id_3', datum_id='id_1')
        assert record is not None
        assert record['content'] == 'content1', record['content']
        assert record['lat'] == '10000000', record['lat']
        assert record['lon'] == '20000000', record['lon']
        assert record['set_id'] == 'set_id_3', record['set_id']
        assert record['timestamp'] == 'timestamp1', record['timestamp']
        assert record['type'] == 'type1', record['type']

        record = self.set_table.get_item(set_id='set_id_3', datum_id='id_2')
        assert record is not None
        assert record['content'] == 'content2', record['content']
        assert record['lat'] == '30000000', record['lat']
        assert record['lon'] == '40000000', record['lon']
        assert record['set_id'] == 'set_id_3', record['set_id']
        assert record['timestamp'] == 'timestamp2', record['timestamp']
        assert record['type'] == 'type2', record['type']

    def test_filter_global_data(self):
        ''' Tests the filter_global_data function. '''
        self.data_factory.create_data(
            'content', 'id1', (0, 0), 'global', strptime('2014-01-02 01:02:03', '%Y-%m-%d %H:%S:%f'), 'type1'
        )
        self.data_factory.create_data(
            'content', 'id2', (0, 0.5), 'global', strptime('2014-01-03 01:02:03', '%Y-%m-%d %H:%S:%f'), 'type2'
        )
        self.data_factory.create_data(
            'content', 'id3', (0.5, 0.5), 'global', strptime('2014-01-02 01:02:05', '%Y-%m-%d %H:%S:%f'), 'type2'
        )

        # Test retrieve data by type only
        datas = self.data_factory.filter_global_data(type='type1')
        for data in datas:
            assert data.get_datum_id() == 'id1', data.get_id()

        # Test retrieve data by location only
        datas = self.data_factory.filter_global_data(min_lat=0, max_lat=1, min_lon=0.25, max_lon=1.25)
        for data in datas:
            assert data.get_datum_id() == 'id3', data.get_id()

        # Test retrieve data by timestamp
        datas = self.data_factory.filter_global_data(
            min_timestamp=strptime('2014-01-03 01:02:03', '%Y-%m-%d %H:%M:%S'),
            max_timestamp=strptime('2014-01-03 01:02:10', '%Y-%m-%d %H:%M:%S')
        )
        for data in datas:
            assert data.get_datum_id() == 'id1' or data.get_datum_id() == 'id3', data.get_datum_id()

    def test_get_set_data(self):
        ''' Tests the get_set_data function. '''
        self.data_factory.create_data(
            'content', 'id1', (0, 0), 'set_1', strptime('2014-01-02 01:02:03', '%Y-%m-%d %H:%S:%f'), 'type1'
        )
        self.data_factory.create_data(
            'content', 'id2', (0, 0), 'set_2', strptime('2014-01-02 01:02:03', '%Y-%m-%d %H:%S:%f'), 'type2'
        )

        datas = self.data_factory.get_data_set('set_1')
        for data in datas:
            assert data.get_datum_id() == 'id1', data.get_datum_id()
