''' Units tests for the Twitter stream consumer. '''

import json
import logging

from boto.dynamodb2.table import Table
from ConfigParser import ConfigParser
from time import strftime, strptime

from smcity.models.test.mock_data import MockDataFactory
from smcity.streams.twitter_stream import TwitterStreamListener

logger = logging.getLogger(__name__)

class TestTwitterStreamListener():
    ''' Unit tests for the TwitterStreamListener class. '''

    def setup(self):
        ''' Set up before each test. '''
        logger.info('Setting up the test tweets table...')
        self.table = Table('test_tweets')

        logger.info('Setting up the TwitterStreamListener instance...')
        config = ConfigParser()
        config.add_section('twitter')
        config.set('twitter', 'auth_file', '.twitter')
        self.data_factory = MockDataFactory()
        self.stream_listener = TwitterStreamListener(config, self.data_factory)

    def test_on_data(self):
        ''' Tests the on_data function. '''
        logger.info('Setting up the test tweet...')
        tweet = json.dumps({
            'id_str' : 'id',
            'geo' : {'coordinates' : [0, 1]},
            'text' : 'text',
            'place' : {'full_name' : 'place'},
            'created_at' : 'Mon Jan 01 01:01:01 +0000 2014'
        })

        logger.info('Running the on_data function...')
        self.stream_listener.on_data(tweet)

        logger.info('Validating the results...')
        assert self.data_factory.created_data is not None
        assert self.data_factory.created_data['id'] == 'id', self.data_factory.created_data['id']
        assert self.data_factory.created_data['content'] == 'text', self.data_factory.created_data['message']
        assert self.data_factory.created_data['type'] == 'twitter', self.data_factory.created_data['type']
        assert self.data_factory.created_data['location'][0] == 0, \
            self.data_factory.created_data['location'][0]
        assert self.data_factory.created_data['location'][1] == 1, \
            self.data_factory.created_data['location'][1]
        assert strftime('%Y-%m-%d %H:%M:%S', self.data_factory.created_data['timestamp']) \
            == '2014-01-01 01:01:01', self.data_factory.created_data['timestamp']
