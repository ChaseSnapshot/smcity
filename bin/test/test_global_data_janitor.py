''' Tests the global_data_janitor script '''

import datetime
import logging
import subprocess

from boto.dynamodb2.table import Table
from ConfigParser import ConfigParser
from threading import Thread

from smcity.models.aws.aws_data import AwsData, AwsDataFactory

logger = logging.getLogger(__name__)

class TestGlobalDataJanitor:
    ''' Tests the global_data_janitor script. '''

    def setup(self):
        ''' Set up before each test. '''
        logger.info('Setting up the test global data table...')
        self.global_table = Table('test_global_data')

        logger.info('Setting up the DataFactory instance...')
        config = ConfigParser()
        config.add_section('database')
        config.set('database', 'global_data_table', 'test_global_data')
        config.set('database', 'set_data_table', 'test_set_data')       
 
        self.data_factory = AwsDataFactory(config)

        logger.info('Emptying the contents of the test data table...')
        for data in self.global_table.scan():
            data.delete()

    def _dump_err_pipe(self, process):
        err_line = ''

        while True:
            # Handle the out stream
            err = process.stderr.read(1)
            if err == '\n' or err == '\r':
                print err_line # Log the line
                err_line = ''
            elif err != '':
                err_line += err

            # Check if the stream are done
            if err == '' and process.poll() != None:
                break

    def _dump_out_pipe(self, process):
        out_line = ''

        while True:
            # Handle the out stream
            out = process.stdout.read(1)
            if out == '\n' or out == '\r':
                print out_line # Log the line
                out_line = ''
            elif out != '':
                out_line += out

            # Check if the stream are done
            if out == '' and process.poll() != None:
                break

    def test_clean_up_old_data(self):
        ''' Tests cleaning up old data from the global data table. '''
        timestamp = datetime.datetime.now() + datetime.timedelta(hours=5) - datetime.timedelta(days=2)
        self.data_factory.create_data('content', 'id1', (0, 0), 'global', timestamp.timetuple(), 'type')

        timestamp = datetime.datetime.now() + datetime.timedelta(hours=5) - datetime.timedelta(days=4)
        self.data_factory.create_data('content', 'id2', (0, 0), 'global', timestamp.timetuple(), 'type')

        timestamp = datetime.datetime.now() + datetime.timedelta(hours=5)
        self.data_factory.create_data('content', 'id3', (0, 0), 'global', timestamp.timetuple(), 'type')
        
        timestamp = datetime.datetime.now() + datetime.timedelta(hours=5) - datetime.timedelta(hours=3)
        self.data_factory.create_data('content', 'id4', (0, 0), 'global', timestamp.timetuple(), 'type')

        # Launch the sub-process
        process = subprocess.Popen(
            './bin/global_data_janitor ./config/test_smcity.conf',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Spin up the logging threads
        out_thread = Thread(target=self._dump_out_pipe, kwargs={'process' : process})
        out_thread.start()
        err_thread = Thread(target=self._dump_err_pipe, kwargs={'process' : process})
        err_thread.start()

        # Wait for the process to finish
        out_thread.join()
        err_thread.join()

        # Verify the correct records were deleted
        for raw_record in self.global_table.scan():
            record = AwsData(raw_record)
            assert (record.get_datum_id() == 'id3') or (record.get_datum_id() == 'id4'), \
                record.get_datum_id()
