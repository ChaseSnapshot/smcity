''' Unit tests for the analytics Worker class. '''

from ConfigParser import ConfigParser

from smcity.analytics.worker import Worker
from smcity.models.test.mock_result_queue import MockResultQueue
from smcity.models.test.mock_task_queue import MockTaskQueue
from smcity.models.test.mock_data import MockData, MockDataFactory

class TestWorker:
    ''' Tests the Worker class. '''

    def setup(self):
        ''' Set up before each test. '''
        # Set up the testing configuration settings
        config = ConfigParser()
        config.add_section('worker')
        config.set('worker', 'batch_copy_size', '10')

        # Set up the mock testing components
        self.result_queue = MockResultQueue()
        self.task_queue = MockTaskQueue()
        self.data_factory = MockDataFactory()
         
        # Set up the Worker instance to be tested
        self.worker = Worker(config, self.result_queue, self.task_queue, self.data_factory)

    def test_calculate_trending_topics(self):
        ''' Tests the calculate_trending_topics function. '''
        self.data_factory.data = [
            MockData({'id' : '1', 'content' : "#yolo something about life!"}),
            MockData({'id' : '2', 'content' : "#yoLO! something else about life #YOLO but more exciting!"}),
            MockData({'id' : '3', 'content' : "#KendrickLamar Money trees, shake em!"}),
            MockData({'id' : '4', 'content' : "#SHESELLSSEASHELLS!!!! #Kendricklamar Snausages!"})
        ]

        # Run the calculate trending topics
        trending_topics = self.worker.calculate_trending_topics('mock_data_set')

        # Check the results
        assert len(trending_topics) == 3, len(trending_topics)
        assert trending_topics[0][0] == '#YOLO', trending_topics[0][0]
        assert trending_topics[0][1] == 3, trending_topics[0][1]
        assert trending_topics[1][0] == '#KENDRICKLAMAR', trending_topics[1][0]
        assert trending_topics[1][1] == 2, trending_topics[1][1]
        assert trending_topics[2][0] == '#SHESELLSSEASHELLS', trending_topics[2][0]
        assert trending_topics[2][1] == 1, trending_topics[2][1]

    def test_filter_data(self):
        ''' Tests the _filter_data() function '''
        # Set up the input data
        self.data_factory.data = [
            MockData({'id' : '1', 'content' : "This is a test message that should be filtered out!"}),
            MockData({'id' : '2', 'content' : "There's a gun in our school!; Don't filter me!"}),
            MockData({'id' : '3', 'content' : "There's a gunman in our school!; Filter me!"})
        ]

        # Run the filter function
        kwargs = {
            'in_data_set_id' : 'in_data_set_id',
            'out_data_set_id' : 'out_data_set_id',
            'keywords' : ['GUN']
        }
        self.worker._filter_data(**kwargs)

        # Check the results
        assert len(self.result_queue.posted_results) == 1, len(self.result_queue.posted_results)
        assert self.result_queue.posted_results[0]['set_id'] == 'out_data_set_id', \
            self.result_queue.posted_results[0]['set_id']
        
        assert len(self.data_factory.copied_data) == 1, len(self.data_factory.copied_data)
        assert self.data_factory.copied_data[0].get_datum_id() == '2', \
            self.data_factory.copied_data[0].get_datum_id()
        assert self.data_factory.copied_data_set_id == 'out_data_set_id', \
            self.data_factory.copied_data_set_id

    def test_strip_lingering_whitespace(self):
        ''' Tests using the _filter_data() function with content tokens burdened by hanging whitespace. '''
        # Set up the input data
        self.data_factory.data = [
            MockData({'id' : '1', 'content' : "'gun,"}),
            MockData({'id' : '2', 'content' : "gun;"}),
            MockData({'id' : '3', 'content' : "gun."})            
        ]

        # Run the filter function
        kwargs = {
            'in_data_set_id' : 'in_data_set_id',
            'out_data_set_id' : 'out_data_set_id',
            'keywords' : ['GUN']
        }
        self.worker._filter_data(**kwargs)

        # Check the results
        assert len(self.data_factory.copied_data) == 3, len(self.data_factory.copied_data)
