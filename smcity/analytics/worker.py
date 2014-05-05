''' Contains the backend worker that actually handles performing the analytical tasks. '''

import operator
import string

from threading import Thread

from smcity.misc.logger import Logger

logger = Logger(__name__)

class ComplexFilter:
    ''' Interface definition for a post-fetch filter that determines whether or not the data point is kept. '''

    def is_filtered(self, data):
        '''
        Determines whether or not the provided data point is filtered out.

        @param data Data point to be checked for filtering
        @paramType Data
        @returns Whether or not the data point is filtered out
        @returnType boolean
        '''
        raise NotImplementedError()

class Worker():
    ''' Handles actually performing the analytical tasks. '''
  
    def __init__(self, config, result_queue, task_queue, data_factory):
        '''
        Constructor.
 
        @param config Configuration settings for this class. Expected definitions:
         
        Section: worker
        Key:     batch_copy_size
        Type:    int
        Desc:    How many data points to process before committing the intermediate results

        @paramType ConfigParser
        @param result_queue Interface for posting work results
        @paramType ResultQueue
        @param task_queue Interface for retrieving tasks to be performed
        @paramType TaskQueue
        @param data_factory Interface for retrieving data
        @paramType DataFactory
        @returns n/a
        '''
        assert result_queue is not None
        assert task_queue is not None
        assert data_factory is not None

        self.batch_copy_size  = config.getint('worker', 'batch_copy_size') 
        self.is_shutting_down = False
        self.result_queue     = result_queue
        self.task_queue       = task_queue
        self.data_factory     = data_factory

    def calculate_trending_topics(self, data_set_id):
        '''
        Calculates the trending topics in the provided data set.

        @param data_set_id Tracking id of the set whose trending topics should be calculated.
        @paramType string
        @returns List of (topic, # occurrence) tuples sorted from most frequent to least frequent
        @returnType List of (string, int) tuples
        '''
        topics = {}
        
        punctuation_list = string.punctuation
        punctuation_list = punctuation_list.replace('#', '') # Keep the topic hash
        print "Punctuation List:", punctuation_list

        punctuation_map = dict((ord(char), None) for char in string.punctuation)
        del punctuation_map[35] # Keep the topic hash
        print "Punctuation Map:", punctuation_map

        for datum in self.data_factory.get_data_set(data_set_id): # For all the data in the set
            # Break the data content into useful tokens
            content_tokens = datum.get_content()
  
            if type(content_tokens) is unicode: # Strip out any non-hash punctuation
                content_tokens = content_tokens.translate(punctuation_map)
            else:
                content_tokens = content_tokens.translate(string.maketrans("",""), punctuation_list)
            
            content_tokens = content_tokens.split()

            for content_token in content_tokens: # Check each token to see if it is a hash-tagged topic
                print content_token
                if content_token.startswith('#'): # If this is a hash-tagged topic
                    if content_token.upper() in topics.keys(): # If this is not the first occurrence
                        topics[content_token.upper()] += 1 # Increase the occurrence count
                    else: # If this is the first occurrence
                        topics[content_token.upper()] = 1

        # Sort the hash-tagged topics in order of occurrence frequency
        results = sorted(topics.iteritems(), key=operator.itemgetter(1))
        results.reverse()

        return results

    def filter_data_parallel(self, num_segments, in_data_set_id, out_data_set_id,
                             min_lat = None, max_lat = None,
                             min_lon = None, max_lon = None,
                             min_timestamp = None, max_timestamp = None,
                             data_type = None, keywords = None,
                             complex_filters = []):
        ''' 
        Performs a parallel filtering operation. @see _filter_data
        
        @param num_segments # of threads to spin up for the parallel filtering
        @paramType int
        @returns n/a
        '''
        filterers = []

        for segment_id in range(num_segments): # For each segment, spin up a filtering thread
            kwargs = {
                'segment_id' : segment_id,
                'num_segments' : num_segments,
                'in_data_set_id' : in_data_set_id,
                'out_data_set_id' : out_data_set_id,
                'min_lat' : min_lat,
                'max_lat' : max_lat,
                'min_lon' : min_lon,
                'max_lon' : max_lon,
                'min_timestamp' : min_timestamp,
                'max_timestamp' : max_timestamp,
                'data_type' : data_type,
                'keywords' : keywords,
                'complex_filters' : complex_filters
            }
            filterer = Thread(target=self._filter_data, kwargs=kwargs)
            filterer.start()
            filterers.append(filterer)

        for filterer in filterers:
            filterer.join() # Wait for the filter threads to finish before returning

    def _filter_data(self, in_data_set_id, out_data_set_id, 
                           min_lat = None, max_lat = None,
                           min_lon = None, max_lon = None,
                           min_timestamp = None, max_timestamp = None,
                           data_type = None, keywords = None,
                           complex_filters = [], segment_id = 0,
                           num_segments = 1):
        '''
        Creates a new filtered data set from the provided data set by applying the provided filtering
        parameters.

        @param min_timestamp Restricts data to those newer than the provided timestamp
        @paramType datetime
        @param max_timestamp Restricts data to those older than the provided timestamp
        @paramType datetime
        @param min_lon Restricts data to those whose longitude location is greater than the provided lon
        @paramType float
        @param max_lon Restricts data to those whose longitude location is less than the provided lon
        @paramType float
        @param min_lat Restricts data to those whose latitude location is greater than the provided lat
        @paramType float
        @param max_lat Restricts data to those whose latitude location is less than the provided lat
        @paramType float
        @param type Restricts data to a particular data type, i.e. twitter data
        @paramType string
        @param keywords Restricts data to those with content containing one or more of the provided keywords
        @paramType list of string
        @param complex_filters Additional complex filtering steps to be applied
        @paramType list of ComplexFilters
        @returns n/a
        '''
        assert in_data_set_id is not None
        assert out_data_set_id is not None

        # Retrieve all of the data points that meet all filtering criteria other than keywords
        datas = self.data_factory.filter_global_data(
            min_timestamp = min_timestamp, max_timestamp = max_timestamp,
            min_lat = min_lat, max_lat = max_lat, min_lon = min_lon, max_lon = max_lon,
            type=data_type, segment_id = segment_id, num_segments = num_segments
        )

        print "Keywords:", keywords

        punctuation_map = dict((ord(char), None) for char in string.punctuation)

        # Filter the data points for those containing the key words of interest
        batch = []
        num_points_processed = 0
        for data in datas:
            if keywords is not None: # If we are filtering on keyword content
                content_tokens = data.get_content()
                if type(content_tokens) is unicode:
                    content_tokens = content_tokens.translate(punctuation_map)
                else:
                    content_tokens = content_tokens.translate(string.maketrans("",""), string.punctuation)
                content_tokens = content_tokens.split()

                # Check each of the content tokens to see if it is a keyword
                keyword_found = False
                for content_token in content_tokens: 
                    if content_token.upper() in keywords: # If we've found a keyword
                        keyword_found = True # The data point passes the filter
                        break

                # If there is not keyword in this data point, filter it
                if not keyword_found:
                    continue
           
            is_filtered = False 
            for complex_filter in complex_filters: # Apply any complex filters
                if complex_filter.is_filtered(data): # If the data point gets filtered out by this filter
                    print "Filtering out point at ", data.get_location()
                    is_filtered = True
                    break

            if is_filtered: # If this data point needs to be filtered out
                continue

            print "Keeping data point:", data.get_content()
            batch.append(data) # Data point has survived all filtering so keep it!

            if len(batch) >= self.batch_copy_size: # If the batch is getting too big
                self.data_factory.copy_data(out_data_set_id, batch) # Commit the batch
                batch = []

        if len(batch) > 0: # Commit the remaining partial batch
            self.data_factory.copy_data(out_data_set_id, batch)

        # Notify the reducer, nothing complicated to send, just tell it we are done filtering
        self.result_queue.post_result({'set_id' : out_data_set_id})
