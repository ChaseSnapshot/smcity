''' Description of a piece of generic data. '''

class Data:
  
    def get_content(self):
        '''
        @returns Content of the data, i.e. for twitter data this would be the tweeted message
        @returnType string
        '''
        raise NotImplementedError()

    def get_datum_id(self):
        '''
        @returns Unique tracking id of the data
        @returnType string/uuid
        '''
        raise NotImplementedError()

    def get_location(self):
        '''
        @returns Location of the data, i.e. for twitter data this would be where the user tweeted
        @returnType (longitude/float, latitude/float)
        '''
        raise NotImplementedError()

    def get_set_id(self):
        '''
        @returns Tracking id of the dataset this data belongs to
        @returnType string/uuid
        '''
        raise NotImplementedError()

    def get_timestamp(self):
        '''
        @returns When the data was created, i.e. for twitter data this would be when the user tweeted
        @returnType datetime
        '''
        raise NotImplementedError()

    def get_type(self):
        '''
        @returns Type of the data, i.e. twitter, facebook, etc.
        @returnType string/uuid
        '''
        raise NotImplementedError()

class DataFactory:
    
    def create_data(self, content, id, location, set_id, timestamp, type):
        '''
        Creates a new data record for the provided data details.

        @param content Data content, i.e. for Twitter this would be the tweet
        @paramType string
        @param id Data generator's original tracking id, i.e. for Twitter this would be the tweet id
        @paramType string
        @param location Location of the data
        @paramType (longitude/float, latitude/float)
        @param set_id Tracking id of the dataset this data belongs to
        @paramType string/uuid
        @param timestamp When the data was created
        @paramType datetime
        @param type Type of the data
        @paramType string
        @returns n/a
        '''
        raise NotImplementedError()

    def copy_data(self, set_id, datas):
        '''
        Copies the provided data record to be part of the set specified by set_id.

        @param set_id Tracking id of the set to which the data copy should belong
        @paramType string/uuid
        @param data Data records to copy to the new data set
        @paramType list of Data
        @returns n/a
        '''
        raise NotImplementedError()

    def filter_global_data(self, min_timestamp=None, max_timestamp=None,
                                 min_lat=None, max_lat=None,
                                 min_lon=None, max_lon=None,
                                 segment_id=0, num_segments=1,
                                 type=None
                                 ):
        '''
        Retrieves a slice of the global data using the provided filtering criteria.

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
        @returns Data set that satisfies the provided filtering criteria
        @returnType Iterator
        '''
        raise NotImplementedError()

    def get_data_set(self, set_id):
        '''
        Retrieves all data in the provided set.

        @param set_id Unique tracking id of the set
        @paramType string/uuid
        @returns Data contained in the specified set
        @returnType Iterator
        '''
        raise NotImplementedError()
