''' Interface definition for result queue. '''

class ResultQueue:
    ''' Abstract interface for result queue that aggregates compute node results. '''

    def finish_result(self, result):
        '''
        Removes the provided result's message from the queue, preventing any other result consumers
        from trying to handle it.

        @param result Result to be removed. Requires keys 'job_id', 'task', 'coordinate_box' to 
        be unaltered since retrieving the result via get_result()
        @paramType dictionary
        @returns n/a
        '''
        raise NotImplementedError()

    def get_result(self):
        '''
        Retrieves a task result from the queue.

        @returns dictionary with at least keys 'job_id', 'task', 'coordinate_box' or None if
        no result is available
        @returnType dictionary
        '''
        raise NotImplementedError()

    def post_result(self, set_id, results={}):
        '''
        Submits the results of a count tweet task.

        @param set_id Tracking id of the produced data set
        @paramType string/uuid
        @param results Features of the resuls
        @paramType dictionary
        @returns n/a
        '''
        raise NotImplementedError()
