''' Interface definition for the mapped task queue. '''

class TaskQueue:
    ''' Interface for requesting the execution of tasks by the computing nodes '''

    def finish_task(self, task):
        '''
        Removes the provided task from the queue, preventing any other compute nodes from trying
        to handle it.

        @param task Task to be removed. Requires keys 'job_id', 'task', 'coordinate_box' to
        be unaltered since retrieving the task via get_task()
        @paramType dictionary
        @returns n/a
        '''
        raise NotImplementedError()

    def get_task(self):
        '''
        Retrieves the next task in the queue.
 
        @returns dictionary with at least keys 'job_id', 'task', 'coordinate_box' or 
        None if no task is available
        @returnType dictionary
        '''
        raise NotImplementedError()

    def request_task(self, task):
        '''
        Submits the provided task request to the back end for processing.

        @param task Settings for what the task is and how it should be performed
        @paramType dictionary with at least keys 'job_id', 'task', 'coordinate_box'
        '''
        raise NotImplementedError()
