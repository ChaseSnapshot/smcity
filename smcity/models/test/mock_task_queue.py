''' Mock implementation of the TaskQueue model. '''

class MockTaskQueue:
    
    def finish_task(self, task):
        self.finished_task = task
 
    def get_task(self):
        return self.task

    def request_task(self, task):
        self.requested_task = task
