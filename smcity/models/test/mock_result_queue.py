''' Mock implemention of the ResultQueue. '''

class MockResultQueue:
    def __init__(self):
        self.posted_results = []

    def finish_result(self, result):
        self.finished_result = result

    def get_result(self):
        return self.result

    def post_result(self, results):
        self.posted_results.append(results)
