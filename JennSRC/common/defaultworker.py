

    


'''
class DefaultWorker():
    
    worker_id = 0
    
    def __init__(self, task_queue: Queue, result_queue: Queue):
        DefaultWorker.worker_id += 1
        self.task_queue = task_queue
        self.result_queue = result_queue
        self._log = self.logger
        
    def run(self)
    '''