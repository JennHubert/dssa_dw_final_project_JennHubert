from queue import Queue as Queue


class QueueFactory:
#Factory class - returns a supported queue type
    @staticmethod 
	#I use the above so I do not need to use self.
    def factory(type: str = 'default'):
        '''Factory that returns a queue based on type
	
	Args:
        type (str) : type of queue to use. Defaults to FIFO thread-safe queue.
	Returns:
            Queue: Python Queue
	'''
        if type == 'default':
            return Queue()
        else:
            raise ValueError(type)
