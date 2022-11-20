from queue import Queue as Queue
from typing import Union


class QueueFactory:
#Factory class - returns a supported queue type
    @staticmethod 
	#I use the above so I do not need to use self.
    def factory(type: str = 'default') -> Union[Queue, AQueue, JoinableQueue]:
        '''Factory that returns a queue based on type
	
	Args:
        type (str) : type of queue to use. Defaults to FIFO thread-safe queue.
        Other accepted types are 'multi-processing 'asyncio' or 'multi-threading'
	Returns:
            Queue - JoinableQueue - AQueue : Python Queue
	'''
        if type == 'default':
            return Queue()
        elif type == 'multi-threading':
            return Queue()
        else:
            raise ValueError(type)
