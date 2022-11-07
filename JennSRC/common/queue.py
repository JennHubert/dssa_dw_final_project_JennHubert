from asyncio import Queue as AQueue
from queue import Queue as Queue
from multiprocessing import JoinableQueue
from typing import Union


class QueueFactory:
#Factory class - returns a supported queue type
    @staticmethod 
	#I use the above so I do not need to use self.
    def factory(type: str = 'default') -> Union[Queue, AQueue, JoinableQueue]:
        '''Factory that returns a queue based on type
	
	Args:
	



	Returns:
	'''
        if type == 'default':
            return Queue()
        elif type == 'multi-threading':
            return Queue()
        elif type == 'multi-processing':
            return JoinableQueue()
        elif type == 'asyncio':
            return AQueue()
        else:
            raise ValueError(type)
