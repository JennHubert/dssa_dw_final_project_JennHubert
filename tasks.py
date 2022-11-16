'''
A bit of code that we'll write and then import somewhere else

Task class is a container
'''
from typing import TypeVar, Callable



class TaskContainer():
    
    def __init__(self, func) -> None:
        self.func = func
        
    def run(self, *args, **kwargs):
        result = self.func(*args, **kwargs)
        return result

