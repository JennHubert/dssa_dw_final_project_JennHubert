from typing import Dict, List, Type
from tasks import TaskContainer
from networkx import(DiGraph, topological_sort)

class DAG:
    def __init__(self) -> None:
        
        self.dag=DiGraph()
        pass
        
        
    def topological_sort(self):
    
        return list(topological_sort(G=self.dag))    
        
        
        
        
   