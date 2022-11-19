from apscheduler.schedulers.background import BackgroundScheduler

class DefaultScheduler(BackgroundScheduler):
    #Puts a singleton design pattern into place for BackgroundScheduler
    
    #This runs the workflow that is located elsewhere!
    
    _instance = None
    
    def _new_(cls):
        if cls._instance is None:
            cls._instance = super(DefaultScheduler, cls)._new_(cls)
            #Enter any initilization
        return cls._instance