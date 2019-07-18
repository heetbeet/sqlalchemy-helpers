import sqlalchemy as sq
import sql_misc as sqm
from sqlalchemy.ext import declarative
import datetime
import uuid

class botInfo:
    #Class methods and SQL data structures
    Base = declarative.declarative_base()
    
    class sqlBotInfo(Base):
        __tablename__ = 'botInfo'
        name          = sq.Column(sqm.type_py2sql(str), primary_key=True)
        last_seen     = sq.Column(sqm.type_py2sql(datetime.datetime))
        def __repr__(self):
            return repr({key:val for key,val in self.__dict__.items() if key[:1]!='_'})
    
    class sqlGlobalInfo(Base):
        __tablename__ = 'globalInfo'
        id            = sq.Column(sqm.type_py2sql(int), primary_key=True)
        next_run      = sq.Column(sqm.type_py2sql(int))
        next_api_time = sq.Column(sqm.type_py2sql(datetime.datetime))
        
        def __repr__(self):
            return repr({key:val for key,val in self.__dict__.items() if key[:1]!='_'})
    
    def __init__(s, name=None):
        if name is None: 
            name = uuid.uuid4().hex
            
        s.name = name
        s.engine = engine = sq.create_engine('sqlite:///botinfo.db', echo=False)
        s.Base.metadata.create_all(s.engine, checkfirst=True)
        Session = sq.orm.sessionmaker(bind=engine)
        s.session = Session()
        s.remove_dead()
        s.check_in()
        
        globinfo = (s.session.query(s.sqlGlobalInfo)
                             .filter(s.sqlGlobalInfo.id==1))
        if not len(list(globinfo)):
            s.session.merge(s.sqlGlobalInfo(id=1,
                                            next_run=0,
                                            next_api_time=sqm.now()))
        s.session.commit()
        
    def remove_dead(s):
        for bot in (s.session.query(s.sqlBotInfo)
                             .filter(s.sqlBotInfo.last_seen < sqm.now()-sqm.secs(20))):
            s.session.delete(bot)
        s.session.commit()
        
    def check_in(s):
        s.session.merge(s.sqlBotInfo(name=s.name,
                                     last_seen=sqm.now()))
        s.session.commit()
        
    def check_out(s, next_api_time=None):
        if next_api_time is None:
            next_api_time = sqm.now()
            
        bots = (s.session.query(s.sqlBotInfo)
                         .filter(s.sqlBotInfo.last_seen > sqm.now()-sqm.secs(20))
                         .order_by(s.sqlBotInfo.name))
        
        idx = [i for i in range(len(list(bots))) if s.name == bots[i].name][0]
        
        next_run = (idx+1)%len(list(bots))
        
        s.session.merge(s.sqlGlobalInfo(id=1,
                                        next_run=next_run,
                                        next_api_time=next_api_time))
        s.session.commit()
            
    def is_my_turn(s):
        #Make sure you set the time every time you check
        s.check_in()
        
        bots = (s.session.query(s.sqlBotInfo)
                         .filter(s.sqlBotInfo.last_seen > sqm.now()-sqm.secs(20))
                         .order_by(s.sqlBotInfo.name))
        
        idx = [i for i in range(len(list(bots))) if s.name == bots[i].name][0]
        
        next_run = (s.session.query(s.sqlGlobalInfo)
                             .filter(s.sqlGlobalInfo.id==1))[0].next_run
        
        if idx == next_run%len(list(bots)):
            return True
        return False
    
    def is_api_ready(s):
        return (s.session.query(s.sqlGlobalInfo)
                         .filter(s.sqlGlobalInfo.id==1)[0].next_api_time <= sqm.now())