from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
from sqlalchemy.orm import *
from time import time
from datetime import datetime
from sqlalchemy.dialects.mysql import DATETIME, VARCHAR, INTEGER, DOUBLE, BIGINT
from sqlalchemy.ext.automap import automap_base


Base = declarative_base()

class Attribute_type(Base) :
    __tablename__ = 'attributetype'
    attributetype = Column ( VARCHAR(20))
    attribute_id = Column (INTEGER, primary_key=True)

class Instrument_type(Base) :
    __tablename__ = 'instrumenttype'    
    instrumenttype = Column ( VARCHAR(20))
    instrument_id = Column (INTEGER, primary_key=True)


class Attributes(Base):
    __tablename__ = 'attributes'
    egid = Column(Integer, primary_key=True, nullable=False,autoincrement=False )
    attribute = Column(String(40),primary_key=True, nullable=False)
    attributevalue = Column(String(40), nullable=False)
    startdate = Column(DATETIME, primary_key=True)
    enddate = Column(DATETIME)

class Instruments(Base):
    __tablename__ = 'instruments'
    egid = Column(Integer, primary_key=True,autoincrement=False )
    instrument_type = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    startdate = Column(DATETIME)
    enddate = Column(DATETIME)

class Prices(Base) :
    __tablename__ = 'prices'
    egid = Column(Integer, primary_key=True,autoincrement=False )
    todays_date = Column(DATETIME, primary_key=True)
    open_price = Column(DOUBLE)
    high_price = Column(DOUBLE)
    low_price = Column(DOUBLE)
    close_price = Column(DOUBLE)
    settle_price = Column(DOUBLE)
    volume_traded = Column(INTEGER)
    #table_of_attributes1 = relationship("table_of_attributes", back_populates="table_of_prices1")

class New_entries(Base) :
     __tablename__ = 'new_entries'
     egid = Column(Integer, primary_key=True,autoincrement=False )
     instrument_type = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
     instrument_name = Column(String(40))
     startdate = Column(DATETIME)
	
class Dead_stocks(Base) :
    __tablename__ = 'dead_stocks'
    egid = Column(Integer, primary_key=True,autoincrement=False )
    instrument_type = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    instrument_name = Column(String(40))
    enddate = Column(DATETIME)
	
class last_egid(Base) :
    __tablename__='last_generated_egid'
    serial_no=Column(Integer, primary_key=True,autoincrement=True)
    last_generated_egid = Column(Integer, nullable=False, autoincrement=False)
	
def populate_last_egid (last_egid_dao, db_session, serial_no,last_generated_egid):
    db_record=last_egid_dao(**{ 'serial_no' : serial_no, 'last_generated_egid' : last_generated_egid})
    db_session.add(db_record)	

def populate_attributestype (attributestype_dao, db_session,attributetype,attribute_id):
    db_record=attributestype_dao(**{ 'attributetype' : attributetype, 'attribute_id' :attribute_id})
    db_session.add(db_record)	

def populate_instrumentstype (instrumentstype_dao, db_session,instrumenttype,instrument_id):
    db_record=instrumentstype_dao(**{ 'instrumenttype' : instrumenttype, 'instrument_id' :instrument_id})
    db_session.add(db_record)
	
if __name__=="__main__":
    t=time()

    # dialect+driver://username:password@host:port/database
    db = create_engine('mysql+pymysql://bors_master:uJ!DicDu*7sW@bors-db1.inmu1.eglp.com:3306/egil')

    db.echo = False  # Try changing this to True and see what happens

    Base.metadata.create_all(db)
  
    metadata = MetaData()

    metadata.reflect(db, only=['last_generated_egid'])

    Base = automap_base(metadata=metadata)

    # calling prepare() just sets up mapped classes and relationships.
    Base.prepare(db, reflect=True)

    last_generated_egid=Base.classes.last_generated_egid
 
    session = sessionmaker()
    session.configure(bind=db)
    s = session()


    try:
       
      populate_last_egid (last_egid, s, 1,0)
   
      populate_attributestype (Attribute_type,s, 'EGID',1)
      populate_attributestype (Attribute_type,s, 'NSE Ticker',2)
      populate_attributestype (Attribute_type,s, 'Name',3)
      populate_attributestype (Attribute_type,s, 'RIC',4)
      populate_attributestype (Attribute_type,s, 'ISIN',5)

      populate_instrumentstype (Instrument_type,s, 'stocks',1)
      populate_instrumentstype (Instrument_type,s, 'futures',2)
      populate_instrumentstype (Instrument_type,s, 'futureseries',3)

      s.commit()

    finally:
      s.close()

    print "Time elapsed: " + str(time() - t) + " s."
