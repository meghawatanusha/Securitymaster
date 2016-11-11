from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
from sqlalchemy.orm import *
from time import time
from datetime import datetime
from sqlalchemy.dialects.mysql import DATETIME, VARCHAR, INTEGER, DOUBLE, BIGINT


Base = declarative_base()

class Attributes(Base):
    __tablename__ = 'attributes'
    egid = Column(Integer, primary_key=True, nullable=False)
    attribute = Column(String(40),primary_key=True, nullable=False)
    attributevalue = Column(String(40), nullable=False)
    startdate = Column(DATETIME, primary_key=True)
    enddate = Column(DATETIME,nullable=False)
 
class Instruments(Base):
    __tablename__ = 'instruments'
    egid = Column(Integer, primary_key=True)
    instrument_type = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    startdate = Column(DATETIME, nullable=False)
    enddate = Column(DATETIME, nullable=False)

class Prices(Base) :
    __tablename__ = 'prices'
    egid = Column(Integer, primary_key=True)
    trade_date = Column(DATETIME, primary_key=True, nullable=False)
    open_price = Column(DOUBLE)
    high_price = Column(DOUBLE)
    low_price = Column(DOUBLE)
    close_price = Column(DOUBLE)
    settle_price = Column(DOUBLE)
    volume_traded = Column(INTEGER)
    total_turnover = Column(DOUBLE)

class Prices_futures(Base) :
    __tablename__ = 'prices_futures'
    egid = Column(Integer, primary_key=True)
    trade_date = Column(DATETIME, primary_key=True, nullable=False)
    open_price = Column(DOUBLE)
    high_price = Column(DOUBLE)
    low_price = Column(DOUBLE)
    close_price = Column(DOUBLE)
    settle_price = Column(DOUBLE)
    volume_traded = Column(INTEGER)

class New_entries(Base) :
    __tablename__ = 'new_entries'
    egid = Column(Integer, primary_key=True)
    instrument_type = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    instrument_name=Column(VARCHAR(40))
    startdate = Column(DATETIME, nullable=False)
    
class last_egid(Base) :
    __tablename__='last_generated_egid'
    serial_no = Column(Integer, primary_key=True)
    last_generated_egid=Column(Integer, primary_key=False)
    
class Future_series(Base):
    __tablename__ = 'futureseries'
    future_series_id = Column(Integer, primary_key=True, nullable=False)
    future_series_name = Column (VARCHAR(40))
    underlying_id = Column(Integer, nullable=False)
    underlying_name = Column(VARCHAR(40))
    startdate = Column(DATETIME, primary_key=True)
    enddate = Column(DATETIME)

class Futures_contract_table(Base):
    __tablename__ = 'futures_contract_table'
    future_id = Column(Integer, primary_key=True)
    trade_date = Column(DATETIME,primary_key=True)
    name = Column(VARCHAR(40))
    future_series_id = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    contract_size= Column(Integer)
    list_date = Column(DATETIME,nullable=False)
    expiration_date = Column(DATETIME, primary_key=True)
    last_trade_date = Column(DATETIME,nullable=False)

    
class Open_interest_table (Base) :
    __tablename__='open_interest_table'
    future_id=Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    trade_date=Column(DATETIME, primary_key=True,nullable=False)
    open_interest= Column(DOUBLE)
    change_in_open_interest=Column(DOUBLE)

def populate_last_egid (last_egid_dao, db_session, serial_no,last_generated_egid):
    db_record=last_egid_dao(**{ 'serial_no' : serial_no, 'last_generated_egid' : last_generated_egid})
    db_session.add(db_record)

  
if __name__=="__main__":
    t=time()

#    db = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/eg8')
    db = create_engine('mysql+pymysql://bors_master:uJ!DicDu*7sW@bors-db1.inmu1.eglp.com:3306/futures_new')

    db.echo = False  # Try changing this to True and see what happens

    Base.metadata.create_all(db)

    session = sessionmaker()
    session.configure(bind=db)
    s = session()

    try:
      #populate_last_egid (last_egid, s, 1,3016)

      s.commit()

    finally:
      s.close()

    print "Time elapsed: " + str(time() - t) + " s."
