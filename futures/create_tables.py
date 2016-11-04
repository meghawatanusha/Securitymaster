from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
from sqlalchemy.orm import *
from time import time
from datetime import datetime
from sqlalchemy.dialects.mysql import DATETIME, VARCHAR, INTEGER, DOUBLE, BIGINT


Base = declarative_base()

class Attributes(Base):
    __tablename__ = 'attributes3'
    egid = Column(Integer, primary_key=True, nullable=False)
    attribute = Column(String(40),primary_key=True, nullable=False)
    attributevalue = Column(String(40), nullable=False)
    startdate = Column(DATETIME, nullable=False)
    enddate = Column(DATETIME, nullable=False)
 
class Instruments(Base):
    __tablename__ = 'instruments3'
    egid = Column(Integer, primary_key=True)
    instrument_type = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    startdate = Column(DATETIME, nullable=False)
    enddate = Column(DATETIME, nullable=False)

class Prices(Base) :
    __tablename__ = 'prices3'
    egid = Column(Integer, primary_key=True)
    trade_date = Column(DATETIME, primary_key=True, nullable=False)
    open_price = Column(DOUBLE)
    high_price = Column(DOUBLE)
    low_price = Column(DOUBLE)
    close_price = Column(DOUBLE)
    settle_price = Column(DOUBLE)
    volume_traded = Column(INTEGER)

class New_entries(Base) :
    __tablename__ = 'new_entries3'
    egid = Column(Integer, primary_key=True)
    instrument_type = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    startdate = Column(DATETIME, nullable=False)
    
class last_egid(Base) :
    __tablename__='last_generated_egid3'
    last_generated_egid=Column(Integer, primary_key=True)
    
class Future_series(Base):
    __tablename__ = 'futureseries3'
    future_series_id = Column(Integer, primary_key=True, nullable=False)
    underlying_id = Column(Integer, nullable=False)
    startdate = Column(DATETIME, nullable=False)
    enddate = Column(DATETIME, nullable=False)

class Futures_contract_table(Base):
    __tablename__ = 'futures_contract_table3'
    future_id = Column(Integer, primary_key=True)
    nseticker = Column(VARCHAR(40))
    future_series_id = Column(Integer, primary_key=False, nullable=False, autoincrement=False)
    contract_size= Column(Integer)
    list_date = Column(DATETIME, nullable=False)
    expiration_date = Column(DATETIME,nullable=False)
    
class Open_interest_table (Base) :
    __tablename__='open_interest_table3'
    future_id=Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    trade_date=Column(DATETIME, primary_key=True,nullable=False)
    open_interest= Column(DOUBLE)
    change_in_open_interest=Column(DOUBLE)
  
if __name__=="__main__":
    t=time()

    db = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/eg11')

    db.echo = False  # Try changing this to True and see what happens

    Base.metadata.create_all(db)

    session = sessionmaker()
    session.configure(bind=db)
    s = session()

    try:
      s.commit()

    finally:
      s.close()

    print "Time elapsed: " + str(time() - t) + " s."
