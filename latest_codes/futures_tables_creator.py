from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
from sqlalchemy.orm import *
from time import time
from datetime import datetime
from sqlalchemy.dialects.mysql import DATETIME, VARCHAR, INTEGER, DOUBLE, BIGINT
from create_tables import Base


#Base_future = declarative_base()

class Future_series(Base):
    __tablename__ = 'future_series1'
    future_series_id = Column(Integer, primary_key=True, nullable=False)
    underlying_id = Column(Integer, nullable==False)

class Futures_contract_table(Base):
    __tablename__ = 'futures_contract_table1'
    future_id = Column(Integer, primary_key=True)
	nseticker = Column(VARCHAR(40))
    future_series_id = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    contract_size= Column(Integer)
	list_date = Column(DATETIME)
    expiration_date = Column(DATETIME)
    #table_of_attributes1 = relationship("table_of_attributes", back_populates="table_of_instruments1")
	
	
class Open_interest_table (Base) :
    __tablename__='open_interest_table1'
	future_id=Column(Integer, primary_key=True, nullable=False, autoincrement=False)
	todays_date=Column(DATETIME)
	open_interest= Column(Integer)
	

class Prices(Base) :
    __tablename__ = 'prices1'
    egid = Column(Integer, primary_key=True)
    todays_date = Column(DATETIME, primary_key=True)
    open_price = Column(DOUBLE)
    high_price = Column(DOUBLE)
    low_price = Column(DOUBLE)
    close_price = Column(DOUBLE)
    settle_price = Column(DOUBLE)
    volume_traded = Column(INTEGER)
    #table_of_attributes1 = relationship("table_of_attributes", back_populates="table_of_prices1")

class New_entries(Base) :
     __tablename__ = 'new_entries1'
    egid = Column(Integer, primary_key=True)
    instrument_type = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    startdate = Column(DATETIME)
	
class Dead_stocks_or_futures(Base) :
    __tablename__ = 'dead_stocks1'
    egid = Column(Integer, primary_key=True)
    instrument_type = Column(Integer, primary_key=True, nullable=False, autoincrement=False)
    enddate = Column(DATETIME)
	
	

class last_egid(Base) :
    __tablename__='last_generated_egid'
    last_generated_egid=Column(Integer, primary_key=True)
	
	
	
	
if __name__=="__main__":
    t=time()

    db = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/eg2')

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
