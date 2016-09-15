from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
from sqlalchemy.orm import *
from time import time
from datetime import datetime
from sqlalchemy.dialects.mysql import DATETIME, VARCHAR, INTEGER, DOUBLE

Base = declarative_base()

class table_of_attributes(Base):
    __tablename__ = 'table_of_attributes1'
    egid = Column(Integer, primary_key=True, auto_increment=True, nullable=False)
    attribute = Column(String(40),primary_key=True, nullable=False)
    attributevalue = Column(String(40), nullable=False)
    table_of_instruments1 = relationship('table_of_instruments', back_populates="table_of_attributes1")
	table_of_prices1 = relationship('table_of_prices', back_populates="table_of_attributes1")

	class table_of_instruments(Base):
    __tablename__ = 'table_of_instruments1'
    instru_id=Column(Integer, primary_key=True, auto_increment=True, nullable=False)
	instrument_egid = Column(Integer, ForeignKey('table_of_attributes1.egid))
    instrument_type= Column(Integer, nullable=False)
    startdate = Column(DATETIME)
	enddate = Column(DATETIME)
    table_of_attributes1 = relationship("table_of_attributes", back_populates="table_of_instruments1")

class table_of_prices(Base) :
    __tablename__ = 'table_of_prices1'
    table_id=Column(Integer, primary_key=True, auto_increment=True, nullable=False)
	instrument_egid = Column(Integer, ForeignKey('table_of_attributes1.egid))
    todays_date = Column(DATETIME)
	open_price = Column(DOUBLE)
	high_price = Column(DOUBLE)
	low_price = Column(DOUBLE)
	close_price = Column(DOUBLE)
	settle_price = Column(DOUBLE)
	volume_traded = Column(INTEGER)
    table_of_attributes1 = relationship("table_of_attributes", back_populates="table_of_prices1")

if __name__=="__main__":
    t=time()

    db = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/eg2')

    db.echo = False  # Try changing this to True and see what happens
    
    Base.metadata.create_all(db)
	
    session = sessionmaker()
    session.configure(bind=db)
    s = session()

try: 

# u1 = table_of_instruments()
 #u1.instrument_type = 1
 #u1.startdate = "1/1/2016"

# a1 = table_of_attributes()
 #a1.attribute = 2
 #a1.att = "xyz"

 #u1.table_of_attributes1 = a1
 
 #s.add(a1)
 #s.add(u1)
 s.commit()
 
finally:
 s.close()
 
print "Time elapsed: " + str(time() - t) + " s." 