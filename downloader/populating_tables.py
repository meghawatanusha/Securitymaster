import sqlalchemy
from sqlalchemy import *
import numpy
from numpy import genfromtxt
from time import time
from datetime import datetime
from sqlalchemy import Column, Integer, Float, Date, String
from sqlalchemy.dialects.mysql import DATETIME, DOUBLE, INTEGER, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def Load_Data(file_name):
   #SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,
    data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda y: str(y),1: lambda z: str(z),10: lambda x: str(x),12: lambda a: str(a)})
    return data.tolist()

Base = declarative_base()

class Price_History1(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'Price_History1'
    #__table_args__ = {'mysql_autoincrement': True}
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, nullable=False,autoincrement=True)
    symbol = Column(VARCHAR(100))
    series= Column(VARCHAR(100))
    opn = Column(DOUBLE)
    hi = Column(DOUBLE)
    lo = Column(DOUBLE)
    close = Column(DOUBLE)
    last = Column(DOUBLE)
    prevclose = Column(DOUBLE)
    tottrdqty = Column(INTEGER)
    tottrdval = Column(DOUBLE)
    timestamp = Column(DATETIME)
    vol = Column(INTEGER)
    ISIN = Column(VARCHAR(100))

if __name__ == "__main__":
    t = time()

    #Create the database
    engine = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/egilsecuritymaster')

    engine.echo = False  # Try changing this to True and see what happens

	#       engine = create_engine('sqlite:///csv_test.db')
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    try:
        file_name = "cm01SEP2016bhav.csv" #sample CSV file used:  http://www.google.com/finance/historical?q=NYSE%3AT&ei=W4ikVam8LYWjmAGjhoHACw&output=csv
        data = Load_Data(file_name)
#SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,

        for i in data:
            record =  Price_History1(**{
                'symbol': i[0],
                'series': i[1],
                'opn':i[2],
                'hi':i[3],
                'lo':i[4],
                'close':i[5],
                'last':i[6],
                'prevclose':i[7],
                'tottrdqty':i[8],
                'tottrdval':i[9],
                'timestamp' : datetime.strptime(i[10],'%d-%b-%Y').date(),
                'vol':i[11],
                'ISIN':i[12]
            })
            s.add(record) #Add all the records

        s.commit() #Attempt to commit all the records
    except:
        s.rollback() #Rollback the changes on error
    finally:
        s.close() #Close the connection
    print "Time elapsed: " + str(time() - t) + " s." #0.091s
-- INSERT --                                                  