import os
import sys
import csv
import sqlalchemy
from sqlalchemy import *
import numpy
from numpy import genfromtxt
from time import time
import datetime
from sqlalchemy import Column, Integer, Float, Date, String, BIGINT
from sqlalchemy.dialects.mysql import DATETIME, DOUBLE, INTEGER, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey
from table_of_attri_instru import Base
from sqlalchemy.ext.automap import automap_base

def Load_Data(year="2013",mon="NOV",dd="06"):
   #SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,
    file_name="cm%s%s%sbhav.csv" % (dd,mon,year)
    if(os.path.isfile(file_name)): 
        data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda y: str(y),1: lambda z: str(z),10: lambda x: str(x),12: lambda a: str(a)})
        return data.tolist()
    return []


def main(args):
    
    start_date = datetime.datetime(int(args[0]), int(args[1]), int(args[2]))
    now = datetime.datetime.now()
    x = 0
    y=0
	
    t = time()

    #Create the database
    engine = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/eg2')

    engine.echo = False  # Try changing this to True and see what happens

    # produce our own MetaData object
    metadata = MetaData()

    # we can reflect it ourselves from a database, using options
    # such as 'only' to limit what tables we look at...
    metadata.reflect(engine, only=['attributes1','instruments1','prices1','last_generated_egid','new_entries1','dead_stocks1'])

#    # ... or just define our own Table objects with it (or combine both)
 #   Table('user_order', metadata,
 #               Column('id', Integer, primary_key=True),
 #               Column('user_id', ForeignKey('user.id'))
 #           )
 # we can then produce a set of mappings from this MetaData.
    Base = automap_base(metadata=metadata)

    # calling prepare() just sets up mapped classes and relationships.
    Base.prepare(engine, reflect=True)

    Attributes = Base.classes.attributes1
    Instruments = Base.classes.instruments1
    Prices = Base.classes.prices1
    last_generated_egid=Base.classes.last_generated_egid
    New_entries = Base.classes.new_entries1
    Dead_stocks = Base.classes.dead_stocks1

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
   
    while(start_date < now):
        month = start_date.strftime("%b")
        day = str(start_date.day)
        if (len(day) != 2):
            day = "0" + day
        
	session1 = sessionmaker()
        session1.configure(bind=engine)
        s1 = session1()

		
	data = Load_Data(start_date.year,month.upper(),day)
    
	#SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,
        try: 
	    for i in data:
			    #if equity already exists, populate price table correspoding to the egid and current date
				
                if (query.filter(Attributes.attributevalue.match('i[0]')) > 0):   #using the match function in orm to find nseticker equivalent to 
				#can use exists,filterby or match querry                        #mysql statement - SELECT * FROM 'mytable' WHERE INSTR('mycol', 'abc') > 0
                #sqlalchemy orm querries                                                                 #maybe its wrong
                    y = s1.query(Attributes).get(nseticker) # get egid of the existing stock corresponding to the nseticker maybe its wrong
		    record3 =  Prices(**{
                                         'egid' : y,
                                         'todays_date' : datetime.strptime(i[10],'%d-%b-%Y').date(),
				                         'open_price':i[2],
                                         'high_price':i[3],
                                         'low_price':i[4],
                                         'close_price':i[5],
                                         'volume_traded':i[11]
                                  })
                    s1.add(record3)
                
				# if equity does not exist already in the attributes table, generate egid, and populate all the three tables - attri,instru,prices.
		else:
		    x=x+1   #generate unique egid for the new stock
                    record1a =  Attributes(**{'egid':x, 'attribute':2, 'attributevalue':i[0],}) #populate attributes table with egid
                    s1.add(record1a)

                    record1b = Attributes(**{'egid':x, 'attribute':5, 'attributevalue':i[12]})   #populate attributes table with isin
                    s1.add(record1b)

                    record2 = Instruments(**{'egid':x,                                            #populate instruments table with egid and startdate 
				                         'instrument_type':1, 
				                         'startdate':datetime.strptime(i[10],'%d-%b-%Y').date(),
										 'enddate':NULL
                                      })
                    s1.add(record2)

                    record3 =  Prices(**{                                                          #populate prices table correspoding to the egid
                                         'egid' : x,
                                         'todays_date' : datetime.strptime(i[10],'%d-%b-%Y').date(),
				                         'open_price':i[2],
                                         'high_price':i[3],
                                         'low_price':i[4],
                                         'close_price':i[5],
                                         'volume_traded':i[11]
                                  })
                    s1.add(record3)

		    record4 =  New_entries(**{'egid':x, 'startdate':datetime.strptime(i[10],'%d-%b-%Y').date()}) #populate New_entries table with egid and start date
                    s1.add(record4)

					
   	    s1.commit() #Attempt to commit all the records before checking the delisting

        #code for finding the delisted stocks
                			
					
	    record6 = last_generated_egid(**{'last_generated_egid':x})                                         #recording the last generated egid
            
	    s1.add(record6)                                                                           #saving it in the last_egid table
            
	    s1.commit() #Attempt to commit all the records
            
	    print  "Last generated egid is" 'x.'
			
         #   add record in last egid table

        except:
                s1.rollback() #Rollback the changes on error

        finally:
                s1.close() #Close the connection
        print "Total Time elapsed in this session is: " + str(time() - t) + " s1." #0.091s
		
	start_date += datetime.timedelta(days=1)
                 
			 
			 
	print "Total Time elapsed: " + str(time() - t) + " s." #0.091s

if __name__ == "__main__":
    main(sys.argv[1:])
