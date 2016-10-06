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
from create_tables3 import Base
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
    #now = datetime.datetime.now()
    now = start_date + datetime.timedelta(days=3)
	
    t = time()

    #Create the database
    engine = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/eg3')

    engine.echo = False  # Try changing this to True and see what happens

    # produce our own MetaData object
    metadata = MetaData()

    # we can reflect it ourselves from a database, using options
    # such as 'only' to limit what tables we look at...
    metadata.reflect(engine, only=['attributes3','instruments3','prices3','last_generated_egid3','new_entries3','dead_stocks3'])

#    # ... or just define our own Table objects with it (or combine both)
 #   Table('user_order', metadata,
 #               Column('id', Integer, primary_key=True),
 #               Column('user_id', ForeignKey('user.id'))
 #           )
 # we can then produce a set of mappings from this MetaData.
    Base = automap_base(metadata=metadata)

    # calling prepare() just sets up mapped classes and relationships.
    Base.prepare(engine, reflect=True)

    Attributes = Base.classes.attributes3
    Instruments = Base.classes.instruments3
    Prices = Base.classes.prices3
    last_egid=Base.classes.last_generated_egid3
    New_entries = Base.classes.new_entries3
    Dead_stocks = Base.classes.dead_stocks3

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s1 = session()
    x = 0
    #egid_counter = s1.query(last_generated_egid).first().last_generated_egid
   
    while(start_date <= now):
        month = start_date.strftime("%b")
        day = str(start_date.day)
        if (len(day) != 2):
            day = "0" + day
	
	data = Load_Data(start_date.year,month.upper(),day)
        active_stocks = {}
    
	#SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,
        try: 
	    for i in data:
                if (i[1] != 'EQ'):
                    continue
			    #if equity already exists, populate price table correspoding to the egid and current date
				
                active_stocks[i[0]] = 1
                #exist_query = s1.query(Attributes).filter(Attributes.attribute == 2).filter(Attributes.attributevalue == i[0])

                exist_query = s1.query(Attributes).filter(Attributes.attribute == 2).filter(Attributes.attributevalue == i[0])
                if (s1.query(exist_query.exists()).scalar()):   #using the match function in orm to find nseticker 
                                
                      existing_egid= s1.query(Attributes).filter(Attributes.attribute == 2).filter(Attributes.attributevalue == i[0]).first()
                                     # get egid of the existing stock corresponding to the nseticker maybe its wrong
                      if not existing_egid:
                        print "Error while fetching egid"
                        return 
		      record3 =  Prices(**{
                                         'egid' : existing_egid.egid,
                                         'todays_date' : datetime.datetime.strptime(i[10],'%d-%b-%Y').date(),
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
                    record1a =  Attributes(**{'egid':x, 'attribute':2, 'attributevalue':i[0]}) #populate attributes table with egid
                    s1.add(record1a)

                    record1b = Attributes(**{'egid':x, 'attribute':5, 'attributevalue':i[12]})   #populate attributes table with isin
                    s1.add(record1b)

                    record2 = Instruments(**{'egid':x,                                            #populate instruments table with egid and startdate 
				                         'instrument_type':1, 
				                         'startdate':datetime.datetime.strptime(i[10],'%d-%b-%Y').date(),
										 'enddate':None
                                      })
                    s1.add(record2)

                    record3 =  Prices(**{                                                          #populate prices table correspoding to the egid
                                         'egid' : x,
                                         'todays_date' : datetime.datetime.strptime(i[10],'%d-%b-%Y').date(),
				         'open_price':i[2],
                                         'high_price':i[3],
                                         'low_price':i[4],
                                         'close_price':i[5],
                                         'volume_traded':i[11]
                                  })
                    s1.add(record3)

		    record4 =  New_entries(**{'egid':x,'instrument_type':1  ,'startdate':datetime.datetime.strptime(i[10],'%d-%b-%Y').date()}) #populate New_entries table with egid and start date
                    s1.add(record4)

					
   	    #s1.commit() #Attempt to commit all the records before checking the delisting

            #code for finding the delisted stocks
            #code for finding the delisted stocks
            #active_stocks_in_db = s1.query(Instruments).filter(Instruments.enddate is None).filter(Instruments.instrument_type==1).all()
            
             #nse_ticker_for_active_stocks_in_db = s1.query(Attributes).select_from(Instruments).join(Attributes.egid==Instruments.egid).filter(Attributes.attribute==2).filter(Instruments.enddate is None).filter(Instruments.instrument_type==1).all()
 
                    nse_ticker_for_active_stocks_in_db = s1.query(Instruments,Attributes).join(Attributes, Instruments.egid==Attributes.egid).filter(Attributes.attribute==2).filter(Instruments.enddate== None).filter(Instruments.instrument_type==1).all()
            

#            y= nse_ticker_for_active_stocks_in_db
 #           y1= y.extend(['\t', ','])
  #          y2=[]
            #u=nse_ticker_for_active_stocks_in_db.__dict__
   #         csvfile="/home/ec2-user/working_scripts/csv.csv"
    #        with open(csvfile, "w") as output:
     #            writer = csv.writer(output, lineterminator='\t')
      #           writer.writerows(y)
 #
  #          parser = genfromtxt(csvfile, delimiter=',', skip_header=0, converters={0: lambda y: str(y),1: lambda y: str(y),2: lambda y: str(y),3: lambda a: str(a),4: lambda y: str(y),5: lambda y: str(y)})
            
   #         y2 = parser.tolist()            

                    deadstock={}             
                    for row in  nse_ticker_for_active_stocks_in_db :        
            #  for row in  nse_ticker_for_active_stocks_in_db : 
                       if (row[1].attributevalue in active_stocks.keys()):
                          continue
                       else:
                          print row[1].attributevalue + " is not present in bhavcopy. Update End date as d-1."
                          deadstock[row[0].egid] = row[1].attributevalue      # d = {1:'one',2:'two,3:'three'} all the keys in [1,2,3]
                          
                #update end date of stock 
                    stock_closing_date = start_date - datetime.timedelta(1) 
                    for key in deadstock:
                       s1.query(Instruments).filter(Instruments.egid==key).filter(Instruments.instrument_type==1).update({Instruments.enddate:stock_closing_date})
                    #   record5 = Dead_stocks(**{'egid':key, 'instrument_type':1, 'enddate':stock_closing_date})
                     #  s1.add(record5) 		
					
	   #         record6 = last_egid(**{'last_generated_egid':x})                                         #recording the last generated egid
            #        s1.add(record6)                                                                           #saving it in the last_egid table
            
	    s1.commit() #Attempt to commit all the records
            #active_stocks_in_db = s1.query(Instruments).filter('enddate' is None).all()
            
	    print  "Last generated egid is "+ str(x)
			
         #   add record in last egid table

        except:
                print "Error while processing files. Rollingback the transactions"
                s1.rollback() #Rollback the changes on error

        print "Total Time elapsed in this session is: " + str(time() - t) + " seconds." #0.091s
	start_date += datetime.timedelta(days=1)		 
	print "Total Time elapsed: " + str(time() - t) + " seconds." #0.091s 
        s1.close()

if __name__ == "__main__":
    main(sys.argv[1:])
