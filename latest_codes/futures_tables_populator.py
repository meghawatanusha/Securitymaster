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
    file_name="fo%s%s%sbhav.csv" % (dd,mon,year)
    if(os.path.isfile(file_name)): 
        data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda y: str(y),1: lambda z: str(z),10: lambda x: str(x),12: lambda a: str(a)})
        return data.tolist()
    return []


def main(args):
    
    start_date = datetime.datetime(int(args[0]), int(args[1]), int(args[2]))
    #now = datetime.datetime.now()
    now = start_date + datetime.timedelta(days=1)
    
    t = time()

    #Create the database
    engine = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/eg2')

    # If you change it to true - it will diaplay all the equivalent mysql statements 
    engine.echo = False  

    # produce our own MetaData object
    metadata = MetaData()

    # we can reflect it ourselves from a database, using options
    # such as 'only' to limit what tables we look at
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
    Future_series = Base.classes.future_series1
    Futures_contract_table=Base.classes.futures_contract_table1
    Open_interest_table=Base.classes.open_interest_table1

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s1 = session()
    future_id_counter = 0
    #egid_counter = s1.query(last_generated_egid).first().last_generated_egid
   
    while(start_date <= now):
        month = start_date.strftime("%b")
        day = str(start_date.day)
        if (len(day) != 2):
            day = "0" + day
    
        data = Load_Data(start_date.year,month.upper(),day)
        active_futures = {}
    
    #SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,
        try: 
            for i in data:
                if (i[0] != 'FUTSTK' or 'FUTIDX'):
                    continue
                
                #put all the active stocks of today in a dictionary
                active_futures[i[1]] = 1
                
                #if equity already exists, populate price table correspoding to the egid and current date
                exist_query = s1.query(Attributes).filter(Attributes.attribute == 2).filter(Attributes.attributevalue == i[1])
                if (s1.query(exist_query.exists()).scalar()):                       
                # get egid of the existing stock corresponding to the nseticker
                    existing_egid = s1.query(Attributes).filter(Attributes.attribute == 2).filter(Attributes.attributevalue == i[1]).first()
                
                    if not existing_egid:
                        print "Error while fetching egid"
                        return 
                         
                         
                    record3 =  Prices(**{
                                         'egid' : existing_egid.egid,
                                         'todays_date' : datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                         'open_price':i[5],
                                         'high_price':i[6],
                                         'low_price':i[7],
                                         'close_price':i[8],
                                         'settle_price':i[9]
                                         'volume_traded':i[11]
                                  })
                    s1.add(record3)
                    
                    record4 =  Open_interest_table(**{'egid':future_id_counter,                                             
                                         'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                         'open_interest':i[12],
                                         'change_in_open_interest':i[13]
                                      })
                    s1.add(record4)

                    
                    
                # if equity does not exist already in the attributes table, generate egid, and populate all the three tables - attri,instru,prices.
                else:
                    
                    #generate unique egid for the new stock
                    future_id_counter=future_id_counter+1   
                    
                    #populate attributes table with egid
                    record1a =  Attributes(**{'egid':future_id_counter, 'attribute':2, 'attributevalue':i[1]}) 
                    s1.add(record1a)

                    #populate attributes table with isin
                    #record1b = Attributes(**{'egid':future_id_counter, 'attribute':5, 'attributevalue':i[12]})   
                    #s1.add(record1b)

                    #populate instruments table with egid and startdate
                    record2 = Instruments(**{'egid':future_id_counter,                                             
                                         'instrument_type':3, 
                                         'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                         'enddate':None
                                      })
                    s1.add(record2)

                    #populate prices table correspoding to the egid
                    record3 =  Prices(**{                                                          
                                         'egid' : future_id_counter,
                                         'todays_date' : datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                         'open_price':i[5],
                                         'high_price':i[6],
                                         'low_price':i[7],
                                         'close_price':i[8],
                                         'volume_traded':i[11]
                                  })
                    s1.add(record3)
                    
                    record4 =  Open_interest_table(**{'egid':future_id_counter,                                             
                                         'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                         'open_interest':i[12],
                                         'change_in_open_interest':i[13]
                                      })
                    s1.add(record4)


                    
                    #populate New_entries table with egid and start date
                    record5 =  New_entries(**{'egid':future_id_counter, 'instrument_type':3, 'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date()}) 
                    s1.add(record5)

            #code for finding the delisted stocks
            
            #active_stocks_in_db = s1.query(Instruments).filter(Instruments.enddate is None).filter(Instruments.instrument_type==1).all()
                    nseticker_for_active_futures_in_db = s1.query(Instruments,Attributes).join(Instruments.egid==Attributes.egid).filter(Attributes.attribute==2).filter(Instruments.instrument_type==3,Instruments.enddate== None).all()
            #nse_ticker_for_active_stocks_in_db=s1.query(Instruments).select_from(Attributes).join(Instruments.egid==Attributes.egid).filter(Attributes.attribute==2).filter(Instruments.enddate is None).filter(Instruments.instrument_type==1).all()
            
            
                    deadfuture={}
                    
                    for row in nse_ticker_for_active_futures_in_db: 
                    if row[1].attvibutevalue in active_futures[]:
                       continue
                    else:
                       print row[1].attvibutevalue " is not present in the CSV file. Update End date as d-1." 
                       deadfuture[row[0].egid] = row[1].attributevalue      # d = {1:'one',2:'two,3:'three'} all the keys in [1,2,3]
                        
                #update end date of stock 
                    future_closing_date = start_date - datetime.timedelta(1) 
                    for key in deadstock:
                       s1.query(Instruments).filter(egid==key).filter(instrument_type==1).update({Instruments.enddate:future_closing_date})
                            
                    
        #record6 = last_generated_egid(**{'last_generated_egid':future_id_counter})                                         #recording the last generated egid
            
        #s1.add(record6)                                                                           #saving it in the last_egid table
            
        s1.commit() #Attempt to commit all the records
                
        print  "Last generated egid is" 'future_id_counter.'
            
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
