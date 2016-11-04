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
from create_tables import Base
from sqlalchemy.ext.automap import automap_base

def Load_Data(year="2013",mon="NOV",dd="06"):
   #SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,
    file_name="fo%s%s%sbhav.csv" % (dd,mon,year)
    if(os.path.isfile(file_name)): 
        data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda y: str(y),1: lambda z: str(z),2: lambda y: str(y),4: lambda y: str(y),10: lambda x: str(x),14: lambda y: str(y), 15: lambda y: str(y)})
        return data.tolist()
    return []


def main(args):
    
    start_date = datetime.datetime(int(args[0]), int(args[1]), int(args[2]))
    #now = datetime.datetime.now()
    now = start_date + datetime.timedelta(days=2)
    
    t = time()

    #Create the database
    engine = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/eg4')

    # If you change it to true - it will diaplay all the equivalent mysql statements 
    engine.echo = False  

    # produce our own MetaData object
    metadata = MetaData()

    # we can reflect it ourselves from a database, using options
    # such as 'only' to limit what tables we look at
    metadata.reflect(engine, only=['attributes3','instruments3','prices3','last_generated_egid3','new_entries3','dead_stocks3','futureseries3','open_interest_table3','futures_contract_table3'])

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
    last_generated_egid=Base.classes.last_generated_egid3
    New_entries = Base.classes.new_entries3
    Dead_stocks = Base.classes.dead_stocks3
    Future_series = Base.classes.futureseries3
    Futures_contract_table=Base.classes.futures_contract_table3
    Open_interest_table=Base.classes.open_interest_table3

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
        active_future_series = {}
        active_futures = {}
    
    #SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,
        try: 
            for i in data:
                if ((i[0] != 'FUTSTK') and (i[0] != 'FUTIDX')):
                    continue
                
                #if Underlying Future already exists, populate price table correspoding to the egid and current date
                underlying_fut = i[1] + ' FUTURE'
                expiry_date = datetime.datetime.strptime(i[2], '%d-%b-%Y')
                future_symbol = i[1] + ' ' + expiry_date.strftime("%b").upper() + ' ' + str(expiry_date.year)
                exist_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut)
                #if ((s1.query(exist_query.exists()).scalar() is None) or ((s1.query(exist_query.exists()).scalar() is not None) and (s1.query(exist_query.exists()).scalar()==False))):
                if ( not(underlying_fut in active_future_series) and (exist_query is not None and exist_query.count() < 1)):   
                    future_id_counter=future_id_counter+1

                    #populate attributes table with egid
                    record1a =  Attributes(**{'egid':future_id_counter, 'attribute':3, 'attributevalue':underlying_fut})
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
                # get egid of the existing stock corresponding to the nseticker
                    #existing_egid = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut).first()
                active_future_series[underlying_fut] = 1
                future_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == future_symbol)
                
                if (not(future_symbol in active_futures) and (future_query is not None and future_query.count() < 1)):
                    future_id_counter += 1
                    future_egid = future_id_counter
                    future_attr =  Attributes(**{'egid':future_egid, 'attribute':3, 'attributevalue':future_symbol})
                    s1.add(future_attr)
                    fut_instrument = Instruments(**{'egid':future_egid,
                                         'instrument_type':2,
                                         'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                         'enddate':None
                                      })
                    s1.add(fut_instrument)
                else:
                    future_egid = future_query.first().egid

                         
                active_futures[future_symbol] = 1

                record3 =  Prices(**{
                                         'egid' : future_egid,
                                         'todays_date' : datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                         'open_price':i[5],
                                         'high_price':i[6],
                                         'low_price':i[7],
                                         'close_price':i[8],
                                         'settle_price':i[9],
                                         'volume_traded':i[11]
                                  })
                s1.add(record3)
                    
                record4 =  Open_interest_table(**{'future_id':future_egid,                                             
                                         'todays_date':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                         'open_interest':i[12],
                                         'change_in_open_interest':i[13]
                                      })
                s1.add(record4)
            s1.commit()            
                    #populate New_entries table with egid and start date
                    #record5 =  New_entries(**{'egid':future_id_counter, 'instrument_type':3, 'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date()}) 
                    #s1.add(record5)

            #code for finding the delisted stocks
            
            #active_stocks_in_db = s1.query(Instruments).filter(Instruments.enddate is None).filter(Instruments.instrument_type==1).all()
        #nseticker_for_active_futures_in_db = s1.query(Instruments,Attributes).join(Instruments.egid==Attributes.egid).filter(Attributes.attribute==3).filter(Instruments.instrument_type==2).all()
            #nse_ticker_for_active_stocks_in_db=s1.query(Instruments).select_from(Attributes).join(Instruments.egid==Attributes.egid).filter(Attributes.attribute==2).filter(Instruments.enddate is None).filter(Instruments.instrument_type==1).all()
            
        #deadfuture={}
                
        #for row in nse_ticker_for_active_futures_in_db: 
            #if row[1].attributevalue in active_futures.keys():
            #    continue
           # else:
          #      print row[1].attributevalue " is not present in the CSV file. Update End date as d-1." 
         #       deadfuture[row[0].egid] = row[1].attributevalue      # d = {1:'one',2:'two,3:'three'} all the keys in [1,2,3]
                        
                #update end date of stock 
        #future_closing_date = start_date - datetime.timedelta(1) 
        #for key in deadstock:
         #   s1.query(Instruments).filter(egid==key).filter(instrument_type==2).update({Instruments.enddate:future_closing_date})


        #nseticker_for_active_future_series_in_db = s1.query(Instruments,Attributes).join(Instruments.egid==Attributes.egid).filter(Attributes.attribute==3).filter(Instruments.instrument_type==3,Instruments.enddate== None).all()
                 
        #dead_future_series={}

        #for row in nse_ticker_for_active_futures_series_in_db:
           # if row[1].attributevalue in active_future_series.keys():
           #     continue
          #  else:
            #    print row[1].attributevalue " is not present in the CSV file. Update End date as d-1."
         #       dead_future_series[row[0].egid] = row[1].attributevalue      # d = {1:'one',2:'two,3:'three'} all the keys in [1,2,3]

                #update end date of stock
        #future_closing_date = start_date - datetime.timedelta(1)
        #for key in dead_future_series.keys():
        #    s1.query(Instruments).filter(egid==key).filter(instrument_type==3).update({Instruments.enddate:future_closing_date})

                            
                    
        #record6 = last_generated_egid(**{'last_generated_egid':future_id_counter})                                         #recording the last generated egid
            
        #s1.add(record6)                                                                           #saving it in the last_egid table
            
    #    s1.commit() #Attempt to commit all the records
                
       # print  "Last generated egid is" 'future_id_counter.'
            
         #   add record in last egid tablei
        #s1.commit()

        except:
                print "Error while processing files. Rollingback the transactions"
                s1.rollback() #Rollback the changes on error

        print "Total Time elapsed in this session is: " + str(time() - t) + " seconds." #0.091s
        
        start_date += datetime.timedelta(days=1)
                 
             
             
        print "Total Time elapsed: " + str(time() - t) + " seconds." #0.091s
    
        s1.close()

__name__ == "__main__"
main(sys.argv[1:])
