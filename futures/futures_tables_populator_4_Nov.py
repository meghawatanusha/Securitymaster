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
    
    default_enddate=datetime.datetime.strptime(str('01-01-2100'),"%d-%m-%Y")

    #Create the database
    #engine = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/eg8')
    engine = create_engine('mysql+pymysql://bors_master:uJ!DicDu*7sW@bors-db1.inmu1.eglp.com:3306/futures_new')
    # If you change it to true - it will diaplay all the equivalent mysql statements 
    engine.echo = False  

    # produce our own MetaData object
    metadata = MetaData()

    # we can reflect it ourselves from a database, using options
    # such as 'only' to limit what tables we look at
    metadata.reflect(engine, only=['attributes','instruments','prices','last_generated_egid','futureseries','open_interest_table','futures_contract_table'])

#    # ... or just define our own Table objects with it (or combine both)
 #   Table('user_order', metadata,
 #               Column('id', Integer, primary_key=True),
 #               Column('user_id', ForeignKey('user.id'))
 #           )
 # we can then produce a set of mappings from this MetaData.
    Base = automap_base(metadata=metadata)

    # calling prepare() just sets up mapped classes and relationships.
    Base.prepare(engine, reflect=True)

    Attributes = Base.classes.attributes
    Instruments = Base.classes.instruments
    Prices = Base.classes.prices
    last_generated_egid=Base.classes.last_generated_egid
    #New_entries = Base.classes.new_entries
    Future_series = Base.classes.futureseries
    Futures_contract_table=Base.classes.futures_contract_table
    Open_interest_table=Base.classes.open_interest_table

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s1 = session()
#    future_id_counter = 0
    egid_counter_query = s1.query(last_generated_egid).first()
    future_id_counter = egid_counter_query.last_generated_egid

 
    while(start_date <= now):
        month = start_date.strftime("%b")
        day = str(start_date.day)
        if (len(day) != 2):
            day = "0" + day
    
        data = Load_Data(start_date.year,month.upper(),day)
        active_future_series = {}
        active_futures = {}
    
        if data :
          try: 
              for i in data:
                  if (i[0]= 'FUTIDX'):
                    underlying_fut = i[1] + ' FUTURE'
                    expiry_date = datetime.datetime.strptime(i[2], '%d-%b-%Y')
                    future_symbol = i[1] + ' ' + expiry_date.strftime("%b").upper() + ' ' + str(expiry_date.year)
                    exist_query = s1.query(Attributes).filter(Attributes.attribute == 4).filter(Attributes.attributevalue == underlying_fut)
                    
                     if ( not(underlying_fut in active_future_series) and (exist_query is not None and exist_query.count() < 1)):
                        future_id_counter=future_id_counter+1

                        record1a =  Attributes(**{'egid':future_id_counter, 'attribute':3, 'attributevalue':underlying_fut, 'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date() , 'enddate':default_enddate })
                        s1.add(record1a)


                        record2 = Instruments(**{'egid':future_id_counter,
                                             'instrument_type':4,
                                             'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                             'enddate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date()
                                          })
                        s1.add(record2)

                       # underlying_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == i[1]).first()
                        record3 = Future_series(**{ 'future_series_id':future_id_counter ,
                                                    'future_series_name':underlying_fut,
                                                    'underlying_id':None,
                                                    'underlying_name':None,
                                                    'startdate':start_date,
                                                    'enddate': datetime.datetime.strptime(str('01-01-2100'),'%d-%m-%Y')
                                                })
                        s1.add(record3)


                     active_future_series[underlying_fut] = 1
                     future_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == future_symbol)

                     if (not(future_symbol in active_futures) and (future_query is not None and future_query.count() < 1)):
                         future_id_counter += 1
                         future_egid = future_id_counter
                         future_attr =  Attributes(**{'egid':future_egid, 'attribute':3, 'attributevalue':future_symbol,'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date() , 'enddate':datetime.datetime.strptime(str('01-01-2100'),'%d-%m-%Y')})
                         s1.add(future_attr)
                         fut_instrument = Instruments(**{'egid':future_egid,
                                              'instrument_type':2,
                                              'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                              'enddate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date()
                                           })
                         s1.add(fut_instrument)

                         record3 =  Prices(**{
                                              'egid' : future_egid,
                                              'trade_date' : datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                              'open_price':i[5],
                                              'high_price':i[6],
                                              'low_price':i[7],
                                              'close_price':i[8],
                                              'settle_price':i[9],
                                              'volume_traded':i[11]
                                       })
                         s1.add (record3)

                         underlying_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut).first()          
                         fut_contract = Futures_contract_table(**{ 'future_id':future_egid,
                                             'nseticker':i[1],
                                             'future_series_id':underlying_query.egid,
                                             'contract_size':i[10] ,
                                             'list_date':start_date,
                                             'expiration_date':default_enddate,
                                          })

                         s1.add(fut_contract)

                         record4 =  Open_interest_table(**{'future_id':future_egid,
                                              'trade_date':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                              'open_interest':i[12],
                                              'change_in_open_interest':i[13]
                                           })
                         s1.add(record4)


                     else:
                         future_egid = future_query.first().egid

                         active_futures[future_symbol] = 1

                         record3 =  Prices(**{
                                               'egid' : future_egid,
                                               'trade_date' : datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                               'open_price':i[5],
                                               'high_price':i[6],
                                               'low_price':i[7],
                                               'close_price':i[8],
                                               'settle_price':i[9],
                                               'volume_traded':i[11]
                                        })
                         s1.add (record3)

                         record4 =  Open_interest_table(**{'future_id':future_egid,
                                           'trade_date':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                           'open_interest':i[12],
                                           'change_in_open_interest':i[13]
                                        })
                         s1.add(record4)

                         underlying_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut).first()          
                         fut_contract = Futures_contract_table(**{ 'future_id':future_egid,
                                          'nseticker':i[1],
                                          'future_series_id':underlying_query.egid,
                                          'contract_size':i[10] ,
                                          'list_date':start_date,
                                          'expiration_date':default_enddate,
                                       })

                         s1.add(fut_contract)


                  else if (i[0]='FUTIVX'):


                  else  if ((i[0] = 'FUTSTK')):
                  
                
                #if Underlying Future already exists, populate price table correspoding to the egid and current date
                     underlying_fut = i[1] + ' FUTURE'
                     expiry_date = datetime.datetime.strptime(i[2], '%d-%b-%Y')
                     future_symbol = i[1] + ' ' + expiry_date.strftime("%b").upper() + ' ' + str(expiry_date.year)
                     exist_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut)
                #if ((s1.query(exist_query.exists()).scalar() is None) or ((s1.query(exist_query.exists()).scalar() is not None) and (s1.query(exist_query.exists()).scalar()==False))):
                     if ( not(underlying_fut in active_future_series) and (exist_query is not None and exist_query.count() < 1)):   
                        future_id_counter=future_id_counter+1

                        record1a =  Attributes(**{'egid':future_id_counter, 'attribute':3, 'attributevalue':underlying_fut, 'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date() , 'enddate':default_enddate })
                        s1.add(record1a)


                        record2 = Instruments(**{'egid':future_id_counter,
                                             'instrument_type':3,
                                             'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                             'enddate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date()
                                          })
                        s1.add(record2)

                        underlying_query = s1.query(Attributes).filter(Attributes.attribute == 2).filter(Attributes.attributevalue == i[1]).first()
                        record3 = Future_series(**{ 'future_series_id':future_id_counter ,
                                                    'future_series_name':underlying_fut,
                                                    'underlying_id':underlying_query.egid,
                                                    'underlying_name':underlying_query.attributevalue,     
                                                    'startdate':start_date,
                                                    'enddate': datetime.datetime.strptime(str('01-01-2100'),'%d-%m-%Y')
                                                })
                        s1.add(record3)
   
                      
                     active_future_series[underlying_fut] = 1
                     future_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == future_symbol)
                
                     if (not(future_symbol in active_futures) and (future_query is not None and future_query.count() < 1)):
                         future_id_counter += 1
                         future_egid = future_id_counter
                         future_attr =  Attributes(**{'egid':future_egid, 'attribute':3, 'attributevalue':future_symbol,'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date() , 'enddate':datetime.datetime.strptime(str('01-01-2100'),'%d-%m-%Y')})
                         s1.add(future_attr)
                         fut_instrument = Instruments(**{'egid':future_egid,
                                              'instrument_type':2,
                                              'startdate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                              'enddate':datetime.datetime.strptime(i[14],'%d-%b-%Y').date()
                                           })
                         s1.add(fut_instrument)
                     
                         record3 =  Prices(**{
                                              'egid' : future_egid,
                                              'trade_date' : datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                              'open_price':i[5],
                                              'high_price':i[6],
                                              'low_price':i[7],
                                              'close_price':i[8],
                                              'settle_price':i[9],
                                              'volume_traded':i[11]
                                       })
                         s1.add (record3)
                      
                         underlying_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut).first()                      
                         fut_contract = Futures_contract_table(**{ 'future_id':future_egid,
                                             'nseticker':i[1],
                                             'future_series_id':underlying_query.egid,
                                             'contract_size':i[10] ,
                                             'list_date':start_date,
                                             'expiration_date':default_enddate,
                                          })
 
                         s1.add(fut_contract)
                         
                         record4 =  Open_interest_table(**{'future_id':future_egid,
                                              'trade_date':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                              'open_interest':i[12],
                                              'change_in_open_interest':i[13]
                                           })
                         s1.add(record4)


                     else:
                         future_egid = future_query.first().egid
                         
                         active_futures[future_symbol] = 1
    
                         record3 =  Prices(**{
                                               'egid' : future_egid,
                                               'trade_date' : datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                               'open_price':i[5],
                                               'high_price':i[6],
                                               'low_price':i[7],
                                               'close_price':i[8],
                                               'settle_price':i[9],
                                               'volume_traded':i[11]
                                        }) 
                         s1.add (record3)
                      
                         record4 =  Open_interest_table(**{'future_id':future_egid,                                             
                                           'trade_date':datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),
                                           'open_interest':i[12],
                                           'change_in_open_interest':i[13]
                                        })
                         s1.add(record4)
                  
                         underlying_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut).first()                      
                         fut_contract = Futures_contract_table(**{ 'future_id':future_egid,
                                          'nseticker':i[1],
                                          'future_series_id':underlying_query.egid,
                                          'contract_size':i[10] ,
                                          'list_date':start_date,
                                          'expiration_date':default_enddate,
                                       })

                         s1.add(fut_contract)
    
                      
                 s1.commit()
                      #populate New_entries table with egid and start date

          except Exception,e:
                  print "Error while processing files. Rollingback the transactions"
                  print str(e)
                  s1.rollback() #Rollback the changes on error

          print "Total Time elapsed in this session is: " + str(time() - t) + " seconds." #0.091s
          
          start_date += datetime.timedelta(days=1)
             
          print "Total Time elapsed: " + str(time() - t) + " seconds." #0.091s
    
          s1.close()
        
        else :
            print "the future bhavcopy for the date " + str(start_date) + " is not present."
            start_date += datetime.timedelta(days=1)

__name__ == "__main__"
main(sys.argv[1:])
