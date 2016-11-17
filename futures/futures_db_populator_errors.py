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
from comprehensive_table_creator1 import Base
from sqlalchemy.ext.automap import automap_base

def Load_Data(year="2013",mon="NOV",dd="06"):
   #SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,
    file_name="fo%s%s%sbhav.csv" % (dd,mon,year)
    if(os.path.isfile(file_name)):
         
        data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda y: str(y),1: lambda z: str(z),2: lambda y: str(y),4: lambda y: str(y),10: lambda x: str(x),14: lambda y: str(y), 15: lambda y: str(y)}, usecols=(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,))
        return data.tolist()
    return []

def populate_attribute(egid, db_session, Attribute_Dao, attribute_type, attribute_value, start_date, end_date):
    db_record = Attribute_Dao(**{'egid':egid, 'attribute':attribute_type, 'attributevalue':attribute_value,'startdate':start_date,'enddate':end_date})
    db_session.add(db_record)


def populate_instruments (egid,db_session,record,instruments_dao,instrument_type,enddate):

    db_record = instruments_dao(**{'egid':egid,
                                 'instrument_type':instrument_type,
                                 'startdate':datetime.datetime.strptime(record[14],'%d-%b-%Y').date(),
                                 'enddate':enddate
                                      })
    db_session.add(db_record)

def populate_futures_price(egid, db_session, record,futures_price_dao,future_series_id,expiration_date):  #Price_Dao
    db_record =  futures_price_dao(**{                                                          #populate prices table correspoding to the egid
                                         'egid' : egid,
                                         'trade_date' : datetime.datetime.strptime(record[14],'%d-%b-%Y').date(),
                                         'future_series_id':future_series_id,
                                         'expiration_date':expiration_date,
                                         'open_price':record[5],
                                         'high_price':record[6],
                                         'low_price':record[7],
                                         'close_price':record[8],
                                         'settle_price':record[9],
                                         'volume_traded':record[11]
                                       })

    db_session.add(db_record)


def populate_new_entries (new_entries_dao,egid,db_session,record,instrument_type,instrument_name):
    db_record =  new_entries_dao(**{'egid':egid,'instrument_type':instrument_type,'instrument_name':instrument_name ,'startdate':datetime.datetime.strptime(record[14],'%d-%b-%Y').date()})
    db_session.add(db_record)

def populate_future_series(future_series_dao,db_session,egid,future_series_name,underlying_id,underlying_name,start_date,enddate):
    db_record =  future_series_dao(**{ 'future_series_id':egid,
                                       'future_series_name':future_series_name,
                                       'underlying_id':underlying_id,
                                       'underlying_name':underlying_name,
                                       'startdate':start_date,
                                       'enddate':enddate,
                                   })
    db_session.add(db_record) 

def populate_futures_contracts(futures_contracts_dao,db_session,egid,record,future_series_id,future_symbol,start_date,expiration_date,last_trade_date):

    db_record = futures_contracts_dao(**{ 'future_id':egid,
                                         'trade_date':datetime.datetime.strptime(record[14],'%d-%b-%Y').date(),
                                         'name':future_symbol,
                                         'future_series_id':future_series_id,
                                         'contract_size':record[10] ,
                                         'list_date':start_date,
                                         'expiration_date':expiration_date,
                                         'last_trade_date': last_trade_date,
                                          })
 
    db_session.add(db_record )

def populate_open_interest(open_interest_dao,db_session,egid,record,future_series_id,expiration_date):
    db_record = open_interest_dao(**{'future_id':egid,
                                     'future_series_id':future_series_id,
                                              'trade_date':datetime.datetime.strptime(record[14],'%d-%b-%Y').date(),
                                              'expiration_date':expiration_date,
                                              'open_interest':record[12],
                                              'change_in_open_interest':record[13]
                                           })
    db_session.add(db_record)

def populate_last_egid (value_of_last_generated_egid,db_session,name_of_class):
    db_record = name_of_class(**{'last_generated_egid':value_of_last_generated_egid})
    db_session.add(db_record)


def main(args):
    
    start_date = datetime.datetime(int(args[0]), int(args[1]), int(args[2]))
    #now = datetime.datetime.now()
    now = start_date + datetime.timedelta(days=4)
    
    t = time()
    
    default_enddate=datetime.datetime.strptime(str('01-01-2100'),"%d-%m-%Y")

    #Create the database
    #engine = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/eg8')
    engine = create_engine('mysql+pymysql://bors_master:uJ!DicDu*7sW@bors-db1.inmu1.eglp.com:3306/borsresdb')
    # If you change it to true - it will diaplay all the equivalent mysql statements 
    engine.echo = False  

    # produce our own MetaData object
    metadata = MetaData()

    metadata.reflect(engine, only=['attributes','instruments','prices','prices_futures' ,'last_generated_egid','new_entries','futureseries','open_interest_table','futures_contract_table'])

    Base = automap_base(metadata=metadata)

    # calling prepare() just sets up mapped classes and relationships.
    Base.prepare(engine, reflect=True)

    Attributes = Base.classes.attributes
    Instruments = Base.classes.instruments
    Prices = Base.classes.prices
    Prices_futures = Base.classes.prices_futures
    last_egid = Base.classes.last_generated_egid
    New_entries = Base.classes.new_entries
    Future_series = Base.classes.futureseries
    Futures_contract_table = Base.classes.futures_contract_table
    Open_interest_table = Base.classes.open_interest_table

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s1 = session()
#    future_id_counter = 0
    egid_counter_query = s1.query(last_egid).first()
    future_id_counter = egid_counter_query.last_generated_egid

 
    while(start_date <= now):
        month = start_date.strftime("%b")
        day = str(start_date.day)
        if (len(day) != 2):
            day = "0" + day
    
        data = Load_Data(start_date.year,month.upper(),day)
        print "Reading Derivatives Bhavcopy for the date" + str(start_date)
        active_future_series = {}
        active_futures = {}
    
        if data :
          try:
              i=() 
              for i in data:
                  if (i[0]== 'FUTIDX'):
                    underlying = i[1] + ' INDEX'
                    underlying_fut = i[1] + ' IDX FUTURE'
                    expiry_date = datetime.datetime.strptime(i[2], '%d-%b-%Y')
                    future_symbol = i[1] + ' ' + expiry_date.strftime("%b").upper() + ' ' + str(expiry_date.year)
                    exist_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut)
                    
                    if ( not(underlying_fut in active_future_series) and (exist_query is not None and exist_query.count() < 1)):
                        #creste new INDEX as the Underlying
                        future_id_counter=future_id_counter+1
                        
                        index_egid = future_id_counter 
                        index_start_date = datetime.datetime.strptime(i[14],'%d-%b-%Y').date()
                        index_end_date = index_start_date

                        populate_attribute(index_egid,s1,Attributes,3,underlying,index_start_date,default_enddate)

                        populate_instruments(index_egid,s1,i,Instruments,4,index_end_date)
                      
                        populate_new_entries (New_entries,index_egid,s1,i,4,underlying) 
                        #Create new Index Future_series
                        future_id_counter=future_id_counter+1
                        future_series_egid = future_id_counter
                        future_series_start_date = datetime.datetime.strptime(i[14],'%d-%b-%Y').date()
                        future_series_end_date = future_series_start_date

                        populate_attribute( future_series_egid,s1,Attributes,3,underlying_fut,future_series_start_date,default_enddate)
                        
                        populate_instruments( future_series_egid,s1,i,Instruments,3,future_series_end_date)

                        underlying_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying).first()
                       
                        populate_future_series(Future_series,s1,future_series_egid,underlying_fut,underlying_query.egid,underlying_query.attributevalue,start_date,future_series_end_date)
                        populate_new_entries (New_entries,future_series_egid,s1,i,3,underlying_fut)


                    active_future_series[underlying_fut] = 1
                    future_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == future_symbol)

                    if (not(future_symbol in active_futures) and (future_query is not None and future_query.count() < 1)):
                         #Create new Index future_contract
                         future_id_counter += 1
                         future_egid = future_id_counter
                         
                         future_contract_start_date = datetime.datetime.strptime(i[14],'%d-%b-%Y').date()
                         future_contract_end_date = future_contract_start_date
                          
                         populate_attribute(future_egid,s1,Attributes,3,future_symbol, future_contract_start_date ,default_enddate)

                         populate_instruments(future_egid,s1,i,Instruments,2,future_contract_end_date)

                         underlying_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut).first() 

                         populate_futures_contracts(Futures_contract_table,s1,future_egid,i,underlying_query.egid,future_symbol,start_date,expiry_date,expiry_date)

                         populate_open_interest(Open_interest_table,s1,future_egid,i,underlying_query.egid,expiry_date)
                            
                         populate_futures_price(future_egid,s1,i,Prices_futures,underlying_query.egid,expiry_date)
                         
                         populate_new_entries (New_entries,future_egid,s1,i,2,future_symbol)
                        
                         s1.query(Future_series).filter(Future_series.future_series_id==underlying_query.egid).update({Future_series.enddate:start_date})
         

                    else:
                         #update database entries for existing Index future_contracts
                         future_egid = future_query.first().egid

                         active_futures[future_symbol] = 1

                         #populate_futures_price(future_egid,s1,i,Prices_futures)

                         #populate_open_interest(Open_interest_table,s1,future_egid,i)

                         underlying_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut).first()          
                         index_query = s1.query(Attributes).filter(Attributes.attribute==3).filter(Attributes.attributevalue == underlying).first()                         
                         populate_futures_contracts(Futures_contract_table,s1,future_egid,i,underlying_query.egid,future_symbol,start_date,expiry_date,expiry_date)
                         populate_open_interest(Open_interest_table,s1,future_egid,i,underlying_query.egid,expiry_date)

                         populate_futures_price(future_egid,s1,i,Prices_futures,underlying_query.egid,expiry_date)
         
                         s1.query(Instruments).filter(Instruments.egid==future_egid).filter(Instruments.instrument_type==2).update({Instruments.enddate:start_date})
                         s1.query(Instruments).filter(Instruments.egid==underlying_query.egid).filter(Instruments.instrument_type==3).update({Instruments.enddate:start_date})
                         s1.query(Instruments).filter(Instruments.egid==index_query.egid).filter(Instruments.instrument_type==4).update({Instruments.enddate:start_date})
                         s1.query(Future_series).filter(Future_series.future_series_id==underlying_query.egid).update({Future_series.enddate:start_date})


                  elif (i[0]=='FUTSTK'):
                  
                #if Underlying Future already exists, populate price table correspoding to the egid and current date
                     underlying_fut = i[1] + ' FUTURE'
                     expiry_date = datetime.datetime.strptime(i[2], '%d-%b-%Y')
                     future_symbol = i[1] + ' ' + expiry_date.strftime("%b").upper() + ' ' + str(expiry_date.year)
                     exist_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut)
                #if ((s1.query(exist_query.exists()).scalar() is None) or ((s1.query(exist_query.exists()).scalar() is not None) and (s1.query(exist_query.exists()).scalar()==False))):
                     if ( not(underlying_fut in active_future_series) and (exist_query is not None and exist_query.count() < 1)):
                        #create new Stock future_series   
                        future_id_counter=future_id_counter+1
                        future_series_egid = future_id_counter 
                        populate_attribute(future_series_egid,s1,Attributes,3,underlying_fut,datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),default_enddate)

                        populate_instruments(future_series_egid,s1,i,Instruments,3,datetime.datetime.strptime(i[14],'%d-%b-%Y').date())

                        underlying_query = s1.query(Attributes).filter(Attributes.attribute == 2).filter(Attributes.attributevalue == i[1]).first()

                        populate_future_series(Future_series,s1,future_series_egid,underlying_fut,underlying_query.egid,underlying_query.attributevalue,start_date,default_enddate)
                        populate_new_entries (New_entries,future_series_egid,s1,i,3,underlying_fut)
                      
                     active_future_series[underlying_fut] = 1
                     future_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == future_symbol)
                
                     if (not(future_symbol in active_futures) and (future_query is not None and future_query.count() < 1)):
                         # Create new Stock future_contracts
                         future_id_counter += 1
                         future_egid = future_id_counter

                         populate_attribute(future_egid,s1,Attributes,3,future_symbol,datetime.datetime.strptime(i[14],'%d-%b-%Y').date(),default_enddate)

                         populate_instruments(future_egid,s1,i,Instruments,2,datetime.datetime.strptime(i[14],'%d-%b-%Y').date())

                        # populate_futures_price(future_egid,s1,i,Prices_futures)

                         underlying_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut).first()
                         populate_futures_contracts(Futures_contract_table,s1,future_egid,i,underlying_query.egid,future_symbol,start_date,expiry_date,expiry_date)          
                         populate_open_interest(Open_interest_table,s1,future_egid,i,underlying_query.egid,expiry_date)

                         populate_futures_price(future_egid,s1,i,Prices_futures,underlying_query.egid,expiry_date)
                         #populate_open_interest(Open_interest_table,s1,future_egid,i)
                         
                         populate_new_entries (New_entries,future_egid,s1,i,2,future_symbol)
                         
                         s1.query(Future_series).filter(Future_series.future_series_id==underlying_query.egid).update({Future_series.enddate:start_date})



                     else:
                         # update the database tables with data for existing Stock Future_contracts
                         future_egid = future_query.first().egid
                         
                         active_futures[future_symbol] = 1
 
                         #populate_futures_price(future_egid,s1,i,Prices_futures)

#                         s1.query(Instruments).filter(Instruments.egid==existing_egid.egid).filter(Instruments.instrument_type==1).update({Instruments.enddate:start_date})

                         underlying_query = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == underlying_fut).first()
                         expiration_date_query = s1.query(Futures_contract_table).filter(Futures_contract_table.future_id==future_egid).first()
                         if (expiration_date_query.expiration_date == expiry_date):
                           populate_futures_contracts(Futures_contract_table,s1,future_egid,i,underlying_query.egid,future_symbol,start_date,expiry_date,expiry_date)
                           populate_open_interest(Open_interest_table,s1,future_egid,i,underlying_query.egid,expiry_date)
                           populate_futures_price(future_egid,s1,i,Prices_futures,underlying_query.egid,expiry_date)
                           continue
                         else: 
                           populate_futures_contracts(Futures_contract_table,s1,future_egid,i,underlying_query.egid,future_symbol,start_date,expiry_date,expiry_date)
                           populate_open_interest(Open_interest_table,s1,future_egid,i,underlying_query.egid,expiry_date)
                           populate_futures_price(future_egid,s1,i,Prices_futures,underlying_query.egid,expiry_date)

                           future_query1 = s1.query(Attributes).filter(Attributes.attribute == 3).filter(Attributes.attributevalue == future_symbol).first()
                           special_expiry_future_name = str(future_query1.attributevalue) 
                           file = open("unusual_expiry_dates.csv", "a")
                           writer = csv.writer(file, delimiter=',',)
                           writer.writerow([ future_egid,special_expiry_future_name,str(expiration_date_query.expiration_date),str(expiry_date)])
                           file.close()
                            
#                         populate_open_interest(Open_interest_table,s1,future_egid,i)
                         
                         s1.query(Instruments).filter(Instruments.egid==future_egid).filter(Instruments.instrument_type==2).update({Instruments.enddate:start_date})
                         s1.query(Instruments).filter(Instruments.egid==underlying_query.egid).filter(Instruments.instrument_type==3).update({Instruments.enddate:start_date})
                         s1.query(Future_series).filter(Future_series.future_series_id==underlying_query.egid).update({Future_series.enddate:start_date})

                  else :
                    file = open("Options.csv", "a")
                    writer = csv.writer(file, delimiter=',',)
                    writer.writerow(["Options and VIX futures are not to be included." ])
                    file.close()
                    continue
                    #print "Options and IVX futures are not to be included"
                    

                  s1.query(last_egid).filter(last_egid.serial_no==1).update({last_egid.last_generated_egid:future_id_counter})
 
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
