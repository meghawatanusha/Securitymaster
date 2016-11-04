import os
import sys
import csv
import sqlalchemy
from sqlalchemy import *
import numpy
from numpy import genfromtxt
import numpy as np
from time import time
import datetime
from sqlalchemy import Column, Integer, Float, Date, String, BIGINT
from sqlalchemy.dialects.mysql import DATETIME, DOUBLE, INTEGER, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey
from create_tables4 import Base
from sqlalchemy.ext.automap import automap_base

def Load_Data(year="2013",mon="NOV",dd="06"):
   #SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,
    file_name="cm%s%s%sbhav.csv" % (dd,mon,year)
    if(os.path.isfile(file_name)):
      csvfile = open(file_name, 'r')
      reader = csv.reader(csvfile, delimiter = ',')
      num_cols = len(next(reader))
      
      if num_cols < 13 :
        data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda y: str(y),1: lambda z: str(z),10: lambda x: str(x)})
        return data.tolist()
      else:
        data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda y: str(y),1: lambda z: str(z),10: lambda x: str(x),12: lambda a: str(a)})
        return data.tolist()
    return []

def populate_price(egid, db_session, record,name_of_class):  #Price_Dao
     if len(record)>12 :
        db_record =  name_of_class(**{                                                          #populate prices table correspoding to the egid
                                         'egid' :egid,
                                         'trade_date' : datetime.datetime.strptime(record[10],'%d-%b-%Y').date(),
                                         'open_price':record[2],
                                         'high_price':record[3],
                                         'low_price':record[4],
                                         'close_price':record[5],
                                         'volume_traded':record[11]
                                  })
        db_session.add(db_record)
     else:
        db_record =  name_of_class(**{                                                          #populate prices table correspoding to the egid
                                         'egid' :egid,
                                         'trade_date' : datetime.datetime.strptime(record[10],'%d-%b-%Y').date(),
                                         'open_price':record[2],
                                         'high_price':record[3],
                                         'low_price':record[4],
                                         'close_price':record[5],
                                         'volume_traded':None
                                  })
        db_session.add(db_record)

def populate_attribute(egid, db_session, Attribute_Dao, attribute_type, attribute_value, start_date, end_date):
    db_record = Attribute_Dao(**{'egid':egid, 'attribute':attribute_type, 'attributevalue':attribute_value,'startdate':start_date,'enddate':end_date})
    db_session.add(db_record)


def populate_instruments (egid,db_session,record,name_of_class):
    
    db_record = name_of_class(**{'egid':egid,
                                 'instrument_type':1,
                                 'startdate':datetime.datetime.strptime(record[10],'%d-%b-%Y').date(),
                                 'enddate':None
                                      })
    db_session.add(db_record) 

def populate_new_entries (egid,db_session,record,name_of_class):
     db_record =  name_of_class(**{'egid':egid,'instrument_type':1,'instrument_name':record[0]  ,'startdate':datetime.datetime.strptime(record[10],'%d-%b-%Y').date()})
     db_session.add(db_record)

def populate_dead_stocks (egid, db_session,name_of_class,instrument_name,date_of_end ):
     db_record = name_of_class(**{'egid':egid,
                                'instrument_type':1,
                                'instrument_name':instrument_name,
                                'enddate':date_of_end,
                                             })

     db_session.add(db_record)

def create_dictionary_for_symbolchange (name_of_symbolchange_file, dict_name):
     reader = csv.reader(open(name_of_symbolchange_file))

     row_in_csv=()
     for row_in_csv in reader:
       key = row_in_csv[1]
       if key in dict_name:
          pass
       dict_name[key] = row_in_csv[2:]
 
def populate_last_egid (value_of_last_generated_egid,db_session,name_of_class):
    db_record = name_of_class(**{'last_generated_egid':value_of_last_generated_egid})
    db_session.add(db_record)



def main(args):
    
    start_date = datetime.datetime(int(args[0]), int(args[1]), int(args[2]))
    #now = datetime.datetime.now()
    now = start_date + datetime.timedelta(days=8)
	
    t = time()
    
    symbolchange={}
    create_dictionary_for_symbolchange ('symbolchange.csv', symbolchange)

    engine = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/eg8')

    #engine = create_engine('mysql+pymysql://bors_master:uJ!DicDu*7sW@bors-db1.inmu1.eglp.com:3306/test')

    engine.echo = False

    metadata = MetaData()

    metadata.reflect(engine, only=['attributes3','instruments3','prices3','last_generated_egid3','new_entries3',])

    Base = automap_base(metadata=metadata)

    Base.prepare(engine, reflect=True)

    Attributes = Base.classes.attributes3
    Instruments = Base.classes.instruments3
    Prices = Base.classes.prices3
    last_egid=Base.classes.last_generated_egid3
    New_entries = Base.classes.new_entries3
    #Dead_stocks = Base.classes.dead_stocks3

    session = sessionmaker()
    session.configure(bind=engine)
    s1 = session() 

    egid_query= s1.query(last_egid).filter(last_egid.serial_no==1).first()
    egid_counter = egid_query.last_generated_egid
   
    while(start_date <= now):
        month = start_date.strftime("%b")
        day = str(start_date.day)
        if (len(day) != 2):
            day = "0" + day

        data = Load_Data(start_date.year,month.upper(),day)
        print "Reading Bhavcopy for the date :  " + str(start_date)

        active_stocks = {}
        
        if data :
          try:
              i=() 
              for i in data:
                  if (i[1] != 'EQ'):
                      continue
			
                  active_stocks[i[0]] = 1

                  exist_query = s1.query(Attributes).filter(Attributes.attribute == 2).filter(Attributes.attributevalue == i[0])
                  if (s1.query(exist_query.exists()).scalar()): 
                      existing_egid = s1.query(Attributes).filter(Attributes.attribute == 2).filter(Attributes.attributevalue == i[0]).first()
                            
                      if not existing_egid:
                          print "Error while fetching egid"
                          return 

                      populate_price(existing_egid.egid,s1,i,Prices )
                  else:
                      egid_counter = egid_counter + 1
                      if i[0] in symbolchange :
                         attr_of_new_ticker = symbolchange.get(i[0])
                         new_ticker_start_date=datetime.datetime.strptime(attr_of_new_ticker[1],'%d-%b-%y').date()
                         old_ticker_end_date = datetime.datetime.strptime(attr_of_new_ticker[1],'%d-%b-%y').date() - datetime.timedelta(1)                        
                         
                        #def populate_attribute(egid, db_session, Attribute_Dao, attribute_type, attribute_value, start_date, end_date):
                         populate_attribute(egid_counter,s1,Attributes,2,i[0],start_date,old_ticker_end_date) 
                        # populate_attributes_with_new_nseticker (egid_counter,s1,attr_of_new_ticker[0], Attributes,new_ticker_start_date)
                         populate_attribute(egid_counter, s1, Attributes, 2, attr_of_new_ticker[0], new_ticker_start_date, None)
                         
                      else:
                         #populate_attributes_old_nseticker (egid_counter,s1,i,Attributes,None)
                         populate_attribute(egid_counter, s1, Attributes, 2, i[0],datetime.datetime.strptime(i[10],'%d-%b-%Y').date(), None)

                      if len(i)>12:
                          populate_attribute(egid_counter,s1,Attributes,5,i[12],datetime.datetime.strptime(i[10],'%d-%b-%Y').date(),None)
                      else:
                          populate_attribute(egid_counter,s1,Attributes,5,'None',datetime.datetime.strptime(i[10],'%d-%b-%Y').date(),None)
                      
                      populate_instruments(egid_counter,s1,i,Instruments)

                      populate_price(egid_counter,s1 ,i,Prices)
                  
                      populate_new_entries (egid_counter,s1,i,New_entries)

              if active_stocks:
                  egid_for_active_stocks_in_bhavcopy={}
                  for keys in active_stocks.keys():
                       z = s1.query(Attributes).filter(Attributes.attribute==2,Attributes.attributevalue==keys).first()
                       egid_for_active_stocks_in_bhavcopy[z.egid] = 1
                  egid_for_active_stocks_in_db = s1.query(Instruments).filter(Instruments.instrument_type == 1,Instruments.enddate==None).all()
                  stock_closing_date = start_date - datetime.timedelta(1)

                  row=()
                  for row in  egid_for_active_stocks_in_db :        
                      if (row.egid in egid_for_active_stocks_in_bhavcopy.keys()):
                          continue
                      else:
      #                    print "The stock with egid " + str(row.egid) + " is not present in the " + str(start_date) +  " bhavcopy. Updating enddate in the Instruments Table as d-1 : " + str(stock_closing_date)
                          s1.query(Instruments).filter(Instruments.egid==row.egid).filter(Instruments.instrument_type==1).update({Instruments.enddate:stock_closing_date}) 

                          query_for_name_of_dead_stock =s1.query(Attributes).filter(Attributes.egid==row.egid, Attributes.attribute==2, Attributes.startdate<=start_date) 
                          name_of_dead_stock = str(query_for_name_of_dead_stock[0].attributevalue)

                        #  populate_dead_stocks (row.egid,s1,Dead_stocks,name_of_dead_stock,stock_closing_date)
                          
                          print "The stock with egid " + str(row.egid) + " and name " +str(name_of_dead_stock ) + " is not present in the " + str(start_date) +  " bhavcopy. Updating enddate in the Instruments Table as d-1 : " + str(stock_closing_date) 


              else:
                  print "Bhavcopy for " + str(start_date) + " is not present."
           
              s1.query(last_egid).filter(last_egid.serial_no==1).update({last_egid.last_generated_egid:egid_counter})

            
              s1.commit()
            
              print  "Last generated egid is "+ str(egid_counter )

          except Exception, e:
                print "Error while processing files. Rollingback the transactions"

                print str(e)
 
                s1.rollback()
          
          finally:
            print "Total Time elapsed in this session is: " + str(time() - t) + " seconds." 
	    start_date += datetime.timedelta(days=1)		 
            s1.close()
        

        else:
              print "Bhavcopy for " + str(start_date) + " is not present."
              start_date += datetime.timedelta(days=1)
              s1.close()


if __name__ == "__main__":
    main(sys.argv[1:])
