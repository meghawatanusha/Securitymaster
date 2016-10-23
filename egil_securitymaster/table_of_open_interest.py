import sqlalchemy
from sqlalchemy import *

db = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/egilsecuritymaster')

db.echo = False  # Try changing this to True and see what happens

metadata = MetaData(db)
table_of_open_interest = Table('table_of_open_interest', metadata, 
                    Column ('future_contract_id', INTEGER, primary_key=True),
					Column ('date', DATETIME, primary_key=True),
					Column ('open_interest', INTEGER),
									   )
table_of_open_interest.create()
