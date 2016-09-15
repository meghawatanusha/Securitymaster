import sqlalchemy
from sqlalchemy import *

db = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/egilsecuritymaster')

db.echo = False  # Try changing this to True and see what happens

metadata = MetaData(db)
table_of_future_contracts = Table('table_of_future_contracts', metadata,
                    Column ('future_id', INTEGER, primary_key=True), 
                    Column ('future_series_id', INTEGER),
					Column ('contractsize', INTEGER),
					Column ('listdate', DATETIME),
					Column ('expirationdate', DATETIME),
					Column ('lasttradedate', DATETIME),
									   )
table_of_future_contracts.create()
