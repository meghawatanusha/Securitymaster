import sqlalchemy
from sqlalchemy import *

db = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/egilsecuritymaster')

db.echo = False  # Try changing this to True and see what happens

metadata = MetaData(db)
table_of_future_series = Table('table_of_future_series', metadata, 
                    Column ('future_series_id', INTEGER, primary_key=True),
					Column ('underlyingid', INTEGER),
									   )
table_of_future_series.create()
