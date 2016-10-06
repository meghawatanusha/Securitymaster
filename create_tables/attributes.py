import sqlalchemy
from sqlalchemy import *

db = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/egilsecuritymaster')

db.echo = False  # Try changing this to True and see what happens

metadata = MetaData(db)
attributes = Table('attributes', metadata, 
                    Column ('attribute', VARCHAR(20)),
					Column ('attribute_id', INTEGER, primary_key=True),
				   )
attributes.create()

i = attributes.insert()
i.execute(attribute='EGID',attribute_id=1)
i.execute(attribute= 'NSE Ticker', attribute_id=2) 
i.execute(attribute='Name', attribute_id=3)
i.execute(attribute='RIC', attribute_id=4)
i.execute(attribute='ISIN', attribute_id=5)

s = attributes.select()
rs = s.execute()

row = rs.fetchone()
print 'attribute', row[0]
print 'attribute_id:', row['attribute_id']

for row in rs:
    print row.attribute, 'is', row.attribute_id