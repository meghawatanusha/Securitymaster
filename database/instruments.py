from sqlalchemy import *

db = create_engine('mysql+pymysql://egil_user:egil_user@egsecuritymaster.cvxyuzctu0dv.ap-south-1.rds.amazonaws.com:3306/egilsecuritymaster')

db.echo = False  # Try changing this to True and see what happens

metadata = MetaData(db)
attributes = Table('instruments', metadata, 
                    Column ('instrument_type', String(20)),
					Column ('instrument_id', integer, primary_key=True),
				   )
instruments.create()

i = instruments.insert()
i.execute(instrument_type='stocks',instrument_id=1)
i.execute(instrument_type='future',instrument_id=2) 
i.execute(instrument_type='futures_series',instrument_id=3)


s = instruments.select()
rs = s.execute()

row = rs.fetchone()
print 'instrument_type', row[0]
print 'instrument_id:', row['instrument_id']

for row in rs:
    print row.instrument_type, 'is', row.instrument_id