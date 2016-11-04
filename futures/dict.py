import csv


#SYMB_COMPANY_NAME, SM_KEY_SYMBOL, SM_NEW_SYMBOL, SM_APPLICABLE_FROM
#3M India Limited,BIRLA3M,3MINDIA,15-Jun-04

reader = csv.reader(open('test_symbolchange.csv'))

symbolchange = {}
row_in_symbolchange=()
for row_in_symbolchange in reader:
    key = row_in_symbolchange[1]
    if key in symbolchange:
          pass
    symbolchange[key] = row_in_symbolchange[2:]
print symbolchange

