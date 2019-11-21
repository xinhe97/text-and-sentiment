from event import *
from loop_year import *


for year in range(2001,2020):
    print(year)
    loop_year(year)

print('loop finish')

print('merge each year to one file')

da = pd.DataFrame()
for i in range(2001,2020):
    tb = pd.read_csv('./xml-data/xml-summary/{}xml.csv'.format(i))
    da = da.append(tb)
    
da.to_csv('./xml-data/xml-summary/all-year-xml.csv')

