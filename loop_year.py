from event import *
from datetime import datetime

import os
print(os.getcwd())

def loop_year(year):
    os.chdir('./xml-data/{}/'.format(year))
    print(os.getcwd())

    tb = pd.DataFrame()
    n=0
    for file in os.listdir('./'):
        n=n+1
        print(n)
        if file.endswith(".xml"):
            print(file)
            # file_name = file[:-4]
            # file_name = '92800_T'
            e1 = event(file)
            e1.get_elements()
            tb[e1.id] = e1.tab()

    tb.index = ['xmleventtypeid','xmlticker','xmleventdate','xmlfakeqtr','xmlcity','xmlcompanyname']
    tb = tb.T

    #dates = [datetime.strptime(i[:-6], '%d-%b-%y %H:%M') for i in d['eventdate']]

    ## data format
    dates = pd.to_datetime(tb['xmleventdate'])
    tb['xmlymd'] = [i.strftime('%Y%m%d') for i in dates]

    ## ticker, some are '\n'
    tic = []
    for i in tb['xmlticker']:
        t = i
        if i[0]=='\n':
            t='NAN'
        tic.append(t)

    tb['xmlticker'] = tic

    ## city, some are '\n'
    city = []
    for i in tb['xmlcity']:
        t = i
        if i[0]=='\n':
            t='NAN'
        city.append(t)

    tb['xmlcity'] = city

    tb.to_csv('../xml-summary/{}xml.csv'.format(year))
    print('finish')
    os.chdir('../')
    os.chdir('../')
    print(os.getcwd())

#
#        txt = read_pdf(file_name)
#        counter = tokenize(file_name,txt)
#        df = pd.DataFrame.from_dict(counter, orient='index').reset_index()
#        df.columns = ['token','count']
#        df = df.sort_values('count',ascending=False)
#        os.chdir('../')
#        os.chdir('../')
#        df.to_csv('./tokenization/PDF2001/'+file_name+'.csv', header=True, index=False)
#        os.getcwd()
#        os.chdir('./conference call/PDF2001/')
#        os.getcwd()

        

