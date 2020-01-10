from all_packages import *

###########################
# CSV      #
###########################

# read downloaded risk & sentiment data
da = pd.read_csv('firmquarter_2019q2_commasep.csv')
da.head()

def gvkey_numToSting(n):
    s = str(n)
    while len(s)<6:
        s='0'+s
    return s

pool = mp.Pool(mp.cpu_count())
gvkey_list = pool.map(gvkey_numToSting, da['gvkey'])
pool.close()

# read conf summary, I made this CSV
info = pd.read_csv('all-year-xml.csv')
info.head()


# wrds db
# merge crsp & compustat

db = wrds.Connection()

###########################
# CRSP Monthly Block      #
###########################

# monthly returns - permno - gvkey
msf = db.raw_sql("""
                select distinct a.permno, gvkey, liid as iid,
                		date, prc, vol, ret, retx
                from
                	crsp.msf as a,
                    (select * from crsp.msenames
                        where shrcd in (10, 11)
                	) as b,
                	(select * from crsp.ccmxpf_linktable
                        where
            			linktype in ('LU', 'LC')
            			and LINKPRIM in ('P', 'C')
            			and USEDFLAG=1
                	) as c
                where a.permno=b.permno and b.permno=c.lpermno
                and NAMEDT<=a.date and a.date<=NAMEENDT
                and linkdt<=a.date and a.date<=coalesce(linkenddt, '%s')
                and '01/01/2000'<=a.date and a.date<='01/01/2020'
                and gvkey in (%s)
                """%(
                    datetime.datetime.today().strftime('%d/%m/%Y'),
                    "'"+"','".join(gvkey_list)+"'"
                    )
                )

msf.head()

# work on monthly crsp
# 1. dlist returns
# 2. aggregate market cap

crsp_m = db.raw_sql("""
                      select a.permno, a.permco, a.date, b.shrcd, b.exchcd,
                      a.ret, a.retx, a.shrout, a.prc
                      from crsp.msf as a
                      left join crsp.msenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '01/01/2000' and '01/01/2020'
                      and b.exchcd between 1 and 3
                      """)

# change variable format to int
crsp_m[['permco','permno','shrcd','exchcd']]=crsp_m[['permco','permno','shrcd','exchcd']].astype(int)

# Line up date to be end of month
crsp_m['date']=pd.to_datetime(crsp_m['date'])
crsp_m['jdate']=crsp_m['date']+MonthEnd(0)

# add delisting return
dlret = db.raw_sql("""
                     select permno, dlret, dlstdt
                     from crsp.msedelist
                     """)
dlret.permno=dlret.permno.astype(int)
dlret['dlstdt']=pd.to_datetime(dlret['dlstdt'])
dlret['jdate']=dlret['dlstdt']+MonthEnd(0)

crsp = pd.merge(crsp_m, dlret, how='left',on=['permno','jdate'])
crsp['dlret']=crsp['dlret'].fillna(0)
crsp['ret']=crsp['ret'].fillna(0)
crsp['retadj']=(1+crsp['ret'])*(1+crsp['dlret'])-1
crsp['me']=crsp['prc'].abs()*crsp['shrout'] # calculate market equity
crsp=crsp.drop(['dlret','dlstdt','prc','shrout'], axis=1)
crsp=crsp.sort_values(by=['jdate','permco','me'])

### Aggregate Market Cap ###
# sum of me across different permno belonging to same permco a given date
crsp_summe = crsp.groupby(['jdate','permco'])['me'].sum().reset_index()
# largest mktcap within a permco/date
crsp_maxme = crsp.groupby(['jdate','permco'])['me'].max().reset_index()
# join by jdate/maxme to find the permno
crsp1=pd.merge(crsp, crsp_maxme, how='inner', on=['jdate','permco','me'])
# drop me column and replace with the sum me
crsp1=crsp1.drop(['me'], axis=1)
# join with sum of me to get the correct market cap info
crsp2=pd.merge(crsp1, crsp_summe, how='inner', on=['jdate','permco'])
# sort by permno and date and also drop duplicates
crsp2=crsp2.sort_values(by=['permno','jdate']).drop_duplicates()

# merge msf and crsp2

msf['permno'] = [int(i) for i in msf['permno']]
msf['date'] = pd.to_datetime(msf['date'])
crsp2['date'] = pd.to_datetime(crsp2['date'])
msf2=pd.merge(msf[['permno','gvkey','date']],crsp2[['permno','date','retadj','me']],how='left',on=['permno','date'])

###########################
# CRSP Daily Block      #
###########################

# daily returns - permno - gvkey
dsf = db.raw_sql("""
                select distinct a.permno, gvkey, liid as iid,
                		date, prc, vol, ret, retx
                from
                	crsp.dsf as a,
                    (select * from crsp.msenames
                        where shrcd in (10, 11)
                	) as b,
                	(select * from crsp.ccmxpf_linktable
                        where
            			linktype in ('LU', 'LC')
            			and LINKPRIM in ('P', 'C')
            			and USEDFLAG=1
                	) as c
                where a.permno=b.permno and b.permno=c.lpermno
                and NAMEDT<=a.date and a.date<=NAMEENDT
                and linkdt<=a.date and a.date<=coalesce(linkenddt, '%s')
                and '01/01/2000'<=a.date and a.date<='01/01/2020'
                and gvkey in (%s)
                """%(
                    datetime.datetime.today().strftime('%d/%m/%Y'),
                    "'"+"','".join(gvkey_list)+"'"
                    )
                )

dsf.head()


###########################
# Save in Shelve      #
###########################

# to save the whole workspace or a list of variables
filename='shelve.out'
my_shelf = shelve.open(filename,'n') # 'n' for new
variable_list = ['dsf','msf','msf2','da','info']
# for key in dir():
for key in variable_list:
    try:
        my_shelf[key] = globals()[key]
    except TypeError:
        # __builtins__, my_shelf, and imported modules can not be shelved.
        print('ERROR shelving: {0}'.format(key))

my_shelf.close()
