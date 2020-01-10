# Xin HE, CITYU HONG KONG, COLLEGE OF BUSINESS
# python 3.7
"""
1. Load risk measure csv from QJE 2019 paper by Hassan et. al.
2. Merge crsp/compustat data for returns and gvkey respectively.
3. Do long-short portfolio, rebalancing quarterly.
"""

from all_packages import *

# to restore all data

filename='shelve.out'
my_shelf = shelve.open(filename)
for key in my_shelf:
    globals()[key]=my_shelf[key]

my_shelf.close()

##########################
# preprocessing data
##########################

# msf2 includes delist returns and aggregated me
msf = msf2
msf = msf[~msf['retadj'].isna()]
msf.shape

# convert gvkey to six-digit string

def gvkey_numToSting(n):
    s = str(n)
    while len(s)<6:
        s='0'+s
    return s

pool = mp.Pool(mp.cpu_count())
da['gvkey'] = pool.map(gvkey_numToSting, da['gvkey'])
pool.close()

# convert date to month-end date python datetime

def qrtToDatetime(q):
    if q[-1]=='1':    # QUARTER ONE
        return datetime.date(int(q[:4]),3,31)
    elif q[-1]=='2':
        return datetime.date(int(q[:4]),6,30)
    elif q[-1]=='3':
        return datetime.date(int(q[:4]),9,30)
    elif q[-1]=='4':
        return datetime.date(int(q[:4]),12,31)

pool = mp.Pool(mp.cpu_count())
da['mdate'] = pool.map(qrtToDatetime, da['date'])
pool.close()

da.head()

# convert date to month-end date

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)

pool = mp.Pool(mp.cpu_count())
msf['mdate'] = pool.map(last_day_of_month, msf['date'])
pool.close()

def gvkey_numToSting(n):
    s = str(n)
    while len(s)<6:
        s='0'+s
    return s

pool = mp.Pool(mp.cpu_count())
msf['gvkey'] = pool.map(gvkey_numToSting, msf['gvkey'])
pool.close()

msf.head()

# merge msf & qje data

msf['mdate'] = pd.to_datetime(msf['mdate'])
da['mdate'] = pd.to_datetime(da['mdate'])
dfm = pd.merge(da,msf,how='left',on=['mdate','gvkey'])
dfm.head()
dfm.columns

nonmiss = dfm[~dfm['retadj'].isna()]
len(set(nonmiss['gvkey']))

# dfm.to_csv('all.csv')
# nonmissing.to_csv('nonmiss.csv')

## summary statistics

grp_all = dfm.groupby('mdate')
num_series_all = pd.Series(grp_all.count()['gvkey'])
num_series_all.index = pd.to_datetime(num_series_all.index)

grp_nonmiss = nonmiss.groupby('mdate')
num_series_ret = pd.Series(grp_nonmiss.count()['gvkey'])
num_series_ret.index = pd.to_datetime(num_series_ret.index)

plt.figure(figsize=(10,5))
plt.plot(num_series_all, label='No. of gvkey')
plt.plot(num_series_ret, label='No. of nonmissing returns')
plt.legend(loc='upper left')
plt.grid()
plt.title('Number of Obs. and Non-missing Returns')
plt.savefig('./output/NoObs.pdf')

##########################
# long-short portfolio
# monthly lag 1 month
##########################

from sorting import *

var_list = ['PRisk', 'NPRisk', 'Risk', 'PSentiment', 'NPSentiment',
       'Sentiment', 'PRiskT_economic', 'PRiskT_environment', 'PRiskT_trade',
       'PRiskT_institutions', 'PRiskT_health', 'PRiskT_security', 'PRiskT_tax',
       'PRiskT_technology']

for var in var_list:
    sortport5(var, 'me', nonmiss, msf)

##### evaluation
# sharpe ratio
# information ratio
# Jansen's alpha

ff3 = pd.read_csv('ff3.csv',index_col=0)
ff3 = ff3/100

ff3['mdate'] = list(map(lambda x: str(x)+'01',ff3.index))
ff3['mdate'] = pd.to_datetime(ff3['mdate'])
ff3['mdate'] = list(map(lambda x: x+MonthEnd(0),ff3['mdate']))
ff3['MKT'] = ff3['EMKT']+ff3['RF']

m = ff3.copy()
m=m.set_index('mdate')
m=m['2002-04-30':'2018-12-31']

# read each ls portfolio returns

for var in var_list:
    ts = pd.read_csv('./output/vret-%s.csv'%(var),index_col=0)['ls']
    ts.index = pd.to_datetime(ts.index)
    m[var] = ts

def myTstat(df,r):
    ts = df[r]
    return ts.mean()/ts.std()*np.sqrt(12)

def infoRatio(df,r,rb):
    diff = df[r]-df[rb]
    return diff.mean()/diff.std()*np.sqrt(12)

def alphaCAPM(df,r):
    regressor = LinearRegression()
    y=df[r].values.reshape(-1,1)
    x=df['EMKT'].values.reshape(-1,1)
    regressor.fit(x, y)
    return regressor.intercept_[0]

def alphaFF3(df,r):
    regressor = LinearRegression()
    y=df[r].values.reshape(-1,1)
    x=df[['EMKT','SMB','HML']].values
    regressor.fit(x, y)
    return regressor.intercept_[0]

tstat=[myTstat(m,var) for var in var_list]
ir_mkt=[infoRatio(m,var,'MKT') for var in var_list]
ir_emkt=[infoRatio(m,var,'EMKT') for var in var_list]
ir_smb=[infoRatio(m,var,'SMB') for var in var_list]
ir_hml=[infoRatio(m,var,'HML') for var in var_list]
sr=[infoRatio(m,var,'RF') for var in var_list]
alpha_Capm = [alphaCAPM(m,var) for var in var_list]
alpha_FF3 = [alphaFF3(m,var) for var in var_list]

summary = pd.DataFrame([tstat,ir_mkt,ir_emkt,ir_smb,ir_hml,sr,alpha_Capm,alpha_FF3])
summary.columns=var_list
summary.index=['tstat','ir_mkt','ir_emkt','ir_smb','ir_hml','sr','alpha_capm','alpha_ff3']
summary = summary.round(4)
summary.T.to_csv('factor_evaluate.csv')

##########################
# event study
# abr 0,1
##########################
