from all_packages import *

def var_bucket(row):
    if row['var']==np.nan:
        value=''
    elif row['var']<=row['20']:
        value='1'
    elif row['var']<=row['40']:
        value='2'
    elif row['var']<=row['60']:
        value='3'
    elif row['var']<=row['80']:
        value='4'
    else:
        value='5'
    return value

def last_day_of_next_1_month(any_day):
    d1 = any_day+relativedelta(months=1)
    next_month = d1.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)

def last_day_of_next_2_month(any_day):
    d1 = any_day+relativedelta(months=2)
    next_month = d1.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)

def last_day_of_next_3_month(any_day):
    d1 = any_day+relativedelta(months=3)
    next_month = d1.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)

def wavg(group, avg_name, weight_name):
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return np.nan

def avg(group, avg_name):
    d = group[avg_name]
    try:
        return d.mean()
    except ZeroDivisionError:
        return np.nan

##########################
# long-short portfolio
# monthly lag 1 month
##########################

def sortport5(var, var_weight, nonmiss, msf):
    print(var)
    df = nonmiss[['gvkey', 'date_x', var, 'mdate']]
    df['date'] = pd.to_datetime(df['mdate'])
    df = df.drop('mdate',1)
    # breakpoint
    bp=df.groupby(['date'])[var].describe(percentiles=[0.2, 0.4,0.6,0.8]).reset_index()
    bp=bp[['date','20%','40%','60%','80%']].rename(columns={'20%':'20', '40%':'40', '60%':'60', '80%':'80'})
    df_bp = pd.merge(df[['gvkey','date',var]], bp, how='inner', on=['date'])
    df_bp.columns = ['gvkey','date','var','20','40','60','80']
    # sorting funx
    df_bp['label'] = df_bp.apply(var_bucket, axis=1)
    df0 = df_bp[['gvkey','date','label']]
    # give this sorting label to next 3 month-end date
    # this is quarterly sorting
    df1 = df0.copy()
    pool = mp.Pool(mp.cpu_count())
    df1['mdate'] = pool.map(last_day_of_next_1_month, df1['date'])
    pool.close()
    df2 = df0.copy()
    pool = mp.Pool(mp.cpu_count())
    df2['mdate'] = pool.map(last_day_of_next_2_month, df2['date'])
    pool.close()
    df3 = df0.copy()
    pool = mp.Pool(mp.cpu_count())
    df3['mdate'] = pool.map(last_day_of_next_3_month, df3['date'])
    pool.close()
    df_concat = pd.concat([df1,df2,df3])
    df_concat = df_concat.sort_values(['gvkey','date','mdate'])
    df_concat['mdate'] = pd.to_datetime(df_concat['mdate'])
    df_concat.head(50)
    # merge monthly returns
    msf['mdate'] = pd.to_datetime(msf['mdate'])
    df_label_ret = pd.merge(df_concat,msf[['gvkey','mdate','retadj',var_weight]],on=['gvkey','mdate'])
    # value weight portfolio returns
    vwret=df_label_ret.groupby(['mdate','label']).apply(wavg, 'retadj', var_weight).to_frame().reset_index().rename(columns={0: 'vwret'})
    pvtb=vwret.pivot('mdate','label','vwret')
    ls = pvtb['5']-pvtb['1']
    lsmean = ls.mean()
    lsstd = ls.std()
    sr = lsmean/lsstd
    if lsmean<0:
        pvtb['ls']=ls*(-1)
    else:
        pvtb['ls']=ls
    pvtb.to_csv('./output/vret-%s.csv'%(var))
    cumret=((pvtb+1).cumprod()-1)
    plt.figure(figsize=(10,5))
    cumret.plot()
    plt.grid()
    plt.savefig('./output/vw5_02_19_%s.pdf'%(var))
    # equal weight portfolio returns
    ewret=df_label_ret.groupby(['mdate','label']).apply(avg, 'retadj').to_frame().reset_index().rename(columns={0: 'ewret'})
    pvtb=ewret.pivot('mdate','label','ewret')
    ls = pvtb['5']-pvtb['1']
    lsmean = ls.mean()
    lsstd = ls.std()
    sr = lsmean/lsstd
    if lsmean<0:
        pvtb['ls']=ls*(-1)
    else:
        pvtb['ls']=ls
    pvtb.to_csv('./output/eret-%s.csv'%(var))
    cumret=((pvtb+1).cumprod()-1)
    plt.figure(figsize=(10,5))
    cumret.plot()
    plt.grid()
    plt.savefig('./output/ew5_02_19_%s.pdf'%(var))
    return 0



##########################
# event study
# abr 0,1
##########################
