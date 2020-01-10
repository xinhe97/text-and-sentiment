
# fuzzy matching
company_name_da = list(set(da['company_name']))
company_name_info = list(set(info['xmlcompanyname']))
print(len(company_name_da), len(company_name_info))

# company_name_matched = [process.extractOne(i, company_name_info, scorer=fuzz.token_set_ratio) for i in company_name_da]
# here I use token_set_ratio as the scorer,
# alternative setting:
# - fuzz.ratio
# - fuzz.partial_ratio
# - fuzz.token_sort_ratio
# - fuzz.token_set_ratio

# matching is too slow
# use parallel computing
# cb server has 48 CPUs
def myFuzzyMatch(i):
    return process.extractOne(i, company_name_info, scorer=fuzz.token_set_ratio)

pool = mp.Pool(mp.cpu_count())
results = pool.map(myFuzzyMatch, company_name_da)
# results = pool.map(lambda i: process.extractOne(i, company_name_info, scorer=fuzz.token_set_ratio), company_name_da)
pool.close()


r0 = [i[0] for i in results]
r1 = [i[1] for i in results]

df_name = pd.DataFrame([company_name_da,r0,r1]).T
df_name.columns = ['name_qje','name_info','token_set_ratio']
df_name
