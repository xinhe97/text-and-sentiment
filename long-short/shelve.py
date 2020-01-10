


# to save the whole workspace
filename='shelve.out'
my_shelf = shelve.open(filename,'n') # 'n' for new
variable_list = ['dsf','msf','da','info']
# for key in dir():
for key in variable_list:
    try:
        my_shelf[key] = globals()[key]
    except TypeError:
        # __builtins__, my_shelf, and imported modules can not be shelved.
        print('ERROR shelving: {0}'.format(key))

my_shelf.close()

# to restore
filename='shelve.out'
my_shelf = shelve.open(filename)
for key in my_shelf:
    globals()[key]=my_shelf[key]

my_shelf.close()
