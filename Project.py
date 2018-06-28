
# coding: utf-8

# Project 1

#  Problem Statement
# 
# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# %matplotlib inline
# df =pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv')
# df​.head(2)
# df1 =pd.read_csv('https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv')
# df1​.head(2)

# In[107]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')



# In[108]:


df =pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv')


# In[109]:


df.head(2)


# In[110]:


df1 =pd.read_csv('https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv')


# In[111]:


df1.head(2)


# Q1. Get the Metadata from the above files.

# In[112]:


print(df.info())


# In[113]:


print(df1.info())


# Q2. Get the row names from the above files.

# In[114]:


np.asarray(list(df.index))


# In[115]:


np.asarray(list(df1.index))


# Q3. Change the column name from any of the above file.

# In[116]:


df.columns


# In[117]:


df.rename(columns = {'Indicator':'Indicator_id'})


# In[118]:


df.head(2)


# Q4.Change the column name from any of the above file and store the changes made permanently.

# In[119]:


df.rename(columns = {'Indicator':'Indicator_id'}, inplace=True )


# In[120]:


df.head()


# Q5.  Change the names of multiple columns.

# In[122]:


df.rename(columns = {'PUBLISH STATES':'Publication Status','WHO region': 'WHO Region'}, inplace=True )


# In[123]:


df.head(2)


# Q6. Arrange values of a particular column in ascending order.

# In[124]:


df.sort_values(['Year'], ascending=[True]).head(5)


# Q7. Arrange multiple column values in ascending order.

# In[125]:


df.sort_values(['Indicator_id', 'Country', 'Year', 'WHO Region', 'Publication Status'], ascending=[True, True, True, True, True]).head(5)


# Q8. Make country as the first column of the dataframe.

# In[126]:


cols = list(df)
cols.insert(0, cols.pop(cols.index('Country')))
cols
df = df.loc[:, cols]
df.head(5)


# Q9. Get the column array using a variable

# In[127]:


np.asarray(df['WHO Region'])


# Q10. Get the subset rows 11, 24, 37

# In[128]:


df.loc[[11,24,37], :]


#     Q11. Get the subset rows excluding 5, 12, 23, and 56

# In[129]:


df.drop([5,12,23,56])


# Load datasets from CSV
# users = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
# sessions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')
# products =pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')
# transactions =pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')

# In[130]:


users = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
users.head()


# In[131]:


sessions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')
sessions.head()


# In[132]:


products =pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')
products.head()


# In[133]:


transactions =pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')
transactions.head()


# Q12. Join users to transactions, keeping all rows from transactions and only matching rows from users (left join)

# In[134]:


transactions = pd.merge(transactions, users, on='UserID', how='left')
df


# Q13. Which transactions have a UserID not in users?

# In[135]:


transactions[~transactions['UserID'].isin(users['UserID'])]


# Q14. Join users to transactions, keeping only rows from transactions and users that match via UserID (inner join)

# In[136]:


transactions.merge(users, how='inner', on='UserID')


# Q15. Join users to transactions, displaying all matching rows AND all non-matching rows (full outer join)

# In[137]:


transactions.merge(users, how='outer', on='UserID')


# Q16. Determine which sessions occurred on the same day each user registered

# In[138]:


pd.merge(left=users, right=sessions, how='inner', left_on=['UserID', 'Registered'], right_on=['UserID', 'SessionDate'])


# Q17.Build a dataset with every possible (UserID, ProductID) pair (cross join)

# In[139]:


df1 = pd.DataFrame({'key': np.repeat(1, users.shape[0]), 'UserID': users.UserID})
df2 = pd.DataFrame({'key': np.repeat(1, products.shape[0]), 'ProductID': products.ProductID})
pd.merge(df1, df2,on='key')[['UserID', 'ProductID']]


# Q18. Determine how much quantity of each product was purchased by each user

# In[140]:


df1 = pd.DataFrame({'key': np.repeat(1, users.shape[0]), 'UserID': users.UserID})
df2 = pd.DataFrame({'key': np.repeat(1, products.shape[0]), 'ProductID': products.ProductID})
user_products = pd.merge(df1, df2,on='key')[['UserID', 'ProductID']]
pd.merge(user_products, transactions, how='left', on=['UserID', 'ProductID']).groupby(['UserID', 'ProductID']).apply(lambda x: pd.Series(dict(
    Quantity=x.Quantity.sum()
))).reset_index().fillna(0)


# Q19. For each user, get each possible pair of pair transactions (TransactionID1, TransacationID2)

# In[141]:


pd.merge(transactions, transactions, on='UserID')


# Q20. Join each user to his/her first occuring transaction in the transactions table

# In[142]:


data=pd.merge(users, transactions.groupby('UserID').first().reset_index(), how='left', on='UserID')
data


# Q 21 Test to see if we can drop columns
# 
# Code with Output :
# my_columns = list(data.columns)
# my_columns
# ['UserID',
# 'User',
# 'Gender',
# 'Registered',
# 'Cancelled',
# 'TransactionID',
# 'TransactionDate',
# 'ProductID',
# 'Quantity']
# list(data.dropna(thresh=int(data.shape[0] * .9), axis=1).columns) #set threshold to drop NAs
# ['UserID', 'User', 'Gender', 'Registered']
# missing_info = list(data.columns[data.isnull().any()])
# missing_info
# ['Cancelled', 'TransactionID', 'TransactionDate', 'ProductID', 'Quantity']
# //for col in missing_info:
# num_missing = data[data[col].isnull() == True].shape[0]
# print('number missing for column {}: {}'.format(col, num_missing))
# Output: Count of missing data
# number missing for column Cancelled: 3
# number missing for column TransactionID: 2
# number missing for column TransactionDate: 2
# 
# number missing for column ProductID: 2
# number missing for column Quantity: 2
# 
# //for col in missing_info:
# num_missing = data[data[col].isnull() == True].shape[0]
# print('number missing for column {}: {}'.format(col, num_missing)) #count of missing data
# for col in missing_info:
# percent_missing = data[data[col].isnull() == True].shape[0] / data.shape[0]
# print('percent missing for column {}: {}'.format(
# col, percent_missing))
# Output of percentage missing data
# percent missing for column Cancelled: 0.6
# percent missing for column TransactionID: 0.4
# percent missing for column TransactionDate: 0.4my_columns = list(data.columns)
# 
# percent missing for column ProductID: 0.4
# percent missing for column Quantity: 0.4
# 
# NOTE:​ ​The​ ​solution​ ​shared​ ​through​

# In[149]:


import pandas as pd
transactions =pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')
users = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
data = transactions.merge(users, how='left', on='UserID')
data


# In[150]:


my_columns = list(data.columns)
my_columns


# In[151]:


list(data.dropna(thresh=int(data.shape[0] * .9), axis=1).columns) #set threshold to drop NAs


# In[152]:


missing_info = list(data.columns[data.isnull().any()])
missing_info


# In[153]:


for col in missing_info:
 num_missing = data[data[col].isnull() == True].shape[0]
 print('number missing for column {}: {}'.format(col, num_missing))

