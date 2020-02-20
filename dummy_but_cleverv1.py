import psycopg2
conn = psycopg2.connect("dbname=survey user=postgres password=surveysurvey")
cur = conn.cursor()
#/root/allocations
#table in survey db is called survey_countrypayoutreport
# survey_revenuereport"
cur.execute("Select * FROM survey_pricebackup LIMIT 0")
colnames = [desc[0] for desc in cur.description]
cur.execute("SELECT * FROM  survey_pricebackup")
hist=[]         
  
mobile_records = cur.fetchall() 
for row in mobile_records:
    print(colnames[0], row[0], )
    print(colnames[1], row[1])
    print(colnames[2], row[2])
    print(colnames[3], row[3])
    print(colnames[4], row[4])
    print(colnames[5], row[5])
    print(colnames[6], row[6], "\n")
    current_row=[row[0],row[1],row[2],row[3],row[4],row[5],row[6]]
    hist.append(current_row)
import pandas as pd
import numpy as np 
train=pd.DataFrame(hist, columns=colnames)
train.fillna(value='-13', inplace=True)


import psycopg2
conn = psycopg2.connect("dbname=survey user=postgres password=surveysurvey")
cur = conn.cursor()
#/root/allocations
#table in survey db is called survey_countrypayoutreport
# survey_revenuereport"
cur.execute("Select * FROM survey_revenuereport LIMIT 0")
colnames = [desc[0] for desc in cur.description]
cur.execute("SELECT * FROM  survey_revenuereport")

hist=[]
mobile_records = cur.fetchall() 
for row in mobile_records:
    print(colnames[0], row[0], )
    print(colnames[1], row[1])
    print(colnames[2], row[2])
    print(colnames[3], row[3])
    print(colnames[4], row[4])
    print(colnames[5], row[5], "\n")
 
    current_row=[row[0],row[1],row[2],row[3],row[4],row[5]]
    hist.append(current_row)
import pandas as pd
import numpy as np 
revenue=pd.DataFrame(hist, columns=colnames)
revenue.fillna(value='-13', inplace=True)
import dateutil
revenue['date']=pd.to_datetime(revenue['date']) 
 

revenue['year'] = pd.DatetimeIndex(revenue['date']).year

revenue['month'] = pd.DatetimeIndex(revenue['date']).month

revenue['day'] = pd.DatetimeIndex(revenue['date']).day

revenue.groupby(['day'])['amount'].count()
 

last_revenue=revenue.iloc[-1]

last_grouped=revenue[revenue['day']==last_revenue['day']]
last_grouped=last_grouped[last_grouped['year']==last_revenue['year']]
last_grouped=last_grouped[last_grouped['month']==last_revenue['month']]
total_revenue=np.sum(last_grouped['amount'])*3
print("total revenue last daya" , total_revenue)


#CREATE TABLE B_hist_revenue (
#   day integer NOT NULL,
#   month integer NOT NULL,
#   year integer NOT NULL,
#   day_revenue integer NOT NULL,
#   mobile_alloc double precision NOT NULL,
#   desktop_alloc double precision NOT NULL
   
   
#);
#ALTER TABLE B_hist_revenue OWNER TO survey;
 
conn = psycopg2.connect("dbname=survey user=postgres password=surveysurvey")
cur = conn.cursor()
 
cur.execute("Select * FROM B_hist_revenue LIMIT 0")
colnames = [desc[0] for desc in cur.description]
cur.execute("SELECT * FROM  B_hist_revenue")
# loading the allocations 

dev_alloc=pd.read_csv('/root/allocations/final_desktop.csv')
mobile_alloc=pd.read_csv('/root/allocations/final_mobile.csv')
dev_alloc.columns=['Country','Min','Max']
mobile_alloc.columns=['Country','Min','Max']

#get desktop rev
 
device_rev=last_grouped[last_grouped['device']==1]
device_gr=pd.DataFrame(device_rev.groupby('country')['amount'].sum()  )

device_gr['location']=device_gr.index
#get mobile rev

mobile_rev=last_grouped[last_grouped['device']==0]
mobile_gr=pd.DataFrame(mobile_rev.groupby('country')['amount'].sum()  )
mobile_gr['location']=mobile_gr.index
 
#writing today revenue by device and mobile  at current time  t
device_gr['day']=[last_grouped['day'].iloc[0]]*device_gr.shape[0]
device_gr['month']=[last_grouped['month'].iloc[0]]*device_gr.shape[0]
device_gr['year']=[last_grouped['year'].iloc[0]]*device_gr.shape[0]
device_gr.to_csv('/root/allocations/device_id_'+str(last_grouped['id'].iloc[0])+'.csv',index=False)


mobile_gr['day']=[last_grouped['day'].iloc[0]]*mobile_gr.shape[0]
mobile_gr['month']=[last_grouped['month'].iloc[0]]*mobile_gr.shape[0]
mobile_gr['year']=[last_grouped['year'].iloc[0]]*mobile_gr.shape[0]
mobile_gr.to_csv('/root/allocations/mobile_id_'+str(last_grouped['id'].iloc[0])+'.csv',index=False)
            
#generation today allocations 
#for dev

dev_alloc_gen=[]
cuts_bins=[]
cut_idx=[]
for alloc in range(dev_alloc.shape[0]):
          current_row=dev_alloc.iloc[alloc]
          print(current_row)
          min_=current_row['Min']
          max_=current_row['Max']
          cut=pd.cut(np.array([min_,max_]),10,retbins=True)
          dev_alloc_gen.append(cut[1][8])#means  6-th  cut
          cut_idx.append(8)
          print(cut[1][7]<max_)
          print(cut[1][6]>min_)
          cuts_bins.append(cut[1])
dev_alloc['alloc']=np.array(dev_alloc_gen)
dev_alloc['bins']=cuts_bins
dev_alloc['cut_idx']=cut_idx
#generation today allocations 
#for mobile
mobile_alloc_gen=[]
cuts_bins=[]
cut_idx=[]
for alloc in range(mobile_alloc.shape[0]):
          current_row=mobile_alloc.iloc[alloc]
          print(current_row)
          min_=current_row['Min']
          max_=current_row['Max']
          cut=pd.cut(np.array([min_,max_]),10,retbins=True)
          mobile_alloc_gen.append(cut[1][8])#means  6-th  cut
          cut_idx.append(8)
          print(cut[1][7]<max_)
          print(cut[1][6]>min_)
          cuts_bins.append(cut[1])
mobile_alloc['alloc']=np.array(mobile_alloc_gen)
mobile_alloc['bins']=cuts_bins
mobile_alloc['cut_idx']=(cut_idx)

          
#write them locally for today meaning time t 
 
import datetime
 
currentDT = datetime.datetime.now()

dev_alloc['day']= currentDT.day

dev_alloc['month']= currentDT.month

dev_alloc['year']= currentDT.year

mobile_alloc['day']= currentDT.day

mobile_alloc['month']= currentDT.month

mobile_alloc['year']= currentDT.year



#push them into DB table survey_price


import psycopg2
conn = psycopg2.connect("dbname=survey user=postgres password=surveysurvey")
cur = conn.cursor()
 
cur.execute("Select * FROM survey_price LIMIT 0")
colnames = [desc[0] for desc in cur.description]
cur.execute("SELECT * FROM  survey_price")

hist=[]
mobile_records = cur.fetchall() 
for row in mobile_records:
    print(colnames[0], row[0], )
    print(colnames[1], row[1])
    print(colnames[2], row[2])
    print(colnames[3], row[3])
    print(colnames[4], row[4], "\n")
   
    current_row=[row[0],row[1],row[2],row[3],row[4]]
    hist.append(current_row)



################################################################################################
 
d=last_revenue['day']
m=last_revenue['month']
y=last_revenue['year']
 
mobile_sum=np.sum(mobile_gr['amount'])*3
dev_sum=np.sum(device_gr['amount'])*3
t=(d,m,y,int(total_revenue),int(mobile_sum),int(dev_sum))
sqlInsert="insert into B_hist_revenue values(%d,%d,%d,%d,%d,%d)" %t
 
cur.execute(sqlInsert)

#hist=[]         
#cur.execute("SELECT * FROM  B_hist_revenue")  
#mobile_records = cur.fetchall() 
#for row in mobile_records:
#    print(colnames[0], row[0], )
#    print(colnames[1], row[1])
#    print(colnames[2], row[2])
#    print(colnames[3], row[3],"\n")
# 
#    current_row=[row[0],row[1],row[2],row[3]]
#    hist.append(current_row)

prices=pd.DataFrame(hist, columns=colnames)
#prices.fillna(value='-13', inplace=True)


import psycopg2
conn = psycopg2.connect("dbname=survey user=postgres password=surveysurvey")
cur = conn.cursor()
 
cur.execute("Select * FROM survey_countriesforprices LIMIT 0")
colnames = [desc[0] for desc in cur.description]
cur.execute("SELECT * FROM  survey_countriesforprices")

hist=[]
mobile_records = cur.fetchall() 
for row in mobile_records:
    print(colnames[0], row[0], )
    print(colnames[1], row[1])
    print(colnames[2], row[2], "\n")
 
   
    current_row=[row[0],row[1],row[2]]
    hist.append(current_row)
    
countries=pd.DataFrame(hist, columns=colnames)

# add country codes
codes=[]
for row in range(dev_alloc.shape[0]):
        country=dev_alloc['Country'].iloc[row]
        code=countries[countries['country']==country]['code']
        code=code.reset_index(drop=True)
        code=code.iloc[0]
        codes.append(code)
        
dev_alloc['codes']=codes   

# add country codes
codes=[]
for row in range(mobile_alloc.shape[0]):
        country=mobile_alloc['Country'].iloc[row]
        code=countries[countries['country']==country]['code']
        code=code.reset_index(drop=True)
        code=code.iloc[0]
        codes.append(code)
dev_alloc.to_csv('/root/allocations/dev_alloc_'+str(currentDT.day)+'_'+str(currentDT.month)+'_'+str(currentDT.year)+'.csv')

mobile_alloc.to_csv('/root/allocations/mobile_alloc_'+str(currentDT.day)+'_'+str(currentDT.month)+'_'+str(currentDT.year)+'.csv')
        
mobile_alloc['codes']=codes    
c=0
for row in range(prices.shape[0]):
        if prices.iloc[row]['country']==None  or mobile_alloc[mobile_alloc['codes']==prices.iloc[row]['country']].shape[0]==0  or dev_alloc[dev_alloc['codes']==prices.iloc[row]['country']].shape[0]==0  :
           a=1
        else:
            print('perform row ' , row)
            c+=1
            prices.at[row,'mobile']=mobile_alloc[mobile_alloc['codes']==prices.iloc[row]['country']]['alloc'].iloc[0]
            prices.at[row,'desktop']=dev_alloc[dev_alloc['codes']==prices.iloc[row]['country']]['alloc'].iloc[0]
        
 

prices.to_csv('/root/allocations/prices_updated_'+str(currentDT.day)+'_'+str(currentDT.month)+'_'+str(currentDT.year)+'.csv')

 
###    
conn = psycopg2.connect("dbname=survey user=postgres password=surveysurvey")
cur = conn.cursor()
m=list(prices['mobile'].astype(float) ) 
tuples = [(mobile , country) for mobile,country in zip(m , prices.country)] 
      
sql_update_query = """Update survey_price set mobile = %s where country = %s"""
cur.executemany(sql_update_query, tuples)
conn.commit()
row_count = cur.rowcount
print(row_count, "Records Updated")
 
 
conn.commit()
cur.close()
conn.close()


#####
conn = psycopg2.connect("dbname=survey user=postgres password=surveysurvey")
cur = conn.cursor()
m=list(prices['desktop'].astype(float) ) 
tuples = [(mobile , country) for mobile,country in zip(m , prices.country)] 
      
sql_update_query = """Update survey_price set desktop = %s where country = %s"""
cur.executemany(sql_update_query, tuples)
conn.commit()
row_count = cur.rowcount
print(row_count, "Records Updated")
 
 
conn.commit()
cur.close()
conn.close()

######
           
        

