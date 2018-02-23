# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 09:03:16 2017

@author: DQ2
"""
#relativedelta(days=+2) > relativedelta(months=3)
import cx_Oracle
import pandas as pd
import datetime
import math
import os
import calendar
import numpy as np
from dateutil.relativedelta import relativedelta

pd.options.mode.chained_assignment = None

import sys
import os
import cif_config
from Query_obj import Query_obj

os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"

g_date_format = "YYYY-MM-DDHH24MISS"

insert_date_format = "DD/MM/YYYY"

#run_list('C:/Users/DQ2/Desktop/sba1/in-tba/input/', 'C:/Users/DQ2/Desktop/sba1/in-tba/output/')

print 'start running'   

# global dataframe
df_gam = pd.DataFrame()
df_tam = pd.DataFrame()
df_htd = pd.DataFrame()
df_int_adm = pd.DataFrame()
df_idt = pd.DataFrame()
df_itc = pd.DataFrame()
df_icv = pd.DataFrame()
df_tvs = pd.DataFrame()

# special schm code table    
tda_special_table = pd.DataFrame([['FD007', 1],
                                  ['FD008', 1], 
                                  ['FD011', 1],
                                  ['FD012', 1],
                                  ['FD013', 1],
                                  ['FD014', 1],
                                  ['FD015', 1],
                                  ['FD038', 1],
                                  ['FD035', 0.5],
                                  ['FD010', 6]], columns=['SCHM_CODE', 'VALUE'])

# interest rounding fact
round_boolean = False

def run(foracid):
    connect_database(foracid)
    run_htd()
    run_int_adm()

# connect oracle to select data into dataframe
def connect_database(db1, foracid): 
    
    global df_gam
    global df_tam
    global df_htd
    global df_int_adm
    global df_idt
    global df_itc
    global df_icv
    global df_tvs
    
    gam_sql = 'select foracid, acid, sol_id, schm_code, acct_cls_flg, clr_bal_amt, acct_cls_date, acct_opn_date from tbaadm.gam where foracid = \''+foracid+'\''
          
    df_gam = pd.DataFrame(db1.get_rows(gam_sql).fetchall(), columns=['FORACID', 'ACID', 'SOL_ID', 'SCHM_CODE', 'ACCT_CLS_FLG', 'CLR_BAL_AMT', 'ACCT_CLS_DATE', 'ACCT_OPN_DATE'])
    
    tam_sql = 'select acid, deposit_period_mths, maturity_date from tbaadm.tam where acid = \''+df_gam.ACID[0]+'\''
    htd_sql = 'select tran_date, tran_id, del_flg, part_tran_type, acid, value_date, tran_amt, tran_particular from tbaadm.htd where acid = \''+df_gam.ACID[0]+'\''
    int_adm_sql = 'select acid, record_type, base_amount, value_date, cr_or_dr_amt_ind, remarks from tbaadm.int_adm where acid = \''+df_gam.ACID[0]+'\''
    idt_sql = 'select entity_id, product_for_int_rate, interest_amount, int_table_code, start_date, end_date from tbaadm.idt where entity_id = \''+df_gam.ACID[0]+'\''     
    itc_sql = 'select entity_id, int_tbl_code_srl_num, start_date, end_date, id_cr_pref_pcnt, pegged_flg from tbaadm.itc where entity_id = \''+df_gam.ACID[0]+'\''
    
    df_tam = pd.DataFrame(db1.get_rows(tam_sql).fetchall(), columns=['ACID', 'DEPOSIT_PERIOD_MTHS', 'MATURITY_DATE'])
      
    df_htd = pd.DataFrame(db1.get_rows(htd_sql).fetchall(), columns=['TRAN_DATE', 'TRAND_ID', 'DEL_FLG', 'PART_TRAN_TYPE', 'ACID', 'VALUE_DATE', 'TRAN_AMT', 'TRAN_PARTICULAR'])
    df_htd = df_htd.sort_values(['VALUE_DATE']).reset_index(drop=True)    
    
    df_int_adm = pd.DataFrame(db1.get_rows(int_adm_sql).fetchall(), columns=['ACID', 'RECORD_TYPE', 'BASE_AMOUNT', 'VALUE_DATE', 'CR_OR_DR_AMT_IND','REMARKS'])
    
    df_idt = pd.DataFrame(db1.get_rows(idt_sql).fetchall(), columns=['ENTITY_ID', 'PRODUCT_FOR_INT_RATE', 'INTEREST_AMOUNT', 'INT_TABLE_CODE', 'START_DATE', 'END_DATE'])
    
    df_itc = pd.DataFrame(db1.get_rows(itc_sql).fetchall(), columns=['ENTITY_ID', 'INT_TBL_CODE_SRL_NUM', 'START_DATE', 'END_DATE', 'ID_CR_PREF_PCNT', 'PEGGED_FLG'])
    
    icv_sql = 'select int_tbl_code, start_date, int_version from tbaadm.icv where int_tbl_code = \''+df_idt.INT_TABLE_CODE[0]+'\''
    tvs_sql = 'select int_tbl_code, int_tbl_ver_num, max_contracted_mths, max_period_run_mths, begin_slab_amount, max_slab_amount, nrml_int_pcnt, penal_pcnt from tbaadm.tvs where int_tbl_code = \''+df_idt.INT_TABLE_CODE[0]+'\' and max_contracted_mths = '+str(df_tam.DEPOSIT_PERIOD_MTHS[0])

    df_icv = pd.DataFrame(db1.get_rows(icv_sql).fetchall(), columns=['INT_TBL_CODE', 'START_DATE', 'INT_VERSION'])
    
    df_tvs = pd.DataFrame(db1.get_rows(tvs_sql).fetchall(), columns=['INT_TBL_CODE', 'INT_TBL_VER_NUM', 'MAX_CONTRACTED_MTHS', 'MAX_PERIOD_RUN_MTHS','BEGIN_SLAB_AMT', 'MAX_SLAB_AMT', 'NRML_INT_PCNT', 'PENAL_PCNT'])
    
    
# check holiday interest after the deposit end    
def check_holiday_int():
    
    tda_table = create_tda()
    #print tda_table.START_DATE[tda_table.shape[0]-1].weekday()
    if(tda_table.START_DATE[tda_table.shape[0]-1] + relativedelta(days=4) > tda_table.END_DATE[tda_table.shape[0]-1]):
        if(tda_table.START_DATE[tda_table.shape[0]-1].weekday() == 6 or tda_table.START_DATE[tda_table.shape[0]-1].weekday() == 5):
            
            if(tda_table.END_DATE[tda_table.shape[0]-1].weekday() == 0):
                if(df_gam.ACCT_CLS_DATE[0] == tda_table.END_DATE[tda_table.shape[0]-1]):
                    return True
    
    return False
    
# create a transaction for schm code that calculate every month
def create_tda_every_month():
    
    df_htd_2 = reduce_htd()
    
    # find first maturity date
    start_date = df_tam.MATURITY_DATE[0] + relativedelta(months=-df_tam.DEPOSIT_PERIOD_MTHS[0])
    while(start_date+relativedelta(months=+df_tam.DEPOSIT_PERIOD_MTHS[0]) >= datetime.datetime.now()+relativedelta(months=-6)):
        start_date = start_date + relativedelta(months=-df_tam.DEPOSIT_PERIOD_MTHS[0])

    money = 0
    j = 0
    df = pd.DataFrame(columns=['START_DATE', 'END_DATE', 'MONEY'])
    df = df.append(pd.DataFrame([[start_date, start_date + relativedelta(months=+df_tam.DEPOSIT_PERIOD_MTHS[0]), 0]], columns=['START_DATE', 'END_DATE', 'MONEY']), ignore_index=True)
    

    
    for i in range(df_htd_2.shape[0]):
        
        if(df_htd_2.DEL_FLG[i] == 'N'):
            if(df_htd_2.VALUE_DATE[i] <= start_date):
                if(df_htd.PART_TRAN_TYPE[i] == 'C'):
                    money = money + round(df_htd.TRAN_AMT[i], 2)
                else:
                    money = money - round(df_htd.TRAN_AMT[i], 2) 
                df.MONEY[j] = money
            else:
                if(df_htd.PART_TRAN_TYPE[i] == 'C'):
                    money = money + round(df_htd.TRAN_AMT[i], 2)
                else:
                    money = money - round(df_htd.TRAN_AMT[i], 2)
                    
                if(df_htd_2.VALUE_DATE[i] > start_date + relativedelta(months=+df_tam.DEPOSIT_PERIOD_MTHS[0])):

                    df = df.append(pd.DataFrame([[df.END_DATE[j], df_htd_2.VALUE_DATE[i], money]], columns=['START_DATE', 'END_DATE', 'MONEY']), ignore_index=True)
                    j = j + 1
                else:
                    
                    df.END_DATE[j] = df_htd_2.VALUE_DATE[i]
   
   # remove the last record if not holiday interest
    if(df.shape[0] > 1):
        if(df.START_DATE[df.shape[0]-1] + relativedelta(days=+7) < df.END_DATE[df.shape[0]-1]):
            df = df.drop(df.shape[0]-1)
    return df    


# create a transaction of schm code FD006    
def create_tda_fd006():
    
    df_htd_2 = reduce_htd()

    money = df_htd_2.TRAN_AMT[0]
    j = 0

    
    df = pd.DataFrame(columns=['START_DATE', 'END_DATE', 'MONEY'])
    
    # find the first maturity date
    start_date = df_tam.MATURITY_DATE[0] + relativedelta(months=-df_tam.DEPOSIT_PERIOD_MTHS[0])
    while((start_date+relativedelta(months=-df_tam.DEPOSIT_PERIOD_MTHS[0])).year >= 2010 and (start_date+relativedelta(months=-df_tam.DEPOSIT_PERIOD_MTHS[0])) >= df_htd_2.VALUE_DATE[0]):  
        start_date = start_date + relativedelta(months=-df_tam.DEPOSIT_PERIOD_MTHS[0])
    
    # exception
    if(start_date.year < 2010):
        return df
    
    maturity_date = start_date + relativedelta(months=24)
    pre_maturity_date = start_date + relativedelta(months=12)
    
    # exception
    if(df_tam.DEPOSIT_PERIOD_MTHS[0] == 0):
        return df
        
    df = df.append(pd.DataFrame([[start_date, start_date, round(df_htd_2.TRAN_AMT[0], 2)]], columns=['START_DATE', 'END_DATE', 'MONEY']), ignore_index=True)
    
    for i in range(1, df_htd_2.shape[0]):
        
        if(df_htd_2.DEL_FLG[i] == 'N'):
        
            if(df_htd_2.VALUE_DATE[i] == maturity_date):
                
                if(df_htd_2.PART_TRAN_TYPE[i] == 'C'):
                    money = money + round(df_htd_2.TRAN_AMT[i], 2)
                else:
                    money = money - round(df_htd_2.TRAN_AMT[i], 2) 
                
                df.END_DATE[j] = maturity_date
                df = df.append(pd.DataFrame([[df_htd_2.VALUE_DATE[i], df_htd_2.VALUE_DATE[i], money]], columns=['START_DATE', 'END_DATE', 'MONEY']), ignore_index=True)
                maturity_date = df_htd_2.VALUE_DATE[i] + relativedelta(months=24)
                pre_maturity_date = df_htd_2.VALUE_DATE[i] + relativedelta(months=12)
                j = j + 1
                
            elif(df_htd_2.VALUE_DATE[i] == pre_maturity_date):
                
                if(df_htd_2.PART_TRAN_TYPE[i] == 'C'):
                    money = money + round(df_htd_2.TRAN_AMT[i], 2)
                else:
                    money = money - round(df_htd_2.TRAN_AMT[i], 2) 
                
                df.END_DATE[j] = pre_maturity_date
                #df.MONEY[j] = money
                pre_maturity_date = df_htd_2.VALUE_DATE[i] + relativedelta(months=12)
            else:
    
                if(df_htd_2.PART_TRAN_TYPE[i] == 'C'):
                    money = money + round(df_htd_2.TRAN_AMT[i], 2)
                else:
                    money = money - round(df_htd_2.TRAN_AMT[i], 2)
    
                df.END_DATE[j] = df_htd_2.VALUE_DATE[i]
                df.MONEY[j] = money

           
          
    if(df.START_DATE[df.shape[0]-1] == df.END_DATE[df.shape[0]-1]):
        df = df.drop(df.shape[0]-1)
     
    if(df.START_DATE[df.shape[0]-1] + relativedelta(months=+24) != df.END_DATE[df.shape[0]-1] and df.START_DATE[df.shape[0]-1] + relativedelta(months=+12) != df.END_DATE[df.shape[0]-1]):
        if(df_htd_2.PART_TRAN_TYPE[df_htd_2.shape[0]-1] == 'C'):
            df.MONEY[df.shape[0]-1] = df.MONEY[df.shape[0]-1] - df_htd_2.TRAN_AMT[df_htd_2.shape[0]-1]
        else:
            df.MONEY[df.shape[0]-1] = df.MONEY[df.shape[0]-1] + df_htd_2.TRAN_AMT[df_htd_2.shape[0]-1]
    
    return df
    
    
# combine the htd record that has the value date
def reduce_htd():
    
    try:
        df = pd.DataFrame(columns=['VALUE_DATE', 'DEL_FLG', 'TRAN_AMT', 'PART_TRAN_TYPE'])
        j = 0
        
        date = 0
        
        for i in range(df_htd.shape[0]):
            
            
            temp = df_htd.loc[df_htd.VALUE_DATE == df_htd.VALUE_DATE[i]]
            
            if(date != temp.VALUE_DATE.iloc[0]):
                date = temp.VALUE_DATE.iloc[0]
                
                if(temp.shape[0] > 1):
                    
                    money = 0
                    for j in range(temp.shape[0]):
                        
                        if(temp.DEL_FLG.iloc[j] == 'N'):
                        
                            if(temp.PART_TRAN_TYPE.iloc[j] == 'C'):
                                money = money + round(temp.TRAN_AMT.iloc[j], 2)
                            else:
                                money = money - round(temp.TRAN_AMT.iloc[j], 2)
                            
                    df = df.append(pd.DataFrame([[temp.VALUE_DATE.iloc[0], 'N', math.fabs(money), 'C' if money >= 0 else 'D']], columns=['VALUE_DATE', 'DEL_FLG', 'TRAN_AMT', 'PART_TRAN_TYPE']), ignore_index=True)
                    
                else:
                    if(temp.DEL_FLG.iloc[j] == 'N'):
                        df = df.append(pd.DataFrame([[temp.VALUE_DATE.iloc[0], 'N', round(temp.TRAN_AMT.iloc[0], 2), temp.PART_TRAN_TYPE.iloc[0]]], columns=['VALUE_DATE', 'DEL_FLG', 'TRAN_AMT', 'PART_TRAN_TYPE']), ignore_index=True)
    
                
        return df
    except:
        return df_htd
  

# create transaction to use instead of directly use htd  
def create_tda():
    
    # special case
    if(df_gam.SCHM_CODE[0] == 'FD006'):
        return create_tda_fd006()
    elif(df_gam.SCHM_CODE[0] in tda_special_table.SCHM_CODE.tolist()):
        return create_tda_every_month()
        
    df_htd_2 = reduce_htd()
    
    maturity_date = df_htd_2.VALUE_DATE[0] + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0]) 
    money = 0
    j = 0
    
    
    df = pd.DataFrame(columns=['START_DATE', 'END_DATE', 'MONEY'])
    
    # exception
    if(df_tam.DEPOSIT_PERIOD_MTHS[0] == 0):
        return df
        
    df = df.append(pd.DataFrame([[df_htd_2.VALUE_DATE[0], df_htd_2.VALUE_DATE[0], round(df_htd_2.TRAN_AMT[0], 2)]], columns=['START_DATE', 'END_DATE', 'MONEY']), ignore_index=True)
    first_date = df_htd_2.VALUE_DATE[0]
    
    for i in range(df_htd_2.shape[0]):
        
        if(df_htd_2.DEL_FLG[i] == 'N'):
            
            # special case when it is the last record
            if(df_htd_2.shape[0]-1 == i):
                
                # current date more than maturity date
                if(df_htd_2.VALUE_DATE[i] >= maturity_date):
                    
                    
                    if(df_htd_2.PART_TRAN_TYPE[i] == 'C'):
                        money = money + round(df_htd_2.TRAN_AMT[i], 2)
                    else:
                        money = money - round(df_htd_2.TRAN_AMT[i], 2)
                    df.END_DATE[j] = maturity_date
                    first_date = df_htd_2.VALUE_DATE[i]
                    df = df.append(pd.DataFrame([[maturity_date, df_htd_2.VALUE_DATE[i], money]], columns=['START_DATE', 'END_DATE', 'MONEY']), ignore_index=True)
                    j = j + 1
                    maturity_date = maturity_date + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0])
                    while(maturity_date <= df_htd_2.VALUE_DATE[i]):
                        
                        if(df_htd_2.PART_TRAN_TYPE[i] == 'C'):
                            df.MONEY[j] = money - round(df_htd_2.TRAN_AMT[i], 2)
                        else:
                            df.MONEY[j] = money + round(df_htd_2.TRAN_AMT[i], 2)
                        df.END_DATE[j] = maturity_date
                        first_date = df_htd_2.VALUE_DATE[i]
                        df= df.append(pd.DataFrame([[maturity_date, df_htd_2.VALUE_DATE[i], df.MONEY[j-1]]], columns=['START_DATE', 'END_DATE', 'MONEY']), ignore_index=True)
                        j = j + 1
                        maturity_date = maturity_date + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0])
                    
                    if(df_gam.SCHM_CODE[0] in tda_special_table.SCHM_CODE.tolist()):
                
                        if(df_htd_2.PART_TRAN_TYPE[i] == 'C'):
                            df.MONEY[j] = money - round(df_htd_2.TRAN_AMT[i], 2)
                        else:
                            df.MONEY[j] = money + round(df_htd_2.TRAN_AMT[i], 2)
                    else:
                        df.MONEY[j] = money
                # current date less than maturity date
                else:
                    
                        
                    df.END_DATE[j] = df_htd_2.VALUE_DATE[i]
            
            # current date less than maturity date
            elif(df_htd_2.VALUE_DATE[i] < maturity_date):
                
                
                if(df_htd_2.PART_TRAN_TYPE[i] == 'C'):
                    money = money + round(df_htd_2.TRAN_AMT[i], 2)
                else:
                    money = money - round(df_htd_2.TRAN_AMT[i], 2)
                
                if(df_htd_2.VALUE_DATE[i] == first_date):
                    df.MONEY[j] = money
                elif(df.MONEY[j] > money):
                    df.MONEY[j] = money
                df.END_DATE[j] = df_htd_2.VALUE_DATE[i]
           
            # current date more than maturity date
            else:
                
                #print 'three'
                
                if(df_htd_2.PART_TRAN_TYPE[i] == 'C'):
                    money = money + round(df_htd_2.TRAN_AMT[i], 2)
                else:
                    money = money - round(df_htd_2.TRAN_AMT[i], 2)
                    
                df.END_DATE[j] = maturity_date
                first_date = df_htd_2.VALUE_DATE[i]
                df= df.append(pd.DataFrame([[maturity_date, df_htd_2.VALUE_DATE[i], money]], columns=['START_DATE', 'END_DATE', 'MONEY']), ignore_index=True)
                j = j + 1
                maturity_date = maturity_date + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0])
        
                while(maturity_date <= df_htd_2.VALUE_DATE[i]):
                    
                    if(df_htd_2.PART_TRAN_TYPE[i] == 'C'):
                        df.MONEY[j] = money - round(df_htd_2.TRAN_AMT[i], 2)
                    else:
                        df.MONEY[j] = money + round(df_htd_2.TRAN_AMT[i], 2)
                    #df.MONEY[j-1] = another_money
                    df.END_DATE[j] = maturity_date
                    first_date = df_htd_2.VALUE_DATE[i]
                    df= df.append(pd.DataFrame([[maturity_date, df_htd_2.VALUE_DATE[i], df.MONEY[j-1]]], columns=['START_DATE', 'END_DATE', 'MONEY']), ignore_index=True)
                    j = j + 1
                    maturity_date = maturity_date + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0])
                
                if(df_gam.SCHM_CODE[0] in tda_special_table.SCHM_CODE.tolist()):
                
                    if(df_htd_2.PART_TRAN_TYPE[i] == 'C'):
                        df.MONEY[j] = money - round(df_htd_2.TRAN_AMT[i], 2)
                    else:
                        df.MONEY[j] = money + round(df_htd_2.TRAN_AMT[i], 2)
                else:
                    df.MONEY[j] = money
        
    if(df.START_DATE[df.shape[0]-1] >= df.END_DATE[df.shape[0]-1]):
        df = df.drop(df.shape[0]-1)
    
   
    return df

# create int adm for special schm code    
def create_int_adm_every_month():
  
    # find first maturity date
    start_date = df_tam.MATURITY_DATE[0] + relativedelta(months=-df_tam.DEPOSIT_PERIOD_MTHS[0])
    while(start_date+relativedelta(months=+df_tam.DEPOSIT_PERIOD_MTHS[0]) >= datetime.datetime.now()+relativedelta(months=-6)):
        start_date = start_date + relativedelta(months=-df_tam.DEPOSIT_PERIOD_MTHS[0])

    df =  pd.DataFrame(columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT'])
    j = 0
    
    base_amount_I = 0
    base_amount_T = 0
    
    df = df.append(pd.DataFrame([[start_date, 'I', base_amount_I]], columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT']), ignore_index=True)
    df = df.append(pd.DataFrame([[start_date, 'T', base_amount_T]], columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT']), ignore_index=True)
    
    
    for i in range(df_int_adm.shape[0]):
        
        if(start_date < df_int_adm.VALUE_DATE[i] and df_int_adm.VALUE_DATE[i] <= start_date+relativedelta(months=+df_tam.DEPOSIT_PERIOD_MTHS[0])):
            
            
            if(df_int_adm.CR_OR_DR_AMT_IND[i] == 'C' and df_int_adm.RECORD_TYPE[i] == 'I'):
                base_amount_I = base_amount_I + round(df_int_adm.BASE_AMOUNT[i], 2)
            elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'D' and df_int_adm.RECORD_TYPE[i] == 'I'):
                base_amount_I = base_amount_I - round(df_int_adm.BASE_AMOUNT[i], 2)
            elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'D' and df_int_adm.RECORD_TYPE[i] == 'T'):
                base_amount_T = base_amount_T + round(df_int_adm.BASE_AMOUNT[i], 2)
            elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'C' and df_int_adm.RECORD_TYPE[i] == 'T'):
                base_amount_T = base_amount_T - round(df_int_adm.BASE_AMOUNT[i], 2)
            df.VALUE_DATE[0] = df_int_adm.VALUE_DATE[i]
            df.VALUE_DATE[1] = df_int_adm.VALUE_DATE[i]
        elif(df_int_adm.VALUE_DATE[i] > start_date+relativedelta(months=+df_tam.DEPOSIT_PERIOD_MTHS[0])):
            
            df = df.append(pd.DataFrame([[df_int_adm.VALUE_DATE[i], df_int_adm.RECORD_TYPE[i], round(df_int_adm.BASE_AMOUNT[i], 2)]], columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT']), ignore_index=True)
    
            
    df.BASE_AMOUNT[0] = base_amount_I
    df.BASE_AMOUNT[1] = base_amount_T
    
    return df

# create int adm
def create_int_adm():
    

    df =  pd.DataFrame(columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT'])
    
    # exception and special case
    if(df_tam.DEPOSIT_PERIOD_MTHS[0] == 0):
        return df
    elif(df_int_adm.shape[0] == 0):
        return df
    elif(df_gam.SCHM_CODE[0] in tda_special_table.SCHM_CODE.tolist()):
        return create_int_adm_every_month()  
        
    # find first maturity date  
    maturity_date = df_htd.VALUE_DATE[0]
    while(maturity_date + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0]) < df_int_adm.VALUE_DATE[0]):
        maturity_date = maturity_date + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0])

     
    base_amount_I = 0
    base_amount_T = 0
    for i in range(df_int_adm.shape[0]):
        
        if(df_int_adm.shape[0]-1 == i):

            if(df_int_adm.VALUE_DATE[i] <= maturity_date):
                
                if(df_int_adm.CR_OR_DR_AMT_IND[i] == 'C' and df_int_adm.RECORD_TYPE[i] == 'I'):
                    base_amount_I = base_amount_I + round(df_int_adm.BASE_AMOUNT[i], 2)
                elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'D' and df_int_adm.RECORD_TYPE[i] == 'I'):
                    base_amount_I = base_amount_I - round(df_int_adm.BASE_AMOUNT[i], 2)
                elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'D' and df_int_adm.RECORD_TYPE[i] == 'T'):
                    base_amount_T = base_amount_T + round(df_int_adm.BASE_AMOUNT[i], 2)
                elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'C' and df_int_adm.RECORD_TYPE[i] == 'T'):
                    base_amount_T = base_amount_T - round(df_int_adm.BASE_AMOUNT[i], 2)
                
                df= df.append(pd.DataFrame([[df_int_adm.VALUE_DATE[i], 'I', base_amount_I]], columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT']), ignore_index=True)
                df= df.append(pd.DataFrame([[df_int_adm.VALUE_DATE[i], 'T', base_amount_T]], columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT']), ignore_index=True)
            else:
                df= df.append(pd.DataFrame([[maturity_date, 'I', base_amount_I]], columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT']), ignore_index=True)
                df= df.append(pd.DataFrame([[maturity_date, 'T', base_amount_T]], columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT']), ignore_index=True)
                
                base_amount_I = 0
                base_amount_T = 0
                
                maturity_date = maturity_date + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0])
                while(maturity_date + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0]) <= df_int_adm.VALUE_DATE[i]):
                    maturity_date = maturity_date + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0])
                    
                #maturity_date = maturity_date - relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0])
                
                if(df_int_adm.CR_OR_DR_AMT_IND[i] == 'C' and df_int_adm.RECORD_TYPE[i] == 'I'):
                    base_amount_I = base_amount_I + round(df_int_adm.BASE_AMOUNT[i], 2)
                elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'D' and df_int_adm.RECORD_TYPE[i] == 'I'):
                    base_amount_I = base_amount_I - round(df_int_adm.BASE_AMOUNT[i], 2)
                elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'D' and df_int_adm.RECORD_TYPE[i] == 'T'):
                    base_amount_T = base_amount_T + round(df_int_adm.BASE_AMOUNT[i], 2)
                elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'C' and df_int_adm.RECORD_TYPE[i] == 'T'):
                    base_amount_T = base_amount_T - round(df_int_adm.BASE_AMOUNT[i], 2)
                
                df= df.append(pd.DataFrame([[df_int_adm.VALUE_DATE[i], 'I', base_amount_I]], columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT']), ignore_index=True)
                df= df.append(pd.DataFrame([[df_int_adm.VALUE_DATE[i], 'T', base_amount_T]], columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT']), ignore_index=True)
        
        elif(df_int_adm.VALUE_DATE[i] <= maturity_date):

            if(df_int_adm.CR_OR_DR_AMT_IND[i] == 'C' and df_int_adm.RECORD_TYPE[i] == 'I'):
                base_amount_I = base_amount_I + round(df_int_adm.BASE_AMOUNT[i], 2)
            elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'D' and df_int_adm.RECORD_TYPE[i] == 'I'):
                base_amount_I = base_amount_I - round(df_int_adm.BASE_AMOUNT[i], 2)
            elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'D' and df_int_adm.RECORD_TYPE[i] == 'T'):
                base_amount_T = base_amount_T + round(df_int_adm.BASE_AMOUNT[i], 2)
            elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'C' and df_int_adm.RECORD_TYPE[i] == 'T'):
                base_amount_T = base_amount_T - round(df_int_adm.BASE_AMOUNT[i], 2)
        else:

            df= df.append(pd.DataFrame([[maturity_date, 'I', base_amount_I]], columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT']), ignore_index=True)
            df= df.append(pd.DataFrame([[maturity_date, 'T', base_amount_T]], columns=['VALUE_DATE', 'RECORD_TYPE', 'BASE_AMOUNT']), ignore_index=True)
            
            base_amount_I = 0
            base_amount_T = 0
            
            maturity_date = maturity_date + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0])
            while(maturity_date + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0]) <= df_int_adm.VALUE_DATE[i]):
                maturity_date = maturity_date + relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0])
                
            #maturity_date = maturity_date - relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0])
     
            
            if(df_int_adm.CR_OR_DR_AMT_IND[i] == 'C' and df_int_adm.RECORD_TYPE[i] == 'I'):
                base_amount_I = base_amount_I + df_int_adm.BASE_AMOUNT[i]
            elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'D' and df_int_adm.RECORD_TYPE[i] == 'I'):
                base_amount_I = base_amount_I - df_int_adm.BASE_AMOUNT[i]
            elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'D' and df_int_adm.RECORD_TYPE[i] == 'T'):
                base_amount_T = base_amount_T + df_int_adm.BASE_AMOUNT[i]
            elif(df_int_adm.CR_OR_DR_AMT_IND[i] == 'C' and df_int_adm.RECORD_TYPE[i] == 'T'):
                base_amount_T = base_amount_T - df_int_adm.BASE_AMOUNT[i]
    
    
    return df

# find the first money of schm code FD006    
def find_first_money(start_date):
    
    df_htd_2 = reduce_htd()
    money = 0
    
    for i in range(df_htd_2.shape[0]):
        
        if(df_htd_2.VALUE_DATE[i] <= start_date):
            
            if(df_htd_2.PART_TRAN_TYPE[i] == 'C'):
                money = money + round(df_htd_2.TRAN_AMT[i], 2)
            else:
                money = money - round(df_htd_2.TRAN_AMT[i], 2)
                
      
    return money

# check across year fact for given period of date      
def check_cross_year(start_date, end_date):
    
    if(start_date.year != end_date.year):
        
        years = np.arange(start_date.year, end_date.year+1)
    
        for i in range(years.shape[0]): 
            
            if(years[i] % 4 == 0):
                return True
                
    return False

# get special interest rate of given date     
def get_special_rate(start_date):
    
    special_rate = df_itc.ID_CR_PREF_PCNT[0]
    for i in range(df_itc.shape[0]):
        if(df_itc.START_DATE[i] <= start_date and start_date <= df_itc.END_DATE[i]):
            special_rate = df_itc.ID_CR_PREF_PCNT[i]

    return special_rate
    
# get interest rate of given date and money    
def get_rate(money, start_date):
    
    special_rate = get_special_rate(start_date)

    int_version = 0
    
    # get interest version
    for i in range(df_icv.shape[0]):
        temp = df_icv.START_DATE[i]

        if temp <= start_date:

            if(int(df_icv.INT_VERSION[i]) > int_version):
                int_version = int(df_icv.INT_VERSION[i])
                
    # get interest rate
    for i in range(df_tvs.shape[0]):
        if int(df_tvs.INT_TBL_VER_NUM[i]) == int_version:

            if(money >= df_tvs.BEGIN_SLAB_AMT[i] and money < df_tvs.MAX_SLAB_AMT[i]):

                return df_tvs.NRML_INT_PCNT[i] + special_rate
                
                
# calculate the duration of the given period date as number               
def get_month(start_date, end_date):
    
    end_date = end_date + relativedelta(days=+1)

    month = 0
    if(end_date.year - start_date.year != 0):
        month = month + (end_date.year - start_date.year) * 12

    if(end_date.month - start_date.month != 0):
        month = month + (end_date.month - start_date.month)
        
    if(end_date.day - start_date.day < 0):

        month = month - 1
        month = month + float((datetime.date(end_date.year, end_date.month, end_date.day) - datetime.date(end_date.year, 12 if (end_date.month-1 == 0) else end_date.month-1, start_date.day)).days) / calendar.monthrange((end_date+relativedelta(days=-1)).year, (end_date+relativedelta(days=-1)).month)[1]
        
    if(end_date.day - start_date.day > 0):

         month = month + float((datetime.date(end_date.year, end_date.month, end_date.day) - datetime.date(end_date.year, end_date.month, start_date.day)).days) / calendar.monthrange((end_date+relativedelta(days=-1)).year, (end_date+relativedelta(days=-1)).month)[1]

    return month
    

# get penalty interest rate    
def get_penalty(money, start_date, end_date):
    
    int_version = 0
    
    # get interest versoin
    for i in range(df_icv.shape[0]):
        temp = df_icv.START_DATE[i]

        if temp <= start_date:

            if(int(df_icv.INT_VERSION[i]) > int_version):
                int_version = int(df_icv.INT_VERSION[i])
            
    # get interest rate
    for i in range(df_tvs.shape[0]):
        if int(df_tvs.INT_TBL_VER_NUM[i]) == int_version:

            if(money >= df_tvs.BEGIN_SLAB_AMT[i] and money < df_tvs.MAX_SLAB_AMT[i]):

                return df_tvs.PENAL_PCNT[i]
    
    
# get pegged_flg fact for floating interest                
def get_pegged_flg(start_date):
    
    
    for i in range(df_itc.shape[0]-1,-1,-1):
        
        if(start_date >= df_itc.START_DATE[i] and start_date < df_itc.END_DATE[i]):
            return df_itc.PEGGED_FLG[i]
            
    return 'Y'

# calculate interest acroos year
def calculate_interest_cross_year(money, rate, start_date, end_date):
    
    years = np.arange(start_date.year, end_date.year+1)
    res = 0
    total_res = 0
    for i in range(years.shape[0]):
        
        if(i == 0):
            cal_start_date = start_date
            cal_end_date = datetime.datetime(start_date.year, 12, 31, 0, 0)
        elif(i == years.shape[0]-1):
            cal_start_date = datetime.datetime(end_date.year, 1, 1, 0, 0)
            cal_end_date = end_date
        else:
            cal_start_date = datetime.datetime(years[i], 1, 1, 0, 0)
            cal_end_date = datetime.datetime(years[i], 12, 31, 0, 0)
        
        
        res = cal_int(money, rate, cal_start_date, cal_end_date)
        total_res = total_res + res
        
    return total_res
        
# calculate interest             
def calculate_interest(money, rate, start_date, end_date):

    end_date = end_date - datetime.timedelta(days=1)
    
    if(check_cross_year(start_date, end_date) and df_gam.SCHM_CODE[0] != 'FD006'):
        return calculate_interest_cross_year(money, rate, start_date, end_date)
    else:
        return cal_int(money, rate, start_date, end_date)

# calculate interest
def cal_int(money, rate, start_date, end_date):
    
    # exception
    if(start_date > end_date):
        return 0
    
    # special case
    if(df_gam.SCHM_CODE[0] == 'FD006' and start_date + relativedelta(days=3) < end_date):
        day_in_year = 12
        days = get_month(start_date, end_date)
    else:
        day_in_year = 366 if (start_date.year % 4 == 0) else 365
        days = float((end_date - start_date).days+1)
    
    money = float(money)
    rate = float(rate) / 100
    
    day_in_year = float(day_in_year)
    res = (money*rate*days)/day_in_year
    
    if(round_boolean):
        return math.fabs(round(res,2))
    else:
        return math.fabs(res)
 

# check floating interest      
def check_int_overlap(start_date, end_date):
    
    int_version_start = 0
    int_version_end = 0
    
    for i in range(df_icv.shape[0]):

        if df_icv.START_DATE[i] <= start_date:

            if(int(df_icv.INT_VERSION[i]) > int_version_start):
                int_version_start = int(df_icv.INT_VERSION[i])
               
    for i in range(df_icv.shape[0]):

        if df_icv.START_DATE[i] <= end_date:

            if(int(df_icv.INT_VERSION[i]) > int_version_end):
                int_version_end = int(df_icv.INT_VERSION[i])
    
    return int_version_start != int_version_end
    

# get max interest version    
def get_max_int_version():
    
    int_version = 0
    
    for i in range(df_icv.shape[0]):
        
        if(int(df_icv.INT_VERSION.iloc[i]) > int_version):
            int_version = int(df_icv.INT_VERSION.iloc[i])
    
    return int_version
    
# get floating interest rate
def get_rate_overlap(money, start_date, end_date):
    
    # exception
    if(money < 0):
        return 0
    
    n = 0
    temp_res = {}
    
    start_overlap_date = start_date
    end_overlap_date = start_date
    max_int_version = get_max_int_version()
    int_version = 0
    
    # go through each int version
    while(end_overlap_date < end_date and int_version != max_int_version):
        
        int_version = 0
        
        # get int rate
        for i in range(df_icv.shape[0]):

            if df_icv.START_DATE.iloc[i] <= end_overlap_date:
                if(int(df_icv.INT_VERSION.iloc[i]) > int_version):
                    int_version = int(df_icv.INT_VERSION.iloc[i])
        
        # get the timeline (starting date and ending date) of that int version   
        for i in range(df_icv.shape[0]):
            
            if(int_version +1 == int(df_icv.INT_VERSION.iloc[i])):

                start_overlap_date = end_overlap_date
                end_overlap_date = df_icv.START_DATE.iloc[i]
            elif(int_version == max_int_version):
                start_overlap_date = end_overlap_date
        
        if(start_overlap_date < end_date):
            
            for i in range(df_tvs.shape[0]):
                
                if int(df_tvs.INT_TBL_VER_NUM[i]) == int_version:
                    if(money >= df_tvs.BEGIN_SLAB_AMT[i] and money <= df_tvs.MAX_SLAB_AMT[i]):
                            
                        temp_res[n] = df_tvs.NRML_INT_PCNT[i]
         
        n = n + 1
        
    return temp_res

# calculate floating interest   
def calculate_overlap(money, start_date, end_date):
    

    final_res = 0
    n = 0

    start_overlap_date = start_date
    end_overlap_date = start_date
    max_int_version = get_max_int_version()
    int_version = 0

    # go through each int version

    while(end_overlap_date < end_date and int_version != max_int_version):
        
        int_version = 0
        
        # get int rate
        for i in range(df_icv.shape[0]):

            if df_icv.START_DATE.iloc[i] <= end_overlap_date:
                if(int(df_icv.INT_VERSION.iloc[i]) > int_version):
                    int_version = int(df_icv.INT_VERSION.iloc[i])
        
        # get the timeline (starting date and ending date) of that int version   
        for i in range(df_icv.shape[0]):
            if(int_version +1 == int(df_icv.INT_VERSION.iloc[i])):
                #print 'int version = ', temp_icv.INT_VERSION.iloc[i]
                start_overlap_date = end_overlap_date
                end_overlap_date = df_icv.START_DATE.iloc[i]
            elif(int_version == max_int_version):
                start_overlap_date = end_overlap_date

        if(start_overlap_date < end_date):
            for i in range(df_tvs.shape[0]):

                if int(df_tvs.INT_TBL_VER_NUM[i]) == int_version:
                    if(money >= df_tvs.BEGIN_SLAB_AMT[i] and money <= df_tvs.MAX_SLAB_AMT[i]):

                        # the first int version (the smallest)
                        if(n == 0):
           
                            cal_start_date = start_date
                            cal_end_date = end_overlap_date

                        # after the first but before the next int version
                        elif(end_overlap_date > end_date):

                            cal_start_date = start_overlap_date
                            cal_end_date = end_date

                        # the latest int version (the highest)    
                        elif(int_version == max_int_version):

                            cal_start_date = start_overlap_date
                            cal_end_date = end_date

                        # between first and next int version
                        else:
                                 
                            cal_start_date = start_overlap_date
                            cal_end_date = end_overlap_date



                        res = calculate_interest(money, df_tvs.NRML_INT_PCNT[i], cal_start_date, cal_end_date)
                        
                        if(round_boolean):
                            final_res = final_res + math.fabs(round(res,2))
                        else:
                            final_res = final_res + math.fabs(res)        
                            
        n = n + 1
        
    if(round_boolean):
        return round(final_res, 2)
    else:
        return final_res
        
# check period mth transaction        
def check_multiple_max_period_run_mths(money, start_date):
    
    # special case
    if(df_gam.SCHM_CODE[0] == 'FD006'):
        return False
    
    int_version = 0
    
    for i in range(df_icv.shape[0]):
        temp = df_icv.START_DATE[i]

        if temp <= start_date:

            if(int(df_icv.INT_VERSION[i]) > int_version):
                int_version = int(df_icv.INT_VERSION[i])
                               
    int_table = df_tvs.loc[df_tvs.INT_TBL_VER_NUM == str(int_version).zfill(5)]  

    return True if (int_table.shape[0] > 1) else False

# calculate period mth transaction    
def calculate_multiple_max_period_run_mths(money, start_date, end_date):
    
             
    end_date = end_date - datetime.timedelta(days=1)
        
    int_version = 0
    
    for i in range(df_icv.shape[0]):
        temp = df_icv.START_DATE[i]
        #print icv.START_DATE[i]
        if temp <= start_date:
            #print icv.INT_VERSION[i]
            if(int(df_icv.INT_VERSION[i]) > int_version):
                int_version = int(df_icv.INT_VERSION[i])
                               
    int_table = df_tvs.loc[df_tvs.INT_TBL_VER_NUM == str(int_version).zfill(5)]

    cal_start_date = start_date
    cal_end_date = start_date
    res = 0
    total_res = 0
  
    
    for i in range(int_table.shape[0]):
        
        
        if(i == 0):
            cal_start_date = start_date
            cal_end_date = start_date + relativedelta(months=+int_table.MAX_PERIOD_RUN_MTHS.iloc[i]) + relativedelta(days=-1)
            
        else:
            cal_start_date = start_date + relativedelta(months=+int_table.MAX_PERIOD_RUN_MTHS.iloc[i-1])
            cal_end_date = start_date + relativedelta(months=+int_table.MAX_PERIOD_RUN_MTHS.iloc[i]) + relativedelta(days=-1)
        
        special_rate = get_special_rate(cal_start_date)
        #print cal_start_date, ' ', int_table.NRML_INT_PCNT.iloc[i]+special_rate
        
        
        # if there is more than one rate to select, must filter using MAX_SLAB_AMT
        new_df = int_table.loc[int_table.MAX_PERIOD_RUN_MTHS == int_table.MAX_PERIOD_RUN_MTHS.iloc[i]]

        rate = 0
        
        if(new_df.shape[0] > 1):
            
            for k in range(new_df.shape[0]):
                
                if(new_df.MAX_SLAB_AMT.iloc[k] > money):
                    rate = new_df.NRML_INT_PCNT.iloc[k] + special_rate
                    break;


        else:
            rate = int_table.NRML_INT_PCNT.iloc[i]+special_rate
        
        #print rate
        if(i == int_table.shape[0]-1 and cal_start_date < end_date):
            
            
            
            if(check_cross_year(cal_start_date, end_date)):
                res = calculate_interest_cross_year(money, rate, cal_start_date, end_date)
            else:
                res = cal_int(money, rate, cal_start_date, end_date)
            
            total_res = total_res + res
            #print i, 'one', ' ', money, ' ', int_table.NRML_INT_PCNT.iloc[i], ' ', cal_start_date, ' ', end_date, ' ', res
            
        
        elif(cal_end_date <= end_date):
            
            
            
            if(check_cross_year(cal_start_date, cal_end_date)):
                res = calculate_interest_cross_year(money, rate, cal_start_date, cal_end_date)
            else:
                res = cal_int(money, rate, cal_start_date, cal_end_date)
            
            total_res = total_res + res
            #print i, 'two', ' ', money, ' ', int_table.NRML_INT_PCNT.iloc[i], ' ', cal_start_date, ' ', cal_end_date, ' ', res
        
            

        elif(cal_start_date < end_date):
            
            if(check_cross_year(cal_start_date, end_date)):
                res = calculate_interest_cross_year(money, rate, cal_start_date, end_date+relativedelta(days=-1))
            else:
                res = cal_int(money, rate, cal_start_date, end_date+relativedelta(days=-1))
            
            total_res = total_res + res
            #print i, 'three', ' ', money, ' ', int_table.NRML_INT_PCNT.iloc[i], ' ', cal_start_date, ' ', end_date, ' ', res
            
    return total_res
    
        

def run_htd():
    
    df_tda = create_tda()
    res = 0
    total_res = 0
    rate = 0
    
    holiday_int = check_holiday_int()
    
    
    df_new_htd = pd.DataFrame(columns=['FORACID', 'ACID', 'SOL_ID', 'SCHM_CODE', 'ACCT_CLS_FLG', 'CLR_BAL_AMT', 'ACCT_OPN_DATE', 'ACCT_CLS_DATE', 'INT_TBL_CODE', 'MONEY', 'START_DATE', 'END_DATE', 'INTEREST'])
       
    
    for i in range(df_tda.shape[0]):
        
        
        if(relativedelta(df_tda.END_DATE[i], df_tda.START_DATE[i]) == relativedelta(months=df_tam.DEPOSIT_PERIOD_MTHS[0])):
            #print i
            if(check_multiple_max_period_run_mths(df_tda.MONEY[i], df_tda.START_DATE[i])):

                res = calculate_multiple_max_period_run_mths(df_tda.MONEY[i], df_tda.START_DATE[i], df_tda.END_DATE[i]) 
                #print 'one', money, ' ', start_date, ' ', end_date, ' ',res
                

            elif(get_pegged_flg(df_tda.START_DATE[i]) == 'N'):

                if(check_int_overlap(df_tda.START_DATE[i], df_tda.END_DATE[i])):
                     
                     res = calculate_overlap(df_tda.MONEY[i], df_tda.START_DATE[i], df_tda.END_DATE[i])
                     #print money, ' ', start_date, ' ', end_date, ' ',res

                else:
                    
                    rate = get_rate(df_tda.MONEY[i], df_tda.START_DATE[i])
                    res = calculate_interest(df_tda.MONEY[i], rate, df_tda.START_DATE[i], df_tda.END_DATE[i]) 
                    
            else:
              
                rate = get_rate(df_tda.MONEY[i], df_tda.START_DATE[i])
                if(df_gam.SCHM_CODE[0] == 'FD006'):

                    res = calculate_interest(df_tda.MONEY[i], rate, df_tda.START_DATE[i], df_tda.END_DATE[i]+relativedelta(years=-1)) 
                    #print 'one', df_tda.MONEY[i], ' ', rate, ' ', df_tda.START_DATE[i], ' ', df_tda.END_DATE[i]+relativedelta(years=-1), ' ',res
        
                    res = res + calculate_interest(df_tda.MONEY[i]+res, rate, df_tda.START_DATE[i]+relativedelta(years=+1), df_tda.END_DATE[i]) 
                    #print 'one', df_tda.MONEY[i]+res, ' ', rate, ' ', df_tda.START_DATE[i]+relativedelta(years=+1), ' ', df_tda.END_DATE[i], ' ',res
        
                    
                else:
                    res = calculate_interest(df_tda.MONEY[i], rate, df_tda.START_DATE[i], df_tda.END_DATE[i]) 
                

                
                #print 'one', df_tda.MONEY[i], ' ', rate, ' ', df_tda.START_DATE[i], ' ', df_tda.END_DATE[i], ' ',res
        elif(df_tda.START_DATE[i] + relativedelta(months=+3) > df_tda.END_DATE[i]):
        #relativedelta(df_tda.END_DATE[i], df_tda.START_DATE[i]) >= relativedelta(months=3)):
            #print 'two ', df_tda.END_DATE[i], ' ',df_tda.START_DATE[i]
            
            if(holiday_int):

                extra_money = find_holidy_extra_money(df_tda.END_DATE[i-1])
                
                if(df_gam.SCHM_CODE[0] == 'FD006' and df_tda.END_DATE[i-1] + relativedelta(years=-2) == df_tda.START_DATE[i-1]):
                    extra_money = extra_money + find_holidy_extra_money(df_tda.END_DATE[i-1] + relativedelta(years=-1))

                rate = get_rate(df_tda.MONEY[i-1]+extra_money, df_tda.START_DATE[i-1]) - get_special_rate(df_tda.START_DATE[i-1]) + get_special_rate(df_tda.START_DATE[i])
                res = calculate_interest(df_tda.MONEY[i-1]+extra_money, rate, df_tda.START_DATE[i], df_tda.END_DATE[i]) 
                #print 'one', df_tda.MONEY[i-1],'+',extra_money, ' ', rate, ' ', df_tda.START_DATE[i], ' ', df_tda.END_DATE[i], ' ',res
        
            else:
                rate = 0
                res = 0
        else:
            #print 'three ', df_tda.END_DATE[i], ' ',df_tda.START_DATE[i]
            
            if(df_gam.SCHM_CODE[0] == 'FD006'):
                
                if(df_tda.START_DATE[i] + relativedelta(months=+12) == df_tda.END_DATE[i]):
                    
                    rate = get_rate(df_tda.MONEY[i], df_tda.START_DATE[i])
                    res = calculate_interest(df_tda.MONEY[i], rate, df_tda.START_DATE[i], df_tda.START_DATE[i]+relativedelta(years=+1))
                    #print df_tda.MONEY[i], ' ', df_tda.START_DATE[i], ' ', df_tda.END_DATE[i], ' ', rate, ' ', res

                elif(df_tda.START_DATE[i] + relativedelta(months=+12) < df_tda.END_DATE[i]):
                    
                    df_tda.MONEY[i] = find_first_money(df_tda.START_DATE[i])
                    rate = get_rate(df_tda.MONEY[i], df_tda.START_DATE[i])
                    res = calculate_interest(df_tda.MONEY[i], rate, df_tda.START_DATE[i], df_tda.START_DATE[i]+relativedelta(years=+1))
                    print 'one', df_tda.MONEY[i], ' ', rate, ' ', df_tda.START_DATE[i], ' ', df_tda.START_DATE[i]+relativedelta(years=+1), ' ',res

                    rate = get_penalty(df_tda.MONEY[i], df_tda.START_DATE[i], df_tda.END_DATE[i]) 
                    penal_res_1 = calculate_interest(df_tda.MONEY[i], rate, df_tda.START_DATE[i], df_tda.START_DATE[i]+relativedelta(years=+1))
                    print 'one', df_tda.MONEY[i], ' ', rate, ' ', df_tda.START_DATE[i], ' ', df_tda.START_DATE[i]+relativedelta(years=+1), ' ',penal_res_1
                    
                    penal_res_2 = penal_res_1 + calculate_interest(df_tda.MONEY[i]+penal_res_1, rate, df_tda.START_DATE[i]+relativedelta(years=+1), df_tda.END_DATE[i])
                    print 'one', df_tda.MONEY[i]+penal_res_1, ' ', rate, ' ', df_tda.START_DATE[i]+relativedelta(years=+1), ' ', df_tda.END_DATE[i], ' ',penal_res_2

                    res = penal_res_2
                else:
                    rate = get_penalty(df_tda.MONEY[i], df_tda.START_DATE[i], df_tda.END_DATE[i]) 
                    res = calculate_interest(df_tda.MONEY[i], rate, df_tda.START_DATE[i], df_tda.END_DATE[i])
                    #print df_tda.START_DATE[i], ' ', df_tda.END_DATE[i], ' ', rate, ' ', res
        
            else:
                rate = get_penalty(df_tda.MONEY[i], df_tda.START_DATE[i], df_tda.END_DATE[i]) 
                res = calculate_interest(df_tda.MONEY[i], rate, df_tda.START_DATE[i], df_tda.END_DATE[i]) 
        
                #print df_tda.MONEY[i], ' ', df_tda.START_DATE[i], ' ', df_tda.END_DATE[i], ' ', rate, ' ', res
        
        df_new_htd = df_new_htd.append(pd.DataFrame([[df_gam.FORACID[0],
                                                      df_gam.ACID[0],
                                                      df_gam.SOL_ID[0],
                                                      df_gam.SCHM_CODE[0],
                                                      df_gam.ACCT_CLS_FLG[0],
                                                      df_gam.CLR_BAL_AMT[0],
                                                      df_gam.ACCT_OPN_DATE[0],
                                                      df_gam.ACCT_CLS_DATE[0],
                                                      df_idt.INT_TABLE_CODE[0],
                                                      df_tda.MONEY[i],
                                                      df_tda.START_DATE[i].strftime("%m/%d/%Y"),
                                                      df_tda.END_DATE[i].strftime("%m/%d/%Y"),
                                                      res]],
                                                      columns=['FORACID', 'ACID', 'SOL_ID', 'SCHM_CODE', 'ACCT_CLS_FLG', 'CLR_BAL_AMT', 'ACCT_OPN_DATE', 'ACCT_CLS_DATE', 'INT_TBL_CODE', 'MONEY', 'START_DATE', 'END_DATE', 'INTEREST']),
                                                      ignore_index=True)            
        #print df_tda.MONEY[i], ' ', rate, ' ', df_tda.START_DATE[i], ' ', df_tda.END_DATE[i], ' ',res
        total_res = total_res + res
        
  
    return df_new_htd
   

def find_holidy_extra_money(date):
    date = date + relativedelta(days=-1)
    string = date.strftime("%d-%m-%Y")

    for i in reversed(range(df_htd.shape[0])):
        if(type(df_htd.TRAN_PARTICULAR[i]) == str):

            if(df_htd.TRAN_PARTICULAR[i].find(string) != -1):
                return float(df_htd.TRAN_AMT[i])
                
    return 0
                    
 
def run_int_adm(df):

    df_int = create_int_adm()    
    df_new_int_adm = pd.DataFrame(columns=['FORACID', 'ACID', 'SOL_ID', 'SCHM_CODE', 'ACCT_CLS_FLG',
                                           'CLR_BAL_AMT', 'ACCT_CLS_DATE', 'ACCT_OPN_DATE', 'INT_TBL_CODE',
                                           'VALUE_DATE', 'RECORD_TYPE', 'INTEREST', 'SYSTEM_AMOUNT', 
                                           'POSITIVE_DIFF', 'NEGATIVE_DIFF'])
    
    for i in range(df.shape[0]):
        
        end_date = datetime.datetime.strptime(df.END_DATE[i], '%m/%d/%Y')
       
        temp = df_int.loc[df_int.VALUE_DATE == end_date]
        if(temp.shape[0] == 1):
            system_amount = temp.BASE_AMOUNT.iloc[0]       
        elif(temp.shape[0] > 1):
            system_amount = temp.loc[temp.RECORD_TYPE == 'I'].BASE_AMOUNT.iloc[0]
        else:
            system_amount = 0
        

        diff = 0 if(abs(df.INTEREST[i] - system_amount) <= 0.01) else (df.INTEREST[i] - system_amount)
        
        if(diff > 0):
            df_new_int_adm = df_new_int_adm.append(pd.DataFrame([[df.FORACID[0], df.ACID[0], df.SOL_ID[0],
                                                              df.SCHM_CODE[0], df.ACCT_CLS_FLG[0], df.CLR_BAL_AMT[0],
                                                              df.ACCT_CLS_DATE[0], df.ACCT_OPN_DATE[0], df.INT_TBL_CODE[0],
                                                              df.END_DATE[i].encode('utf8'), 'I', df.INTEREST[i], system_amount, 
                                                              diff, 0]], columns=['FORACID', 'ACID', 'SOL_ID', 'SCHM_CODE', 'ACCT_CLS_FLG',
                                                               'CLR_BAL_AMT', 'ACCT_CLS_DATE', 'ACCT_OPN_DATE', 'INT_TBL_CODE',
                                                               'VALUE_DATE', 'RECORD_TYPE', 'INTEREST', 'SYSTEM_AMOUNT', 
                                                               'POSITIVE_DIFF', 'NEGATIVE_DIFF']), ignore_index=True)
            
        elif(diff < 0):
            df_new_int_adm = df_new_int_adm.append(pd.DataFrame([[df.FORACID[0], df.ACID[0], df.SOL_ID[0],
                                                              df.SCHM_CODE[0], df.ACCT_CLS_FLG[0], df.CLR_BAL_AMT[0],
                                                              df.ACCT_CLS_DATE[0], df.ACCT_OPN_DATE[0], df.INT_TBL_CODE[0],
                                                              df.END_DATE[i].encode('utf8'), 'I', df.INTEREST[i], system_amount,
                                                              0, diff]], columns=['FORACID', 'ACID', 'SOL_ID', 'SCHM_CODE', 'ACCT_CLS_FLG',
                                                               'CLR_BAL_AMT', 'ACCT_CLS_DATE', 'ACCT_OPN_DATE', 'INT_TBL_CODE',
                                                               'VALUE_DATE', 'RECORD_TYPE', 'INTEREST', 'SYSTEM_AMOUNT', 
                                                               'POSITIVE_DIFF', 'NEGATIVE_DIFF']), ignore_index=True)

        else:
            df_new_int_adm = df_new_int_adm.append(pd.DataFrame([[df.FORACID[0], df.ACID[0], df.SOL_ID[0],
                                                              df.SCHM_CODE[0], df.ACCT_CLS_FLG[0], df.CLR_BAL_AMT[0],
                                                              df.ACCT_CLS_DATE[0], df.ACCT_OPN_DATE[0], df.INT_TBL_CODE[0],
                                                              df.END_DATE[i].encode('utf8'), 'I', df.INTEREST[i], system_amount,
                                                              0, 0]], columns=['FORACID', 'ACID', 'SOL_ID', 'SCHM_CODE', 'ACCT_CLS_FLG',
                                                               'CLR_BAL_AMT', 'ACCT_CLS_DATE', 'ACCT_OPN_DATE', 'INT_TBL_CODE',
                                                               'VALUE_DATE', 'RECORD_TYPE', 'INTEREST', 'SYSTEM_AMOUNT', 
                                                               'POSITIVE_DIFF', 'NEGATIVE_DIFF']), ignore_index=True)

            
        if(temp.shape[0] > 1):

            df_new_int_adm = df_new_int_adm.append(pd.DataFrame([[df.FORACID[0], df.ACID[0], df.SOL_ID[0],
                                                              df.SCHM_CODE[0], df.ACCT_CLS_FLG[0], df.CLR_BAL_AMT[0],
                                                              df.ACCT_CLS_DATE[0], df.ACCT_OPN_DATE[0], df.INT_TBL_CODE[0],
                                                              df.END_DATE[i].encode('utf8'), 'T', 0, temp.loc[temp.RECORD_TYPE == 'T'].BASE_AMOUNT.iloc[0],
                                                              0, 0]], columns=['FORACID', 'ACID', 'SOL_ID', 'SCHM_CODE', 'ACCT_CLS_FLG',
                                                               'CLR_BAL_AMT', 'ACCT_CLS_DATE', 'ACCT_OPN_DATE', 'INT_TBL_CODE',
                                                               'VALUE_DATE', 'RECORD_TYPE', 'INTEREST', 'SYSTEM_AMOUNT', 
                                                               'POSITIVE_DIFF', 'NEGATIVE_DIFF']), ignore_index=True)
        
            
    return df_new_int_adm
        
def run_service(db1, job_id, job_start_date_time, df_input):
    
        
    for i in range(df_input.shape[0]):
        try:
            connect_database(db1, df_input.FORACID[i])
            df_run_htd = run_htd()
            
            df_run_int_adm = run_int_adm(df_run_htd)
            
            res = df_run_int_adm.POSITIVE_DIFF.sum() + df_run_int_adm.NEGATIVE_DIFF.sum()
            if(res != 0):
                sql = '''
                    INSERT INTO RES_DWS_01E_RESULT
                    VALUES(:FORACID, :ACID, :SOL_ID, :SCHM_CODE, :ACCT_CLS_FLG, :CAL_BAL_AMT, :ACCT_OPN_DATE,
                    :ACCT_CLS_DATE, '{3}', TO_DATE('{1}','{2}'), '{0}')
                
                '''.format(job_id, job_start_date_time, g_date_format, res)
                
                df = df_run_int_adm.drop(['INT_TBL_CODE', 'VALUE_DATE', 'RECORD_TYPE', 'INTEREST', 'SYSTEM_AMOUNT', 'POSITIVE_DIFF', 'NEGATIVE_DIFF'], 1)
                df['ACCT_OPN_DATE'] = pd.to_datetime(df['ACCT_OPN_DATE'])
                df['ACCT_CLS_DATE'] = pd.to_datetime(df['ACCT_CLS_DATE'])
                
                db1.update_dataframes(sql, [df.loc[0].values.tolist()])
        except Exception as inst:
            print df_input.FORACID[i]
            print inst
 
    
if __name__ == '__main__':

    #print len(sys.argv)
    if (len(sys.argv) <= 1):
        sys.exit("ERROR:Not found JOB_ID \nEx " + sys.argv[0] + " <JOB_ID>")

    job_id = sys.argv[1]
    process_id = os.getpid()
    
    print "Start Script ...DWS_01D_0001"
    print "Process ID = " + str(process_id)
    
    print 'new query obj'
    db1 = Query_obj(cif_config.username, cif_config.password, cif_config.txtcnt)
    
    
    # Get SYSDATE for START_DATE
    print 'start get sysdate'
    sql = "SELECT TO_CHAR(SYSDATE,'" + g_date_format + "') FROM DUAL"
    
    try:
        job_start_date_time = db1.get_scalar(sql)
    except:
        print "Unexpected error:", sys.exc_info()[0]
    
    
    # STAMP START JOB
    sql = "UPDATE JOB_TRN_BACKEND \
           SET STATUS='P' \
             , PROGRESS='0' \
             , JOB_START_DATE_TIME=TO_DATE('" + job_start_date_time + "','" + g_date_format + "') \
             , PROCESS_ID='" + str(process_id) + "'  \
           WHERE JOB_KEY = '" + str(job_id) + "' "
    #print sql
    db1.update_rows(sql)
    
    sql = 'select * from int_dws_01e_input'
    df_input = pd.DataFrame(db1.get_rows(sql).fetchall(), columns=['FORACID'])

    run_service(db1, job_id, job_start_date_time, df_input)
    

    # STAMP FINISH JOB
    sql = "UPDATE JOB_TRN_BACKEND \
           SET STATUS='S' \
             , PROGRESS='100' \
             , JOB_END_DATE_TIME=SYSDATE \
           WHERE JOB_KEY = '" + str(job_id) + "' "
    #print sql
    cur = db1.update_rows(sql)
