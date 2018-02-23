# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 11:10:43 2017

@author: DQ2
"""

import cx_Oracle
import pandas as pd
import datetime
import math
from dateutil.relativedelta import relativedelta
import sys
import os
import cif_config
from Query_obj import Query_obj

os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"

g_date_format = "YYYY-MM-DDHH24MISS"

insert_date_format = "DD/MM/YYYY"

# global dataframe
df_gam = pd.DataFrame()
df_htd = pd.DataFrame()
df_int_adm = pd.DataFrame()
df_idt = pd.DataFrame()
df_itc = pd.DataFrame()
df_icv = pd.DataFrame()
df_ivs = pd.DataFrame()

# rounding number fact
round_boolean = False

# interest period gap fact
start_date_int_pd = False
end_date_int_pd = False

# connect oracle to select data into dataframe
def connect_database(db1, foracid): 
    
    global df_gam
    global df_htd
    global df_int_adm
    global df_idt
    global df_itc
    global df_icv
    global df_ivs
      
    gam_sql = 'select foracid, acid, sol_id, schm_code, acct_cls_flg, clr_bal_amt, acct_cls_date, acct_opn_date from tbaadm.gam where foracid = \''+foracid+'\''
       

    df_gam = pd.DataFrame(db1.get_rows(gam_sql).fetchall(), columns=['FORACID', 'ACID', 'SOL_ID', 'SCHM_CODE', 'ACCT_CLS_FLG', 'CLR_BAL_AMT', 'ACCT_CLS_DATE', 'ACCT_OPN_DATE'])
    
    htd_sql = 'select tran_id, del_flg, part_tran_type, acid, value_date, tran_amt, tran_particular from tbaadm.htd where acid = \''+df_gam.ACID[0]+'\''
    int_adm_sql = 'select acid, record_type, base_amount, value_date from tbaadm.int_adm where acid = \''+df_gam.ACID[0]+'\''
    idt_sql = 'select entity_id, product_for_int_rate, interest_amount, int_table_code, start_date, end_date, last_comp_date, last_comp_amount from tbaadm.idt where entity_id = \''+df_gam.ACID[0]+'\''     
    itc_sql = 'select entity_id, int_tbl_code_srl_num, start_date, end_date, id_cr_pref_pcnt, pegged_flg from tbaadm.itc where entity_id = \''+df_gam.ACID[0]+'\''
    

    df_htd = pd.DataFrame(db1.get_rows(htd_sql).fetchall(), columns=['TRAN_ID', 'DEL_FLG', 'PART_TRAN_TYPE', 'ACID', 'VALUE_DATE', 'TRAN_AMT', 'TRAN_PARTICULAR'])
    df_htd = df_htd.sort_values(['VALUE_DATE']).reset_index(drop=True)    
    
    df_int_adm = pd.DataFrame(db1.get_rows(int_adm_sql).fetchall(), columns=['ACID', 'RECORD_TYPE', 'BASE_AMOUNT', 'VALUE_DATE'])
    

    df_idt = pd.DataFrame(db1.get_rows(idt_sql).fetchall(), columns=['ENTITY_ID', 'PRODUCT_FOR_INT_RATE', 'INTEREST_AMOUNT', 'INT_TABLE_CODE', 'START_DATE', 'END_DATE', 'LAST_COMP_DATE','LAST_COMP_AMOUNT'])
    
    df_itc = pd.DataFrame(db1.get_rows(itc_sql).fetchall(), columns=['ENTITY_ID', 'INT_TBL_CODE_SRL_NUM', 'START_DATE', 'END_DATE', 'ID_CR_PREF_PCNT', 'PEGGED_FLG'])
   
    icv_sql = 'select int_tbl_code, start_date, int_version from tbaadm.icv where int_tbl_code = \''+df_idt.INT_TABLE_CODE[0]+'\''
    ivs_sql = 'select int_tbl_code, int_tbl_ver_num, begin_slab_amt, end_slab_amt, nrml_int_pcnt from tbaadm.ivs where int_tbl_code = \''+df_idt.INT_TABLE_CODE[0]+'\''
    
    df_icv = pd.DataFrame(db1.get_rows(icv_sql).fetchall(), columns=['INT_TBL_CODE', 'START_DATE', 'INT_VERSION'])
    
    df_ivs = pd.DataFrame(db1.get_rows(ivs_sql).fetchall(), columns=['INT_TBL_CODE', 'INT_TBL_VER_NUM', 'BEGIN_SLAB_AMT', 'END_SLAB_AMT', 'NRML_INT_PCNT'])
    
    rearrange_int_pd_date()
    
    
# sort 29/6/YYYY and 29/12/YYYY at df_htd.RCRE_TIME to balance int.pd flag
def rearrange_int_pd_date():
    
    last_update_date = 0
    
    for i in range(df_htd.shape[0]):
        
         if((df_htd.VALUE_DATE[i].month == 6 and df_htd.VALUE_DATE[i].day == 30) or (df_htd.VALUE_DATE[i].month == 12 and df_htd.VALUE_DATE[i].day == 31)):    
             
             if(last_update_date != df_htd.VALUE_DATE[i]):
                 #print df_htd.VALUE_DATE[i]
                 
                 temp = df_htd.loc[df_htd.VALUE_DATE == df_htd.VALUE_DATE[i]]
                 
                 if(temp.shape[0] > 1):
                     
                     for j in range(temp.shape[0]):
                         
                         if(check_int_pd(i+j)):
                             
                             df_swap = df_htd.loc[i+j]
                             df_htd.loc[i+j] = df_htd.loc[i+temp.shape[0]-1]
                             df_htd.loc[i+temp.shape[0]-1] = df_swap
                             
                         
                 last_update_date = df_htd.VALUE_DATE[i]
             

# create dataframe of the last_comp_amount
# it will add an extra value into the interest result   
def create_last_comp_amount():
    
    df_comp_amount = pd.DataFrame(columns=['START_DATE', 'LAST_COMP_AMOUNT'])
    last_comp_date = ''
    start_date = ''
    
    for i in range(df_idt.shape[0]):
        
        if(df_idt.LAST_COMP_DATE[i] != None and df_idt.LAST_COMP_DATE[i] != last_comp_date):
            #print df_idt.LAST_COMP_DATE[i], ' ', df_idt.LAST_COMP_AMOUNT[i]
            if(df_idt.LAST_COMP_DATE[i].month == 6):
                start_date = df_idt.LAST_COMP_DATE[i]+relativedelta(months=6)
            elif(df_idt.LAST_COMP_DATE[i].month == 12):
                start_date = df_idt.LAST_COMP_DATE[i]+relativedelta(months=6)
        
            df_comp_amount = df_comp_amount.append(pd.DataFrame([[start_date, df_idt.LAST_COMP_AMOUNT[i]]], columns=['START_DATE', 'LAST_COMP_AMOUNT']), ignore_index=True)
            
            last_comp_date = df_idt.LAST_COMP_DATE[i]
    return df_comp_amount
    

# check if there is a different in the number of day in year between two date of one transaction or not
def check_cross_year(start_date, end_date):
    
    if(start_date.year != end_date.year):
        if((366 if (start_date.year % 4 == 0) else 365) == (366 if (end_date.year % 4 == 0) else 365)):
            return False
        else:
            return True
    else:
        return False

# get a special interest rate of the given date
def get_special_rate(start_date):
    
    special_rate = df_itc.ID_CR_PREF_PCNT[0]
    for i in range(df_itc.shape[0]):
        if(df_itc.START_DATE[i] <= start_date and start_date <= df_itc.END_DATE[i]):
            special_rate = df_itc.ID_CR_PREF_PCNT[i]
            #print special_rate
    return special_rate

# get an interest rate of the given date and money
def get_rate(money, start_date):
    
    # get special interest rate
    special_rate = get_special_rate(start_date)
    
    # exception
    if(money < 0):
        return 0

    int_version = 0
    
    # get interest version
    for i in range(df_icv.shape[0]):
        temp = df_icv.START_DATE[i]

        if temp <= start_date:

            if(int(df_icv.INT_VERSION[i]) > int_version):
                int_version = int(df_icv.INT_VERSION[i])
    
    # get interest rate
    for i in range(df_ivs.shape[0]):
        if int(df_ivs.INT_TBL_VER_NUM[i]) == int_version:
            if(money >= df_ivs.BEGIN_SLAB_AMT[i] and money <= df_ivs.END_SLAB_AMT[i]):
                return df_ivs.NRML_INT_PCNT[i] + special_rate
    
# check the special floating interest on the given period of date
def check_special_int_overlap(start_date, end_date):

    special_rate_start = df_itc.ID_CR_PREF_PCNT[0]
    special_rate_end = df_itc.ID_CR_PREF_PCNT[0]
    
    for i in range(df_itc.shape[0]):
        
        if(df_itc.START_DATE[i] <= start_date):
            special_rate_start = df_itc.ID_CR_PREF_PCNT[i]
            
        if(df_itc.START_DATE[i] <= end_date):
            special_rate_end = df_itc.ID_CR_PREF_PCNT[i]

    return special_rate_start != special_rate_end
            
# check the floating interest on the given period of date
def check_int_overlap(start_date, end_date):
    
    int_version_start = 0
    int_version_end = 0
    
    # get the first and last interest version
    for i in range(df_icv.shape[0]):

        if df_icv.START_DATE[i] <= start_date:

            if(int(df_icv.INT_VERSION[i]) > int_version_start):
                int_version_start = int(df_icv.INT_VERSION[i])
                
    # get the first and last interest rate and then compare them           
    for i in range(df_icv.shape[0]):

        if df_icv.START_DATE[i] <= end_date:

            if(int(df_icv.INT_VERSION[i]) > int_version_end):
                int_version_end = int(df_icv.INT_VERSION[i])
    
    return int_version_start != int_version_end

# get maximum interest version                
def get_max_int_version():
    

    int_version = 0
    
    for i in range(df_icv.shape[0]):
        
        if(int(df_icv.INT_VERSION.iloc[i]) > int_version):
            int_version = int(df_icv.INT_VERSION.iloc[i])
    
    return int_version

# calculate the interest across two year (number of days may not equal)    
def calculate_interest_cross_year(money, rate, start_date, end_date):
    
    first_start_date = start_date
    first_end_date = datetime.datetime(first_start_date.year, 12, 31, 0, 0)

    second_end_date = end_date
    second_start_date = datetime.datetime(second_end_date.year, 1, 1, 0, 0)
    
    # calculate first year
    res1 = cal_int(money, rate, first_start_date, first_end_date)
    # calculate second year
    res2 = cal_int(money, rate, second_start_date, second_end_date)

    return res1 + res2
    
# calculate the floating interest if there is a special rate
def calculate_special_int_overlap(money, rate, start_date, end_date):
    
    special_rate_start = df_itc.ID_CR_PREF_PCNT[0]
    special_rate_end = df_itc.ID_CR_PREF_PCNT[0]
    overlap_date = df_itc.START_DATE[0]
    
    for i in range(df_itc.shape[0]):
        if(df_itc.START_DATE[i] <= start_date):
            special_rate_start = df_itc.ID_CR_PREF_PCNT[i]
            
        if(df_itc.START_DATE[i] <= end_date):
            overlap_date = df_itc.START_DATE[i]
            special_rate_end = df_itc.ID_CR_PREF_PCNT[i]
            
    first_start_date = start_date
    first_end_date = overlap_date - datetime.timedelta(days=1)

    second_end_date = end_date
    second_start_date = overlap_date

    res1 = cal_int(money, rate, first_start_date, first_end_date)
    res2 = cal_int(money, rate-special_rate_start+special_rate_end, second_start_date, second_end_date)
    
    return res1 + res2
    
# check interest period at 29/6/xxxx or 29/12/xxxx    
def check_int_pd(i):
    
    if(i < 0):
        return False
    
    if(type(df_htd.TRAN_PARTICULAR[i]) == str):
        if(df_htd.TRAN_PARTICULAR[i].find('Int.Pd') != -1):
            return True
            
    return False
                
     
def calculate_interest(money, rate, start_date, end_date):
    
    global start_date_int_pd
    global end_date_int_pd

    if((start_date.month == 6 and start_date.day == 30) or (start_date.month == 12 and start_date.day == 31)):
        if(start_date_int_pd):
            start_date = start_date + datetime.timedelta(days=1)
         
         # remove date 29/6 and 29/12 from the end date by backward one day
    if(not(((end_date.month == 6 and end_date.day == 30) or (end_date.month == 12 and end_date.day == 31)) and end_date_int_pd)):  
        end_date = end_date - datetime.timedelta(days=1)

    #print start_date, ' ', end_date
    
    if(end_date < start_date):
        if(check_cross_year(start_date- datetime.timedelta(days=1), end_date)):
            return calculate_interest_cross_year(money, rate, end_date, start_date- datetime.timedelta(days=1))
        else:
            return cal_int(money, rate, end_date, start_date- datetime.timedelta(days=1))

    if(check_cross_year(start_date, end_date)):
        return calculate_interest_cross_year(money, rate, start_date, end_date)
    else:
        return cal_int(money, rate, start_date, end_date)
    
# calcalte interest  
def cal_int(money, rate, start_date, end_date):
    
    # exception
    if(start_date > end_date):

        return 0
    
    day_in_year = 366 if (start_date.year % 4 == 0) else 365
    money = float(money) - 10000
    rate = float(rate) / 100
    days = float((end_date - start_date).days+1)
    day_in_year = float(day_in_year)     
    res = (money*rate*days)/day_in_year
    
    if(round_boolean):
        return math.fabs(round(res,2))
    else:
        return math.fabs(res)

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
            
            for i in range(df_ivs.shape[0]):
                
                if int(df_ivs.INT_TBL_VER_NUM[i]) == int_version:
                    if(money >= df_ivs.BEGIN_SLAB_AMT[i] and money <= df_ivs.END_SLAB_AMT[i]):
                            
                        temp_res[n] = df_ivs.NRML_INT_PCNT[i]
         
        n = n + 1
        
    return temp_res
    
# calculate floatint interest
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
            for i in range(df_ivs.shape[0]):

                if int(df_ivs.INT_TBL_VER_NUM[i]) == int_version:
                    if(money >= df_ivs.BEGIN_SLAB_AMT[i] and money <= df_ivs.END_SLAB_AMT[i]):

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

                        res = calculate_interest(money, df_ivs.NRML_INT_PCNT[i]+get_special_rate(cal_start_date), cal_start_date, cal_end_date)

                        if(round_boolean):
                            final_res = final_res + math.fabs(round(res,2))
                        else:
                            final_res = final_res + math.fabs(res)
                            
        n = n + 1
        
    if(round_boolean):
        return round(final_res, 2)
    else:
        return final_res
        
        
# creat int_adm from the run_htd() function
def run_int_adm(df):
    
    df_comp_amount = create_last_comp_amount()
    res = 0
    
    df_new_int_adm = pd.DataFrame(columns = ['FORACID', 'ACID', 'SOL_ID', 'SCHM_CODE', 'ACCT_CLS_FLG', 'CAL_BAL_AMT', 'ACCT_OPN_DATE',
                                             'ACCT_CLS_DATE', 'INT_TBL_CODE', 'VALUE_DATE', 'RECORD_TYPE', 'INTEREST', 'SYSTEM_AMOUNT', 
                                             'POSITIVE_DIFF', 'NEGATIVE_DIFF'])
       
    for i in range(df.shape[0]):
        
        date = datetime.datetime.strptime(df.END_DATE[i], '%m/%d/%Y')
        #date = df.END_DATE[i]
        
        if(i+1 < df.shape[0]):
            next_date = datetime.datetime.strptime(df.END_DATE[i+1], '%m/%d/%Y')
            #next_date = df.END_DATE.iloc[i+1]
        
        # exclude 2007
        if(date.year != 2007):
        
            res = res + float(df.INTEREST[i])
            
            # check for the date of 29/06/xxxx or 29/12/xxxx for the period gap
            if((((date.month == 6 and date.day == 30) or (date.month == 12 and date.day == 31)) and not(next_date.month == date.month and next_date.day == date.day)) or (((date.month == 6 or date.month == 12) and date.day == 29) and i == df.shape[0]-1)):
        
            
                
                # check last_comp_amount
                if(not df_comp_amount.loc[df_comp_amount.START_DATE == date].empty):
                    res = res + df_comp_amount.loc[df_comp_amount.START_DATE == date].LAST_COMP_AMOUNT.iloc[0]

                #worksheet.write(index, 11, res)
                
                temp = df_int_adm.loc[df_int_adm.VALUE_DATE == date]
                if(temp.shape[0] == 1):
                    system_amount = temp.BASE_AMOUNT.iloc[0]
                    
                elif(temp.shape[0] > 1):
                    system_amount = temp.loc[temp.RECORD_TYPE == 'I'].BASE_AMOUNT.iloc[0]
                else:
                    system_amount = 0
                
                
                
                diff = 0 if(abs(res - system_amount) <= 0.01) else (res - system_amount)
                
                if(diff > 0):
                    df_new_int_adm = df_new_int_adm.append(pd.DataFrame([[df.FORACID[0], df.ACID[0], df.SOL_ID[0], df.SCHM_CODE[0],
                                                                      df.ACCT_CLS_FLG[0], df.CLR_BAL_AMT[0], df.ACCT_CLS_DATE[0],
                                                                      df.ACCT_OPN_DATE[0], df.INT_TBL_CODE[0], df.END_DATE[i].encode('utf8'),
                                                                      'I', res, system_amount, diff, 0]], columns = ['FORACID', 'ACID', 'SOL_ID',
                                                                      'SCHM_CODE', 'ACCT_CLS_FLG', 'CAL_BAL_AMT', 'ACCT_OPN_DATE', 'ACCT_CLS_DATE',
                                                                      'INT_TBL_CODE', 'VALUE_DATE', 'RECORD_TYPE', 'INTEREST', 'SYSTEM_AMOUNT', 
                                                                      'POSITIVE_DIFF', 'NEGATIVE_DIFF']), ignore_index=True)
                
                elif(diff < 0):
                    df_new_int_adm = df_new_int_adm.append(pd.DataFrame([[df.FORACID[0], df.ACID[0], df.SOL_ID[0], df.SCHM_CODE[0],
                                                                      df.ACCT_CLS_FLG[0], df.CLR_BAL_AMT[0], df.ACCT_CLS_DATE[0],
                                                                      df.ACCT_OPN_DATE[0], df.INT_TBL_CODE[0], df.END_DATE[i].encode('utf8'),
                                                                      'I', res, system_amount, 0, diff]], columns = ['FORACID', 'ACID', 'SOL_ID',
                                                                      'SCHM_CODE', 'ACCT_CLS_FLG', 'CAL_BAL_AMT', 'ACCT_OPN_DATE', 'ACCT_CLS_DATE',
                                                                      'INT_TBL_CODE', 'VALUE_DATE', 'RECORD_TYPE', 'INTEREST', 'SYSTEM_AMOUNT', 
                                                                      'POSITIVE_DIFF', 'NEGATIVE_DIFF']), ignore_index=True)
                

                else:
                    df_new_int_adm = df_new_int_adm.append(pd.DataFrame([[df.FORACID[0], df.ACID[0], df.SOL_ID[0], df.SCHM_CODE[0],
                                                                      df.ACCT_CLS_FLG[0], df.CLR_BAL_AMT[0], df.ACCT_CLS_DATE[0],
                                                                      df.ACCT_OPN_DATE[0], df.INT_TBL_CODE[0], df.END_DATE[i].encode('utf8'),
                                                                      'I', res, system_amount, 0, 0]], columns = ['FORACID', 'ACID', 'SOL_ID',
                                                                      'SCHM_CODE', 'ACCT_CLS_FLG', 'CAL_BAL_AMT', 'ACCT_OPN_DATE', 'ACCT_CLS_DATE',
                                                                      'INT_TBL_CODE', 'VALUE_DATE', 'RECORD_TYPE', 'INTEREST', 'SYSTEM_AMOUNT', 
                                                                      'POSITIVE_DIFF', 'NEGATIVE_DIFF']), ignore_index=True)
                

                    
                
                
                if(temp.shape[0] > 1):

                    df_new_int_adm = df_new_int_adm.append(pd.DataFrame([[df.FORACID[0], df.ACID[0], df.SOL_ID[0], df.SCHM_CODE[0],
                                                  df.ACCT_CLS_FLG[0], df.CLR_BAL_AMT[0], df.ACCT_CLS_DATE[0],
                                                  df.ACCT_OPN_DATE[0], df.INT_TBL_CODE[0], df.END_DATE[i].encode('utf8'),
                                                  'T', 0, temp.loc[temp.RECORD_TYPE == 'T'].BASE_AMOUNT.iloc[0], 0, 0]], columns = ['FORACID', 'ACID', 'SOL_ID',
                                                  'SCHM_CODE', 'ACCT_CLS_FLG', 'CAL_BAL_AMT', 'ACCT_OPN_DATE', 'ACCT_CLS_DATE',
                                                  'INT_TBL_CODE', 'VALUE_DATE', 'RECORD_TYPE', 'INTEREST', 'SYSTEM_AMOUNT', 
                                                  'POSITIVE_DIFF', 'NEGATIVE_DIFF']), ignore_index=True)


                res = 0

                
    return df_new_int_adm

  
def run_htd():
    
    global start_date_int_pd
    global end_date_int_pd
    
    start_date_int_pd = False
    end_date_int_pd = False
    
    last_date = df_htd.VALUE_DATE[0]
    this_date = df_htd.VALUE_DATE[0]
    interest = 0    # total interest
    money = 0       # totaly balance
    res = 0         # interest of each transaction in htd   
    
    
    df_new_htd = pd.DataFrame(columns=['FORACID', 'ACID', 'SOL_ID', 'SCHM_CODE', 'ACCT_CLS_FLG', 'CLR_BAL_AMT',
                                       'ACCT_OPN_DATE', 'ACCT_CLS_DATE', 'INT_TBL_CODE', 'MONEY', 'START_DATE',
                                       'END_DATE', 'INTEREST'])
    
    
    #for i, ids in enumerate(df_htd.ACID):
    for i in range(df_htd.shape[0]):   
        
        
        if(df_htd.DEL_FLG[i] != 'Y'):
            this_date = df_htd.VALUE_DATE[i]
            
            start_date_int_pd = check_int_pd(i-1)
            end_date_int_pd = check_int_pd(i)

             
            # if the money is on the different day, we will calculate the int of that transaction
            if((this_date != last_date) or (((last_date.month == 6 and last_date.day == 30) or (last_date.month == 12 and last_date.day == 31)) and end_date_int_pd)):
               
                foracid = df_gam.FORACID.iloc[0]
                acid = df_gam.ACID.iloc[0]
                sol_id = df_gam.SOL_ID.iloc[0]
                schm_code = df_gam.SCHM_CODE.iloc[0]
                acct_cls_flg = df_gam.ACCT_CLS_FLG.iloc[0]
                clr_bal_amt = df_gam.CLR_BAL_AMT.iloc[0]
                acct_opn_date = df_gam.ACCT_OPN_DATE.iloc[0].strftime("%m/%d/%Y")
                try:
                    acct_cls_date = df_gam.ACCT_CLS_DATE.iloc[0].strftime("%m/%d/%Y")
                except Exception:
                    acct_cls_date = ''             
                int_table_code = df_idt.INT_TABLE_CODE[0]
                last = last_date.strftime("%m/%d/%Y")
                this = this_date.strftime("%m/%d/%Y")
                               
                
                if(i != 0):
                    
                    start_date = last_date
                    end_date = this_date

                    money = round(money, 2)
                    
                    #print start_date, ' ', end_date, ' ', money
    
                    if(check_int_overlap(start_date, end_date)):
                        
                        
                        
                        rate = get_rate_overlap(money, start_date, end_date)
                        
                        if(len(rate) > 1):
                            if(end_date.year == '2011'):
                                print 'first'
                            res = calculate_overlap(money, start_date, end_date)
                        else:
                            if(end_date.year == '2011'):
                                print 'second'
                            res = calculate_interest(money, rate[0], start_date, end_date)
                        if(end_date.year == '2011'):
                                #print 'second'
                                print start_date, ' ', end_date, ' ', money, ' ', rate, ' ', res, ' ', 'overlap'
                        
                    else:
                                        
                        rate = get_rate(money, start_date)                
                        res = calculate_interest(money, rate, start_date, end_date)
                        

                    interest = interest + res
                    
                    
                # check debit or credit
                df_new_htd = df_new_htd.append(pd.DataFrame([[foracid, acid, sol_id, schm_code, acct_cls_flg, clr_bal_amt,
                                                acct_opn_date, acct_cls_date, int_table_code, money, last,
                                                this, res]],
                                                columns=['FORACID', 'ACID', 'SOL_ID', 'SCHM_CODE', 'ACCT_CLS_FLG',
                                                         'CLR_BAL_AMT', 'ACCT_OPN_DATE', 'ACCT_CLS_DATE', 'INT_TBL_CODE',
                                                         'MONEY', 'START_DATE', 'END_DATE', 'INTEREST']), ignore_index=True)
                
                
                if(df_htd.PART_TRAN_TYPE[i] == 'C'):
                    money = money + df_htd.TRAN_AMT[i]
                else:
                    money = money - df_htd.TRAN_AMT[i]

                last_date = df_htd.VALUE_DATE[i]

                
            else:
    
                if(df_htd.PART_TRAN_TYPE[i] == 'C'):
                    money = money + df_htd.TRAN_AMT[i]
                else:
                    money = money - df_htd.TRAN_AMT[i]

    return df_new_htd
    

def run():
    
    print 'start'
    df_input = pd.DataFrame([['001010017536'], ['001010070186'], ['001010019130']], columns=['FORACID'])
   

    for i in range(df_input.shape[0]):

        connect_database(df_input.FORACID[i])
        df_run_htd = run_htd()
        
        df_run_int_adm = run_int_adm(df_run_htd)
        
        if(df_run_int_adm.POSITIVE_DIFF.sum() + df_run_int_adm.NEGATIVE_DIFF.sum() != 0):
        
            print df_run_int_adm.FORACID[0]


    
def run_service(db1, job_id, job_start_date_time, df_input):
    
        
    for i in range(df_input.shape[0]):
        try:
            connect_database(db1, df_input.FORACID[i])
            df_run_htd = run_htd()
            
            df_run_int_adm = run_int_adm(df_run_htd)
            
            res = df_run_int_adm.POSITIVE_DIFF.sum() + df_run_int_adm.NEGATIVE_DIFF.sum()
            if(res != 0):
                sql = '''
                    INSERT INTO RES_DWS_01C_RESULT
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
    
    print "Start Script ...DWS_01C_0001"
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
    
    sql = 'select * from int_dws_01c_input'
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

