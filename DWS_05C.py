# -*- coding: utf-8 -*-
"""
Created on Mon Jun 26 22:33:39 2017

@author: ghb
"""

import sys
import os
import pandas as pd
import numpy as np
from Query_obj import Query_obj
from checkFlag import DataValidation
from timeit import default_timer as timer
import dws_config
import cx_Oracle
import datetime
import cif_config
from dateutil.relativedelta import relativedelta
from timeit import default_timer as timer
DBHOST = dws_config.DBHOST
DBPORT = dws_config.DBPORT
DBSID = dws_config.DBSID
DBUSER = dws_config.DBUSER
DBPASS = dws_config.DBPASS
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
g_date_format = "YYYY-MM-DDHH24MISS"
insert_date_format = "DD/MM/YYYY"
table = 'res_dws_05c_result'
def to_tyr(row):
    row["MATURITY_DATE"] = row["MATURITY_DATE"] + relativedelta(years=+543)   
    #row["MATURITY_DATE"] = row["MATURITY_DATE"].strftime('%Y-%m-%d%H%M%S')
    return row
def transform_iforacid(row):
    row = str(row)[:-4]    
    return row

if __name__ == '__main__':
    #print len(sys.argv)
    if (len(sys.argv) <= 1):
        sys.exit("ERROR:Not found JOB_ID \nEx " + sys.argv[0] + " <JOB_ID>")

    job_id = sys.argv[1]
    process_id = os.getpid()
    
    print "Start Script ...DWS_05C "
    print "Process ID = " + str(process_id)
    
    print 'Create query obj'
    db1 = Query_obj(cif_config.username,cif_config.password,cif_config.txtcnt)
    
    
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

    start = timer()
    print("Start Checking AAS")
    joint = DataValidation(pd.DataFrame(), "SOL_ID,FORACID,ACCT_NAME,SCHM_TYPE,SCHM_CODE,CIF_M,CIF_J2,CIF_J3,CIF_J4,CIF_J5,CIF_J6,CIF_J7,CIF_J8,CIF_J9", "tbaadm", "GAM")
    joint.checkjoint()
    joint.df['key'] = joint.df['FORACID'].map(transform_iforacid)
    joint.df['Wrong_CIF_M'] = np.where(joint.df.groupby('key').CIF_M.transform('nunique') > 1, 'T', '')
    joint.df['Wrong_CIF_J2'] = np.where(joint.df.groupby('key').CIF_J2.transform('nunique') > 1, 'T', '')
    joint.df['Wrong_CIF_J3'] = np.where(joint.df.groupby('key').CIF_J3.transform('nunique') > 1, 'T', '')
    joint.df['Wrong_CIF_J4'] = np.where(joint.df.groupby('key').CIF_J4.transform('nunique') > 1, 'T', '')
    joint.df['Wrong_CIF_J5'] = np.where(joint.df.groupby('key').CIF_J5.transform('nunique') > 1, 'T', '')
    joint.df['Wrong_CIF_J6'] = np.where(joint.df.groupby('key').CIF_J6.transform('nunique') > 1, 'T', '')
    joint.df['Wrong_CIF_J7'] = np.where(joint.df.groupby('key').CIF_J7.transform('nunique') > 1, 'T', '')
    joint.df['Wrong_CIF_J8'] = np.where(joint.df.groupby('key').CIF_J8.transform('nunique') > 1, 'T', '')
    joint.df['Wrong_CIF_J9'] = np.where(joint.df.groupby('key').CIF_J9.transform('nunique') > 1, 'T', '')
    joint.df = joint.df[ (joint.df.Wrong_CIF_M == 'T') | (joint.df.Wrong_CIF_J2 == 'T') | (joint.df.Wrong_CIF_J3 == 'T') 
    | (joint.df.Wrong_CIF_J4 == 'T') | (joint.df.Wrong_CIF_J5 == 'T') | (joint.df.Wrong_CIF_J6 == 'T') 
    | (joint.df.Wrong_CIF_J7 == 'T') | (joint.df.Wrong_CIF_J8 == 'T') | (joint.df.Wrong_CIF_J9 == 'T')]
    joint.df.drop(['key','Wrong_CIF_M','Wrong_CIF_J2','Wrong_CIF_J3','Wrong_CIF_J4','Wrong_CIF_J5','Wrong_CIF_J6','Wrong_CIF_J7','Wrong_CIF_J8','Wrong_CIF_J9'], axis=1, inplace=True)
    joint.df = joint.df.sort_values('FORACID')
    mask = joint.df.applymap(lambda x: x is None)
    cols = joint.df.columns[(mask).any()]
    for col in joint.df[cols]:
        joint.df.loc[mask[col], col] = ''
    
    joint.df = joint.df.reset_index(drop=True)
    for i in range(joint.df.shape[0]):
    #for i in range(0, 100):    
        sql_2 = '''
        INSERT INTO RES_DWS_05C_RESULT
        select sol_desc, '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}', '{14}', '{15}', '{16}', '{0}', TO_DATE('{1}','{2}')
        from
        (select sol_desc, sol_id from tbaadm.sol 
        where sol_id = '{3}')
        '''.format(job_id,job_start_date_time, g_date_format,joint.df.SOL_ID[i],joint.df.FORACID[i],joint.df.ACCT_NAME[i],joint.df.SCHM_TYPE[i],
        joint.df.SCHM_CODE[i],joint.df.CIF_M[i],joint.df.CIF_J2[i],joint.df.CIF_J3[i],joint.df.CIF_J4[i],joint.df.CIF_J5[i],joint.df.CIF_J6[i],joint.df.CIF_J7[i],joint.df.CIF_J8[i],joint.df.CIF_J9[i])
        db1.update_rows(sql_2)
        if i%20 == 0:
            print "Done: "+ str(i)
    end = timer()
    joint.elaspedTime = end - start
    print( "AAS Completed, Time Elapsed : " + str(joint.elaspedTime) + " seconds")
    # STAMP FINISH JOB
    sql = "UPDATE JOB_TRN_BACKEND \
           SET STATUS='S' \
             , PROGRESS='100' \
             , JOB_END_DATE_TIME=SYSDATE \
           WHERE JOB_KEY = '" + str(job_id) + "' "
    #print sql
    cur = db1.update_rows(sql)
    