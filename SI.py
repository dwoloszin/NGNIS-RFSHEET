import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
from datetime import datetime
import timeit

def processArchive():
    print("\nProcessing " + os.path.basename(__file__) + '...')
    inicio = timeit.default_timer()
    fields = ['REGIONAL','LOCATION','LATITUDE','LONGITUDE','CS_NAME','CS_STATUS','AZIMUTH','ALTURA','MS_TYPE', 'SI_PORT_BAND','MECHANICAL_TILT','ANTENA_MODEL','AMD_TILT_E','CST_NAME']
    pathImport = '/import/SI'
    filtrolabel = ['REGIONAL','CS_STATUS','CST_NAME']
    filtroValue = ['TSP','In Service']#'Implementation','Implementation '
    filtroValue2 = ['GSM 1800','GSM 900','LTE 1800','LTE 2100','LTE 2600','LTE 700','UMTS 2100','UMTS 850','UMTS 900','NR 2600','NR 3500','NR 700']
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/SI/'+archiveName+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    lastData = all_filesSI[0][len(all_filesSI[0])-19:len(all_filesSI[0])-11]
    for filename in all_filesSI:
        dataArchive = filename[len(pathImportSI)+14:len(filename)-11]
        iter_csv = pd.read_csv(filename, index_col=None,header=0, error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
        #df = pd.concat([chunk[(chunk[filtrolabel[0]].isin([filtroValue[0]])) & (chunk[filtrolabel[1]].isin([filtroValue[1]])) & (chunk[filtrolabel[2]].isin(filtroValue2))] for chunk in iter_csv]) # WORKS
        #df = pd.concat([chunk[(chunk[filtrolabel[0]].isin([filtroValue[0]])) & (chunk[filtrolabel[2]].isin(filtroValue2))] for chunk in iter_csv]) # WORKS
        df = pd.concat([chunk[(chunk[filtrolabel[0]].isin([filtroValue[0]]))] for chunk in iter_csv]) # WORKS
        
        df2 = df[fields] # ordering labels   
        li.append(df2)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI = frameSI.drop_duplicates()
    
    # limpa celulas vazias
    #frameSI = frameSI.dropna(subset = ['MS_TYPE', 'LOCATION', 'SI_PORT_BAND','CS_NAME']) #algumas celulas não estão cadastrando a freq no campo SI_PORT_BAND
    frameSI = frameSI.dropna(subset = ['MS_TYPE', 'LOCATION','CS_NAME'])

    for index, row in frameSI.iterrows(): 
        if pd.notna(row["MS_TYPE"]):
            row["MS_TYPE"] = row["MS_TYPE"][len(row["MS_TYPE"])-2:len(row["MS_TYPE"])]
            row['LATITUDE'] = str(row['LATITUDE']).replace('.',',')
            row['LONGITUDE'] = str(row['LONGITUDE']).replace('.',',')
            #row["LATITUDE"] = row["LATITUDE"][:3] + "," + row["LATITUDE"][4:len(row["LATITUDE"])].replace('.','')
            #row["LONGITUDE"] = row["LONGITUDE"][:3] + "," + row["LONGITUDE"][4:len(row["LONGITUDE"])].replace('.','')

    frameSI = frameSI.drop_duplicates()
    frameSI = frameSI.reset_index(drop=True)
    frameSI.to_csv(csv_path,index=True,header=True,sep=';')
    fim = timeit.default_timer()
    print ('duracao: %f' % ((fim - inicio)/60) + ' min')


