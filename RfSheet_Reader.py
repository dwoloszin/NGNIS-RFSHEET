import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import timeit
import time
import warnings



def processArchive():
    warnings.simplefilter(action='ignore', category=UserWarning)
    print("\nProcessing " + os.path.basename(__file__) + '...')
    timeexport = time.strftime("%Y%m%d_")
    inicio = timeit.default_timer()
    pathImport = '/import/RFSHEET'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    #csv_path = os.path.join(script_dir, 'export/'+ 'RFSHEET' +'/'+ timeexport + 'RFSHEET_All'+'.csv')
    csv_path = os.path.join(script_dir, 'export/'+ 'RFSHEET' +'/' + 'RFSHEET_All'+'.csv')
    
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.xlsm")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    df = pd.DataFrame()
    indexw = 0
    for filename in all_filesSI:
        arquivoName = filename[77:]
        print(arquivoName)
        indexw +=1
        print(indexw,'/',len(all_filesSI))
        #indexw +=1
        data = pd.read_excel(filename,skiprows=27,sheet_name = 'RFSHEET', nrows=52,usecols = 'A:AC',engine='openpyxl')
        data.columns = ['Site','Setor','STATUS','Tipo','"Altura(M)"','Azimute (NV)','Tilt Mec','Modelo de Antena','Gain (dBi)','TRX Power (W)','Tilt Elt','Modelo do TRX','Banda','Diplexer1','Diplexer2','EiRP','Triplexer1','Triplexer2','FILTER','Cabo RFS 1/2 SF - Cu (metros)','Cabo RFS 7/8 - Al (metros)','COMBINER (EHCU)','Cabo RFS 1 5/8 - Al (metros)','1/2"','5/8" / 7/8"','Perda(EHCU)_Perda_do_Filter','Dip/Trip','Total da linha','TEC']
        data = data.dropna(subset=['Site'])
        data.insert(0,'Arquivo',arquivoName)
        #data = data.loc[data['STATUS'].isin(statusList)]
        df = df.append(data,ignore_index=True)    

    #Verificar os casos IoT
    df.insert(3,'Setor2',df['Setor'])
    modelosAntenaAir = ['Radio AIR 1641']
    for index, row in df.iterrows():
      #print(row['Setor'])
      if str(row['Setor'][-2:-1]) == 'I' and len(str(row['Setor'])) > 14:
          df.at[index,'Setor2'] = str(row['Setor'][:-2]) + str(row['Setor'][-1:])
      if str(row['Setor'][-3:-2]) == 'I' and len(str(row['Setor'])) > 14:
          df.at[index,'Setor2'] = str(row['Setor'][:-3]) + str(row['Setor'][-2:])
          
      if row['TEC'] in ['2100 UMTS'] and row['Setor2'][:2] == 'SP' and str(row['Setor2'][-1:]) in ['I','J','K','1','2','3']:
          df.at[index,'STATUS'] = 'VERIFICAR'

      if row['Modelo de Antena'] in modelosAntenaAir or row['Modelo do TRX'] in modelosAntenaAir:
          df.at[index,'STATUS'] = 'VERIFICAR'
  
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    df.to_csv(csv_path,index=True,header=True,sep=';')

    fim = timeit.default_timer()
    print ('duracao: %f' % ((fim - inicio)/60) + ' min') 





