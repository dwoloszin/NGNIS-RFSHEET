import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import timeit
import NormalizeAntenaName
import time



def processArchive():
    print("\nProcessing " + os.path.basename(__file__) + '...')
    inicio = timeit.default_timer()
    timeexport = time.strftime("%Y%m%d_")
    pathImport = '/import/AsBuilt'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    #csv_path = os.path.join(script_dir, 'export/'+ 'AsBuilt' +'/'+ timeexport +'AsBuilt_ALL'+'.csv')
    csv_path = os.path.join(script_dir, 'export/'+ 'AsBuilt' +'/' +'AsBuilt_ALL'+'.csv')
    
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.xlsx")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    df = pd.DataFrame()
    indexw = 0
    for filename in all_filesSI:
        arquivoName = filename[77:]
        site1 = arquivoName.split('_')
        print(arquivoName)
        indexw +=1
        print(indexw,'/',len(all_filesSI))
        #indexw +=1
        try:
          data = pd.read_excel(filename,skiprows=26,sheet_name = 'INFORMAÇÕES GERAIS', nrows=100,usecols = 'A:J',engine='openpyxl')
          data.columns = ['Setor','RNC/BSC/MME','"Altura(M)"','Azimute (NV)','Logical Azimute (NV)','Downtilt (E+M)','Modelo de Antena','Dell0','STATUS','Dell1']
          #data = data.dropna(subset=['Site'])
          data.insert(0,'Arquivo',arquivoName)
          siteName = site1[3].split('.')
          data.insert(1,'Site',siteName[0])
          indexValue = 0
          firstOne = True
          for i in data['Setor']:
              if pd.isnull(i):
                  firstOne = False
                  #print(i,indexValue,arquivoName)
              if firstOne:
                  indexValue +=1
          data = data.drop(data.index[np.where(data.index > (indexValue-1))[0]])
          data = data.reset_index(drop=True)
          df = df.append(data,ignore_index=True)    
        except:
          #print('ERROR IMPORT, Verificar: ',arquivoName)
          data = pd.read_excel(filename,skiprows=26,sheet_name = 'INFORMAÇÕES GERAIS', nrows=100,usecols = 'A:J',engine='openpyxl')
          data.columns = ['Setor','RNC/BSC/MME','"Altura(M)"','Azimute (NV)','Logical Azimute (NV)','Downtilt (E+M)','Modelo de Antena','Dell0','STATUS']
          #data = data.dropna(subset=['Site'])
          data.insert(0,'Arquivo',arquivoName)
          siteName = site1[3].split('.')
          data.insert(1,'Site',siteName[0])
          indexValue = 0
          firstOne = True
          for i in data['Setor']:
              if pd.isnull(i):
                  firstOne = False
                  #print(i,indexValue,arquivoName)
              if firstOne:
                  indexValue +=1
          data = data.drop(data.index[np.where(data.index > (indexValue-1))[0]])
          data = data.reset_index(drop=True)
          df = df.append(data,ignore_index=True)
          pass
    print('Import de arquivos OK!\n\n')
    #df = NormalizeAntenaName.name(df,''):

    for index, row in df.iterrows():
        if str(row['Logical Azimute (NV)'])[:1] == '(':
            temp = row['Logical Azimute (NV)']
            df.at[index,'Modelo de Antena'] = row['Downtilt (E+M)']
            df.at[index,'Downtilt (E+M)'] = temp
            df.at[index,'Logical Azimute (NV)'] = float(row['Azimute (NV)'])

    df['Downtilt (E+M)2'] = df['Downtilt (E+M)'].map(lambda x: str(x).lstrip('( ').rstrip(' )'))

    df.insert(len(df.columns),'Tilt Elt',0.0)
    df.insert(len(df.columns),'Tilt Mec',0.0)
    for index, row in df.iterrows():
        splitdata = row['Downtilt (E+M)2'].split('+')
        try:
            df.at[index,'Tilt Elt'] = float(splitdata[0])
            df.at[index,'Tilt Mec'] = float(splitdata[1])
        except:
            print('Verificar Downtilt (E+M): ',row['Arquivo'],row['Tilt Elt'],row['Tilt Mec'])


    df = df.drop(['STATUS'],1)
    df.insert(len(df.columns),'STATUS','')

    #Verificar os casos IoT
    df.insert(3,'Setor2',df['Setor'])
    modelosAntenaAir = ['Radio AIR 1641','Radio AIR 3246']
    for index, row in df.iterrows():
        if row['Setor'][-2:-1] == 'I' and len(row['Setor']) > 14:
            df.at[index,'Setor2'] = row['Setor'][:-2] + row['Setor'][-1:]
        if row['Setor'][-3:-2] == 'I' and len(row['Setor']) > 14:
            df.at[index,'Setor2'] = row['Setor'][:-3] + row['Setor'][-2:]
            
        if len(row['Setor2']) == 7 and row['Setor2'][:2] == 'SP' and row['Setor2'][-1:] in ['I','J','K','1','2','3']:
            df.at[index,'STATUS'] = 'VERIFICAR'

        '''
        #Tratar casos 5G
        if row['Setor2'][:2] == '5G':
            df.at[index,'Setor2'] = '4G' + row['Setor2'][2:]
            df.at[index,'Site'] = '4G' + row['Site'][2:]
        '''



        if row['Modelo de Antena'] in modelosAntenaAir:
            df.at[index,'STATUS'] = 'VERIFICAR'
     
    df = df.drop_duplicates()
    #df = df.drop(['Downtilt (E+M)2','Dell0','Dell1'],1)
    df = df.reset_index(drop=True)
    df.to_csv(csv_path,index=True,header=True,sep=';')

    fim = timeit.default_timer()
    print ('duracao: %f' % ((fim - inicio)/60) + ' min') 


