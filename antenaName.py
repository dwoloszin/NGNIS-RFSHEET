
import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import timeit




def normalizedAntenaName(df,columnameA,columnameB):
    print("\nProcessing " + os.path.basename(__file__) + '...')
    inicio = timeit.default_timer()
    dfCopy = df.copy()
    #AIR1641
    for index, row in dfCopy.iterrows():
        if str(row[columnameB]) == 'AIR1641':
            dfCopy.at[index,columnameB] = 'AIR 1641'
        #AIR3246B3
        if str(row[columnameB]) == 'AIR3246B3':
            dfCopy.at[index,columnameB] = 'AIR 3246'

        if str(row[columnameB]) == 'AIR6449':
            dfCopy.at[index,columnameB] = 'AIR 6449'
        
        if str(row[columnameB]) == 'AIR6419':
            dfCopy.at[index,columnameB] = 'AIR 6419'

        if str(row[columnameB]) == 'AIR1641_3DS':
            dfCopy.at[index,columnameB] = 'AIR 1641'

        if str(row[columnameB]) == 'AIR1641_2DS':
            dfCopy.at[index,columnameB] = 'AIR 1641'

        if str(row[columnameB]) == 'AIR3219':
            dfCopy.at[index,columnameB] = 'AIR 3219'

        #AIR6449
        #AIR6419


    for index, row in dfCopy.iterrows():
        if str(row[columnameB]) in str(row[columnameA]):
            dfCopy.at[index,columnameA] = row[columnameB]


    fim = timeit.default_timer()
    print ('duracao: %f' % ((fim - inicio)/60) + ' min')
    return dfCopy
     
