import pandas as pd

def count2(df,ref):
    
    dataframe = df.copy()
    removefromloop = [ref]
    locationBase_top = list(dataframe.columns)
    res = list(set(locationBase_top)^set(removefromloop))
    dataframe = dataframe.drop(res,1) 
    dataframe['count'] = dataframe.groupby(ref)[ref].transform('count')
    return dataframe

def count(df,ref):
    '''
    dataframe = df.copy()
    removefromloop = [ref]
    locationBase_top = list(dataframe.columns)
    res = list(set(locationBase_top)^set(removefromloop))
    dataframe = dataframe.drop(res,1) 
    dataframe['count'] = dataframe.groupby(ref)[ref].transform('count')
    return dataframe
    '''
    #Qtd Eventos por Location
    counts = dict()
    
    for i, row in df.iterrows():
        key = (
                row[ref]
  
            )  
        if key in counts:
            counts[key] = int(counts[key] + 1)
        else:
            counts[key] = 1
    dictt = pd.DataFrame.from_dict(counts, orient='index')
    dictt = dictt.reset_index(drop=False)
    dictt.columns = [ref, 'count']
    dictt['count'] = round(dictt['count'].astype(int),0)
    return dictt
    
    
