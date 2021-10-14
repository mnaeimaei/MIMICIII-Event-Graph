#loan application

import pandas as pd
import time, os, csv

#config


inputpath = './'
path_to_neo4j_import_directory = '/home/Final/'




def LoadLog(localFile):
    datasetList = []
    headerCSV = []
    i = 0
    with open(localFile) as f:
        reader = csv.reader(f)
        for row in reader:
            if (i==0):
                headerCSV = list(row)
                i +=1
            else:
               datasetList.append(row)
        
    log = pd.DataFrame(datasetList,columns=headerCSV)
    
    return headerCSV, log

def MIMIC3(inputpath, path_to_neo4j_import_directory, fileName):
    csvLog = pd.read_csv(os.path.realpath(inputpath+'EventLog.csv'), keep_default_na=True) #load full log from csv
    csvLog.drop_duplicates(keep='first', inplace=True) #remove duplicates from the dataset
    csvLog = csvLog.reset_index(drop=True) #renew the index to close gaps of removed duplicates 
    

    sampleIds = []


    # rename CSV columns to standard value
    # Activity
    # timestamp
    # resource
    # lifecycle for life-cycle transtiion

    csvLog = csvLog.rename(columns={'SUBJECT_ID': 'case','Event': 'Activity','Timestamp':'timestamp'})

    csvLog['EventIDraw'] = csvLog['EventID']


    sampleList = [] #create a list (of lists) for the sample data containing a list of events for each of the selected cases
    # fix missing entity identifier for one record: check all records in the list of sample cases (or the entire dataset)
    for index, row in csvLog.iterrows():
        if sampleIds == [] or row['case'] in sampleIds:

            rowList = list(row) #add the event data to rowList
            sampleList.append(rowList) #add the extended, single row to the sample dataset
    
    header =  list(csvLog) #save the updated header data
    logSamples = pd.DataFrame(sampleList,columns=header) #create pandas dataframe and add the samples  

    logSamples['timestamp'] = pd.to_datetime(logSamples['timestamp'], format='%Y-%m-%d %H:%M:%S')
    
    logSamples.fillna(0)
    logSamples.sort_values(['case','timestamp'], inplace=True)
    logSamples['timestamp'] = logSamples['timestamp'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3]+'+0100')
    
    logSamples.to_csv(path_to_neo4j_import_directory+fileName, index=True, index_label="idx",na_rep="Unknown")



fileName = 'MIMICfull.csv'
perfFileName = 'MIMICfullPerformance.csv'


start = time.time()
MIMIC3(inputpath, path_to_neo4j_import_directory, fileName)
end = time.time()
print("Prepared data for import in: "+str((end - start))+" seconds.") 
