import pandas as pd
import time, csv
from neo4j import GraphDatabase


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))


path_to_neo4j_import_directory = '/home/milad/.config/Neo4j Desktop/Application/relate-data/dbmss/dbms-15f4fdfe-2fa3-4b99-9733-bd675e7adcd3/import/'






fileName = 'MIMICfull.csv'
perfFileName = 'MIMICfullPerformance.csv'
dataSet = 'MIMIC'

include_entities =  ['Logistic', 'Laboratory_Measurement', 'Prescriptions', 'Diagnosis',
                     'Case_LM', 'Case_LP', 'Case_LD', 'Case_MP', 'Case_MD', 'Case_PD', 'Adm']


case_entity =  ['Case_All']


model_entities =  [['Logistic', 'case', 'WHERE e.EventOrigin ="Logistic"'],
                   ['Laboratory_Measurement', 'case', 'WHERE e.EventOrigin ="Laboratory_Measurement"'],
                   ['Prescriptions', 'case', 'WHERE e.EventOrigin ="Prescriptions"'],
                   ['Diagnosis', 'case', 'WHERE e.EventOrigin ="Diagnosis"'],
                   ['Case_All', 'case', 'WHERE e.case IS NOT NULL'],
                   ['Adm', 'HADM_ID', 'WHERE e.HADM_ID IS NOT NULL']]


model_relations =  [['Case_LM', 'Logistic', 'Laboratory_Measurement', 'case'],
                    ['Case_LP', 'Logistic', 'Prescriptions', 'case'],
                    ['Case_LD', 'Logistic', 'Diagnosis', 'case'],
                    ['Case_MP', 'Laboratory_Measurement', 'Prescriptions', 'case'],
                    ['Case_MD', 'Laboratory_Measurement', 'Diagnosis', 'case'],
                    ['Case_PD', 'Prescriptions', 'Diagnosis', 'case']]


model_entities_derived =  ['Case_LM', 'Case_LP', 'Case_LD', 'Case_MP', 'Case_MD', 'Case_PD']


attr_relations =  [['Adm', 'HADM_ID', 'Admission']]



# several steps of import, each can be switch on/off
step_ClearDB = True           # entire graph shall be cleared before starting a new import
step_ClearConstraints = True
step_LoadEventsFromCSV = True # import all (new) events from CSV file
step_FilterEvents = False       # filter events prior to graph construction
step_createLog = True         # create log nodes and relate events to log node
step_createEntities = True          # create entities from identifiers in the data as specified in this script
step_createEntityRelations = True   # create foreign-key relations between entities
step_reifyRelations = True      # reify relations into derived entities 
step_createDF = True            # compute directly-follows relation for all entities in the data
step_deleteParallelDF = True    # remove directly-follows relations for derived entities that run in parallel with DF-relations for base entities

step_createEventClasses = True  # aggregate events to event classes from data
step_createEventAttrClasses = True # aggregate eventsAttr (Admission, Resource, ...) to event classes from data

step_createDFCAll = False       # aggregate ALL directly-follows relation to event classes

step_createDFCWithoutFiltering = True           # aggregate ONLY a specific directly-follows relation to event classes
step_createDFCFWithFiltering=False                 # aggregate ONLY a specific with a minimum threshold directly-follows relation to event classes

step_createHOWnetwork = True   # create HOW network (aggregate ONLY a specific directly-follows relation to event classes)

#option_filter_removeEventsWhere = 'WHERE e.lifecycle in ["SUSPEND","RESUME"]'

option_DF_entity_type_in_label = False # set to False when step_createDFC is enabled

option_Contains_Lifecycle_Information = False # whether events hold attribute "Lifecycle" to be used in event classifiers

### end config

######################################################
############# DEFAULT METHODS AND QUERIES ############
######################################################

# load data from CSV and import into graph
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



def CreateEventQuery(logHeader, fileName, LogID = ""):
    query = f' LOAD CSV WITH HEADERS FROM \"file:///{fileName}\" as line'
    for col in logHeader:
        if col == 'idx':
            column = f'toInteger(line.{col})'
        elif col in ['timestamp','start','end']:
            column = f'datetime(line.{col})'
        else:
            column = 'line.'+col
        newLine = ''
        if (logHeader.index(col) == 0 and LogID != ""):
            newLine = f' CREATE (e:Event {{Log: "{LogID}",{col}: {column},'
        elif (logHeader.index(col) == 0):
            newLine = f' CREATE (e:Event {{ {col}: {column},'
        else:
            newLine = f' {col}: {column},'
        if (logHeader.index(col) == len(logHeader)-1):
            newLine = f' {col}: {column} }})'

        query = query + newLine
    return query



# run query for Neo4J database
def runQuery(driver, query):
    with driver.session() as session:
        result = session.run(query).single()
        if result != None:
            return result.value()
        else:
            return None

def filterEvents(tx, condition):
    qFilterEvents = f'MATCH (e:Event) {condition} DELETE e'
    print(qFilterEvents)
    tx.run(qFilterEvents)

def add_log(tx, log_id):
    qCreateLog = f'CREATE (:Log {{ID: "{log_id}" }})'
    print(qCreateLog)
    tx.run(qCreateLog)

    qLinkEventsToLog = f'''
            MATCH (e:Event {{Log: "{log_id}" }}) 
            MATCH (l:Log {{ID: "{log_id}" }}) 
            CREATE (l)-[:HAS]->(e)'''
    print(qLinkEventsToLog)
    tx.run(qLinkEventsToLog)

def create_entity(tx, entity_type, entity_id, WHERE_event_property):
    qCreateEntity = f'''
            MATCH (e:Event) {WHERE_event_property}
            WITH e.{entity_id} AS id
            MERGE (en:Entity {{ID:id, uID:("{entity_type}"+toString(id)), EntityType:"{entity_type}" }})'''
    print(qCreateEntity)
    tx.run(qCreateEntity)

def correlate_events_to_entity(tx, entity_type, entity_id, WHERE_event_property):
    qCorrelate = f'''
            MATCH (e:Event) {WHERE_event_property}
            MATCH (n:Entity {{EntityType: "{entity_type}" }}) WHERE e.{entity_id} = n.ID
            CREATE (e)-[:CORR]->(n)'''
    print(qCorrelate)
    tx.run(qCorrelate)

def create_entity_relationships(tx, relation_type, entity_type1, entity_type2, reference_from1to2):
    qCreateRelation = f'''
            MATCH ( e1 : Event ) -[:CORR]-> ( n1:Entity ) WHERE n1.EntityType="{entity_type1}"
            MATCH ( e2 : Event ) -[:CORR]-> ( n2:Entity ) WHERE n2.EntityType="{entity_type2}"
                AND n1 <> n2 AND e2.{reference_from1to2} = n1.ID
            WITH DISTINCT n1,n2
            CREATE ( n1 ) <-[:REL {{Type:"{relation_type}"}} ]- ( n2 )'''
    print(qCreateRelation)
    tx.run(qCreateRelation)

def reify_entity_relations(tx, relation_type):
    qReifyRelation = f'''
            MATCH ( n1 : Entity ) -[rel:REL {{Type:"{relation_type}"}}]-> ( n2:Entity )
            CREATE (n1) <-[:REL {{Type:"Reified"}}]- (new : Entity {{ 
                ID:toString(n1.ID)+"_"+toString(n2.ID),
                EntityType: "{relation_type}",
                uID:"{relation_type}"+toString(n1.ID)+"_"+toString(n2.ID) }} )
                -[:REL {{Type:"Reified"}}]-> (n2)'''
    print(qReifyRelation)
    tx.run(qReifyRelation)



def correlate_events_to_derived_entity(tx, derived_entity_type):
    qCorrelate = f'''
            MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REL {{Type:"Reified"}}]- (r:Entity {{EntityType:"{derived_entity_type}"}} )
            CREATE (e)-[:CORR]->(r)'''
    print(qCorrelate)
    tx.run(qCorrelate)

def createDirectlyFollows(tx, entity_type, option_DF_entity_type_in_label):
    qCreateDF = f'''
        MATCH ( n : Entity ) WHERE n.EntityType="{entity_type}"
        MATCH ( n ) <-[:CORR]- ( e )
        
        WITH n , e as nodes ORDER BY e.timestamp,ID(e)
        WITH n , collect ( nodes ) as nodeList
        UNWIND range(0,size(nodeList)-2) AS i
        WITH n , nodeList[i] as first, nodeList[i+1] as second'''
    qCreateDF = qCreateDF  + '\n'

    if option_DF_entity_type_in_label == True:
        qCreateDF = qCreateDF  + f'MERGE ( first ) -[df:DF_{entity_type}]->( second )'
    else:
        qCreateDF = qCreateDF  + f'MERGE ( first ) -[df:DF {{EntityType:"{entity_type}"}} ]->( second )'

    print(qCreateDF)
    tx.run(qCreateDF)

def deleteParallelDirectlyFollows_Derived(tx, derived_entity_type, original_entity_type):
    if option_DF_entity_type_in_label == True:
        qDeleteDF = f'''
            MATCH (e1:Event) -[df:DF_{derived_entity_type}]-> (e2:Event)
            WHERE (e1:Event) -[:DF_{original_entity_type}]-> (e2:Event)
            DELETE df'''
    else:
        qDeleteDF = f'''
            MATCH (e1:Event) -[df:DF {{EntityType: "{derived_entity_type}" }}]-> (e2:Event)
            WHERE (e1:Event) -[:DF {{EntityType: "{original_entity_type}" }}]-> (e2:Event)
            DELETE df'''

    print(qDeleteDF)
    tx.run(qDeleteDF)


def createEventClass_Activity(tx):
    qCreateEC = f'''
        MATCH ( e : Event ) WITH distinct e.Activity AS actName
        MERGE ( c : Class {{ Name:actName, Type:"Activity", ID: actName}})'''
    print(qCreateEC)
    tx.run(qCreateEC)

    qLinkEventToClass = f'''
        MATCH ( c : Class ) WHERE c.Type = "Activity"
        MATCH ( e : Event ) WHERE c.Name = e.Activity
        CREATE ( e ) -[:OBSERVED]-> ( c )'''
    print(qLinkEventToClass)
    tx.run(qLinkEventToClass)


def createEventClass_ActivityANDLifeCycle(tx):
    qCreateEC = f'''
        MATCH ( e : Event ) WITH distinct e.Activity AS actName,e.lifecycle AS lifecycle
        MERGE ( c : Class {{ Name:actName, Lifecycle:lifecycle, Type:"Activity+Lifecycle", ID: actName+"+"+lifecycle}})'''
    print(qCreateEC)
    tx.run(qCreateEC)

    qLinkEventToClass = f'''
        MATCH ( c : Class ) WHERE c.Type = "Activity+Lifecycle"    
        MATCH ( e : Event ) where e.Activity = c.Name AND e.lifecycle = c.Lifecycle
        CREATE ( e ) -[:OBSERVED]-> ( c )'''
    print(qLinkEventToClass)
    tx.run(qLinkEventToClass)

def createEventClass_Attribute(tx, entity_cl, event_cl):
    qCreateEC = f'''
        MATCH ( e : Event ) WITH distinct e.{entity_cl} AS name
        MERGE ( c : Class {{ Name:name, Type:"{event_cl}", ID: name}})'''
    print(qCreateEC)
    tx.run(qCreateEC)

    qLinkEventToClass = f'''
        MATCH ( e : Event )
        MATCH ( c : Class ) WHERE c.Type = "{event_cl}" AND c.ID = e.{entity_cl}
        CREATE ( e ) -[:OBSERVED]-> ( c )'''
    print(qLinkEventToClass)
    tx.run(qLinkEventToClass)

def aggregateAllDFrelations(tx):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.EntityType = df.EntityType
        WITH n.EntityType as EType,c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{EntityType:EType}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq'''
    print(qCreateDFC)
    tx.run(qCreateDFC)


def aggregateDFrelationsFiltering(tx, entity_type, event_cl, df_threshold, relative_df_threshold):
    # aggregate only for a specific entity type and event classifier
    # include only edges with a minimum threshold, drop weak edges (similar to heuristics miner)
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE n.EntityType = "{entity_type}" AND df.EntityType = "{entity_type}" AND c1.Type = "{event_cl}" AND c2.Type="{event_cl}"
        WITH n.EntityType as EntityType,c1,count(df) AS df_freq,c2
        WHERE df_freq > {df_threshold}
        OPTIONAL MATCH ( c2 : Class ) <-[:OBSERVED]- ( e2b : Event ) -[df2:DF]-> ( e1b : Event ) -[:OBSERVED]-> ( c1 : Class )
        WITH EntityType as EType,c1,df_freq,count(df2) AS df_freq2,c2
        WHERE (df_freq*{relative_df_threshold} > df_freq2)
        MERGE ( c1 ) -[rel2:DF_C  {{EntityType:EType}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq'''
    print(qCreateDFC)
    tx.run(qCreateDFC)

def aggregateDFrelations(tx, entity_type, event_cl):
    # aggregate only for a specific entity type and event classifier
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE n.EntityType = "{entity_type}" AND df.EntityType = "{entity_type}" AND c1.Type = "{event_cl}" AND c2.Type="{event_cl}"
        WITH n.EntityType as EType,c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{EntityType:EType}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq'''
    print(qCreateDFC)
    tx.run(qCreateDFC)

def aggregateDFrelationsForEntities(tx, entity_types, event_cl):
    # aggregate only for a specific entity type and event classifier
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE n.EntityType = df.EntityType AND df.EntityType IN {entity_types} AND c1.Type = "{event_cl}" AND c2.Type="{event_cl}"
        WITH n.EntityType as EType,c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{EntityType:EType}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq'''
    print(qCreateDFC)
    tx.run(qCreateDFC)


    
    
###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################

if step_ClearDB: ### delete all nodes and relations in the graph to start fresh
    print('Clearing DB...')

    # run one delete transaction per relationship type: smaller transactions require less memory and execute faster
    relationTypes = [":DF",":CORR",":OBSERVED",":HAS",":DF_C",":REL"]
    for relType in relationTypes:
        qDeleteRelation = f'''MATCH () -[r{relType}]- () DELETE r'''
        print(qDeleteRelation)
        runQuery(driver,qDeleteRelation)
    # delete all remaining relationships
    qDeleteAllRelations = "MATCH () -[r]- () DELETE r"
    runQuery(driver,qDeleteAllRelations)

    # run one delete transaction per node type type: smaller transactions require less memory and execute faster
    nodeTypes = [":Event",":Entity",":Log",":Class"]
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n{nodeType}) DELETE n'''
        print(qDeleteNodes)
        runQuery(driver,qDeleteNodes)
    # delete all remaining relationships
    qDeleteAllNodes = "MATCH (n) DELETE n"
    runQuery(driver,qDeleteAllNodes)

    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start




def runQuery2(driver, query):
    with driver.session() as session:
        result = session.run(query)
        # Return a list of all records
        return [record for record in result]

if step_ClearConstraints:
    #delete_constraints
    for x in runQuery2(driver, 'SHOW CONSTRAINTS;'):
        if x is not None:
            y = x["name"]
            z = x["labelsOrTypes"]
            print(y)
            drop_query = f"DROP CONSTRAINT {y} IF EXISTS;"
            runQuery2(driver, drop_query)
    # table to measure performance
    perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
    start = time.time()
    last = start
    
if step_LoadEventsFromCSV:
    print('Import events from CSV')
    # load CSV tables
    header, csvLog = LoadLog(path_to_neo4j_import_directory+fileName)
    # convert each record in the CSV table into an Event node
    qCreateEvents = CreateEventQuery(header, fileName, dataSet) #generate query to create all events with all log columns as properties
    print(qCreateEvents)
    runQuery(driver, qCreateEvents)

    #create unique constraints
    runQuery(driver, 'CREATE CONSTRAINT FOR (e:Event) REQUIRE e.ID IS UNIQUE;') #for implementation only (not required by schema or patterns)
    runQuery(driver, 'CREATE CONSTRAINT FOR (en:Entity) REQUIRE en.uID IS UNIQUE;') #required by core pattern
    runQuery(driver, 'CREATE CONSTRAINT FOR (l:Log) REQUIRE l.ID IS UNIQUE;') #required by core pattern


if step_FilterEvents:
    print('Filtering events')
    with driver.session() as session:
        session.write_transaction(filterEvents, option_filter_removeEventsWhere)
        
    end = time.time()
    perf = perf.append({'name':dataSet+'_filter_events', 'start':last, 'end':end, 'duration':(end - last)},ignore_index=True)
    print('Filter event nodes done: took '+str(end - last)+' seconds')
    last = end

    

##create log node and :HAS relationships
if step_createLog:
    with driver.session() as session:
        session.write_transaction(add_log, dataSet)


    
##create entities
if step_createEntities:
    for entity in model_entities: #per entity
       #if entity[0] in include_entities:
            with driver.session() as session:
                session.write_transaction(create_entity, entity[0], entity[1], entity[2])
                print(f'{entity[0]} entity nodes done')
                session.write_transaction(correlate_events_to_entity, entity[0], entity[1], entity[2])
                print(f'{entity[0]} E_EN relationships done')



## create relationships between entities
if step_createEntityRelations:
    for relation in model_relations: #per relation
        with driver.session() as session:
            session.write_transaction(create_entity_relationships, relation[0], relation[1], relation[2], relation[3])
            print(f'{relation[0]} relationships created')
    


if step_reifyRelations:
    for relation in model_relations: #per relation
        derived_entity = relation[0]
        if derived_entity in model_entities_derived and derived_entity in include_entities:
            with driver.session() as session:
                session.write_transaction(reify_entity_relations, derived_entity)
                print(f'{derived_entity} relationships reified')
                session.write_transaction(correlate_events_to_derived_entity, derived_entity)
                print(f'{derived_entity} E_EN relationships created')
        

        
if step_createDF:
    mylist = include_entities.copy()
    mylist.extend(case_entity)
    for entity in mylist: #per entity
        with driver.session() as session:
            session.write_transaction(createDirectlyFollows,entity,option_DF_entity_type_in_label)
            

        
if step_deleteParallelDF:
    for relation in model_relations: #per relation
        derived_entity = relation[0]
        if derived_entity not in include_entities or derived_entity not in model_entities_derived:
            continue

        parent_entity = relation[1]
        child_entity = relation[2]

        with driver.session() as session:
            # entities are derived from 2 other entities, delete parallel relations wrt. to those
            session.write_transaction(deleteParallelDirectlyFollows_Derived, derived_entity, parent_entity)
            session.write_transaction(deleteParallelDirectlyFollows_Derived, derived_entity, child_entity)



        


if step_createEventClasses:
        with driver.session() as session:
            if option_Contains_Lifecycle_Information == True:
                session.write_transaction(createEventClass_ActivityANDLifeCycle)
            else:
                session.write_transaction(createEventClass_Activity)




if step_createEventAttrClasses:
    for relation in attr_relations:  # per relation
        columnName = relation[1]
        Type = relation[2]
        with driver.session() as session:
            session.write_transaction(createEventClass_Attribute, columnName, Type)






if step_createDFCAll: #Step14
    with driver.session() as session:
        session.write_transaction(aggregateAllDFrelations)


if step_createDFCWithoutFiltering: #Step14
    mylist = include_entities.copy()
    mylist.extend(case_entity)
    for entity in mylist: #per entity
        with driver.session() as session:
            if option_Contains_Lifecycle_Information:
                classifier = "Activity+Lifecycle"
            else:
                classifier = "Activity"

            session.write_transaction(aggregateDFrelations, entity, classifier)



if step_createDFCFWithFiltering: #Step14
    mylist = include_entities.copy()
    mylist.extend(case_entity)
    for entity in mylist: #per entity
        with driver.session() as session:
            if option_Contains_Lifecycle_Information:
                classifier = "Activity+Lifecycle"
            else:
                classifier = "Activity"

            session.write_transaction(aggregateDFrelationsFiltering, entity, classifier, 5000, 3)
            # session.write_transaction(aggregateDFrelationsFiltering,entity,classifier,1,3)






if step_createHOWnetwork:      #Step14
    # create HOW relations along all process entities, except Attr
    how_entities = include_entities.copy()
    for relation in attr_relations:  # per relation
        AtrAS = relation[0]
        how_entities.remove(AtrAS)

    for relation in attr_relations:  # per relation
        Type = relation[2]
        with driver.session() as session:
            session.write_transaction(aggregateDFrelationsForEntities, how_entities, Type)



driver.close()
