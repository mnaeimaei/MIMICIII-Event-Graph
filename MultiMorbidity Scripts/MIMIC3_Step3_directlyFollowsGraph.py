from neo4j import GraphDatabase
from graphviz import Digraph


import os

### begin config
# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))

option_Contains_Lifecycle_Information = False # whether events hold attribute "Lifecycle" to be used in event classifiers
if option_Contains_Lifecycle_Information:
    classifier = "Activity+Lifecycle"
else:
    classifier = "Activity"

show_lifecycle=False

##### colors

c1_yellow = '#ffffbf'  #lighter #font: c_black
c2_yellow = '#fee090'           #font: c_black
c3_yellow = "#fed47f"           #font: c_black
c4_yellow = "#ffd965"           #font: c_black
c5_yellow = "#feb729"           #font: c_white
c6_yellow = "#ffc000" #darker   #font: c_white


c1_blue = '#e0f3f8'  #lighter   #font: c_black
c2_blue = '#bbd1ff'             #font: c_black
c3_blue = '#abd9e9'             #font: c_black
c4_blue = '#91bfdb'             #font: c_black
c5_blue = "#5b9bd5"             #font: c_black
c6_blue = '#2c7bb6'             #font: c_white
c7_blue = '#4575b4' #darker     #font: c_white



c1_cyan = "#93f0ea"  #lighter   #font: c_black
c2_cyan = "#19b1a7"             #font: c_white
c3_cyan = "#13857d"             #font: c_white
c4_cyan = "#318599"  #darker    #font: c_white


c1_orange = '#fdae61'  #lighter #font: c_black
c2_orange = "#f59d56"           #font: c_white
c3_orange = '#fc8d59'           #font: c_white
c4_orange = "#ea700d"  #darker  #font: c_white


c1_red = '#f9cccc'  #lighter #font: c_black
c2_red = "#ff0000"           #font: c_white
c3_red = '#d73027'           #font: c_white
c4_red = '#d7191c'           #font: c_white
c5_red = '#c81919' #darker   #font: c_white


c1_green = "#4ae087" #lighter   #font: c_black
c2_green = "#70ad47"            #font: c_white
c3_green = "#178544" #darker    #font: c_white


c1_purple = "#e7bdeb" #lighter  #font: c_black
c2_purple= "#a034a8" #darker    #font: c_white


c_white = "#ffffff"
c_black = "#000000"



def getNodeLabel_Event(name):
    return name[2:800]


def getEventsDF(tx, dot, entity_type, color, fontcolor, edge_width, show_lifecycle):
    q = f'''
        MATCH (e1:Event) -[:CORR]-> (n:Entity{{EntityType:"{entity_type}"}}) WHERE {case_selector}
        OPTIONAL MATCH (e1) -[df:DF{{EntityType:"{entity_type}"}}]-> (e2:Event)
        RETURN e1,df,e2
        '''
    # print(q)

    dot.attr("node", shape="circle", fixedsize="True", width="0.4", height="0.4", fontname="Helvetica", fontsize="6",
             margin="0")
    for record in tx.run(q):




        if show_lifecycle:
            e1_name = record["e1"]["Activity"] + '\n' + record["e1"]["lifecycle"][0:5]
        else:
            e1_name = record["e1"]["Activity"]
            e1_name44 = getNodeLabel_Event(e1_name)
            e1_name2 = e1_name44.replace(' ', '\n')

            #z=len(e1_name)
            #if (z % 2) == 0:
            #    j = int(z/2)
            #    e1_name1=(e1_name[:j])
            #    e1_name2=(e1_name[j:])

            #else:
            #    j = int(z/2)
            #    e1_name1=(e1_name[:j])
            #    e1_name2=(e1_name[j:])

            #dot.node(str(record["e1"].id), e1_name1+ "_\n"+ e1_name2,  color=color, style="filled", fillcolor=color, fontcolor=fontcolor)
        dot.node(str(record["e1"].id), e1_name2,  color=color, style="filled", fillcolor=color, fontcolor=fontcolor)


        if record["e2"] != None:
            edge_label = ""
            xlabel = ""
            pen_width = str(edge_width)
            edge_color = color

            dot.edge(str(record["e1"].id), str(record["e2"].id), label=edge_label, color=edge_color, penwidth=pen_width,
                     xlabel=xlabel, fontname="Helvetica", fontsize="3", fontcolor=edge_color)


def getDF(tx, dot, entity_type, color, fontcolor, edge_width):
    q = f'''
        MATCH (e1:Event) -[:CORR]-> (n:Entity{{EntityType:"{entity_type}"}}) WHERE {case_selector}
        MATCH (e1) -[df:DF{{EntityType:"{entity_type}"}}]-> (e2:Event)
        RETURN distinct e1,df,e2
        '''
    # print(q)

    dot.attr("node", shape="circle", fixedsize="true", width="0.4", height="0.4", fontname="Helvetica", fontsize="8",
             margin="0")
    for record in tx.run(q):
        edge_label = ""
        xlabel = record["df"].type[len("DF_Case_"):]
        pen_width = str(edge_width)
        edge_color = color

        dot.edge(str(record["e1"].id), str(record["e2"].id), label=edge_label, color=edge_color, penwidth=pen_width,
                 xlabel=xlabel, fontname="Helvetica", fontsize="8", fontcolor=edge_color)


def getResourcesWithinCaseDF(tx, dot , entity_type, edge_color):
    q = f'''     
        match(e1: Event) -[df: DF{{EntityType: "{entity_type}"}}]-> (e2:Event) -[: CORR]-> (r:Entity {{EntityType:"{entity_type}"}})
        WHERE e1.case = e2.case AND {case_selector}
        return e1, df, e2, r
        '''
    print(q)
    for record in tx.run(q):
        edge_label = ""
        xlabel = record["r"]["ID"]


        pen_width = "1"
        dot.node(str(record["e1"].id), color=c4_red, penwidth="2")
        dot.node(str(record["e2"].id), color=c4_red, penwidth="2")
        dot.edge(str(record["e1"].id), str(record["e2"].id), label=edge_label, color=edge_color, penwidth=pen_width,
                 xlabel=xlabel, fontname="Helvetica", fontsize="8", fontcolor=c4_red)


def getEntityForFirstEvent(tx, dot, entity_type, color, fontcolor):
    q = f'''
        MATCH (e1:Event) -[corr:CORR]-> (n:Entity)
        WHERE n.EntityType = "{entity_type}" AND NOT (:Event)-[:DF{{EntityType:"{entity_type}"}}]->(e1) AND {case_selector}
        return e1,corr,n   
        '''
    print(q)

    dot.attr("node", shape="rectangle", fixedsize="false", width="0.4", height="0.4", fontname="Helvetica",
             fontsize="8", margin="0")
    for record in tx.run(q):
        e_id = str(record["e1"].id)
        # e_name = getNodeLabel_Event(record["e"]["Activity"])
        entity_type = record["n"]["EntityType"]

        entity_id = record["n"]["ID"]
        entity_uid = record["n"]["uID"]
        entity_label = entity_type + '\n' + entity_id

        dot.node(entity_uid, entity_label, color=color, style="filled", fillcolor=color, fontcolor=fontcolor)
        dot.edge(entity_uid, e_id, style="dashed", arrowhead="none", color=color)


def getEntityForFirstEvent2(tx, dot, entity_type, color, fontcolor):
    q = f'''
        MATCH(e1: Event) -[corr: CORR]-> (n:Entity)
        WHERE {case_selector}
        return distinct e1.HADM_ID as HADM
        '''
    print(q)

    dot.attr("node", shape="rectangle", fixedsize="false", width="0.4", height="0.4", fontname="Helvetica",
             fontsize="8", margin="0")

    for record in tx.run(q):
        e3 = str(record["HADM"])
        q2 = f'''
            MATCH(e1: Event) -[corr: CORR]-> (n:Entity)
            WHERE n.EntityType = "{entity_type}" AND {case_selector} and e1.HADM_ID = "{e3}"
            return e1, corr, n
            order by e1.timestamp
            limit 1
            '''

        print(q2)

        for record in tx.run(q2):
            e_id = str(record["e1"].id)
            print(e_id)
            # e_name = getNodeLabel_Event(record["e"]["Activity"])
            entity_type = record["n"]["EntityType"]

            entity_id = str(record["n"]["ID"])
            entity_uid = record["n"]["uID"]
            entity_label = "Admission" + '\n' + str(entity_id)

            dot.node(entity_uid, entity_label, color=color, style="filled", fillcolor=color, fontcolor=fontcolor)
            dot.edge(entity_uid, e_id, style="dashed", arrowhead="none", color=color)




def getDFcNodes(tx, dot, entity_prefix, entity_name, clusternumber, color, fontcolor, min_freq, event_cl):
    q = f'''
        MATCH (c1:Class {{Type:"{event_cl}"}}) -[df:DF_C]- ()
        WHERE df.count > {min_freq}
        return distinct c1
        '''
    print(q)



    dot.attr("node", shape="circle", fixedsize="true", width="0.4", height="0.4", fontname="Helvetica", fontsize="8",
             margin="0")
    c_entity = Digraph(name="cluster" + str(clusternumber))
    c_entity.attr(rankdir="LR", style="invis")

    for record in tx.run(q):
        c1_id = str(record["c1"].id)  #id=identitiy
        if record["c1"]["Name"][0:2] == entity_prefix:
            c1_name = record["c1"]["Name"]
            #print(c1_name)
            c_entity.node(c1_id, c1_name, color=color, style="filled", fillcolor=color, fontcolor=fontcolor)

    q = f'''
        MATCH (c1:Class {{Type:"{event_cl}"}})
        WHERE NOT (:Class)-[:DF_C {{ EntityType: "{entity_name}"}}]->(c1)
        return distinct c1
        '''
    print(q)
    for record in tx.run(q):
        c1_id = str(record["c1"].id)
        if record["c1"]["Name"][0:2] == entity_prefix:
            c1_id = str(record["c1"].id)
            dot.node(entity_name, entity_name, shape="rectangle", fixedsize="false", color=color, style="filled",
                     fillcolor=color, fontcolor=fontcolor)
            dot.edge(entity_name, c1_id, style="dashed", arrowhead="none", color=color)

    dot.subgraph(c_entity)


def getDFcEdges(tx, dot, entity, edge_color, minlen, edge_label, show_count, min_freq, event_cl):
    q = f'''
        MATCH (c1:Class {{Type:"{event_cl}"}}) -[df:DF_C ]-> (c2:Class {{Type:"{event_cl}"}})
        WHERE df.count > {min_freq} and df.EntityType = "{entity}"
        return distinct c1,df,c2
        '''
    print(q)

    dot.attr("node", shape="circle", fixedsize="true", width="0.4", height="0.4", fontname="Helvetica", fontsize="8",
             margin="0")
    dot.attr("edge", fontname="Helvetica", fontsize="8")
    for record in tx.run(q):
        c1_id = str(record["c1"].id)
        c2_id = str(record["c2"].id)

        # c1_name = getNodeLabel_Event(record["c1"]["Name"])+'\n'+record["c1"]["Lifecycle"][0:5]
        # c2_name = getNodeLabel_Event(record["c2"]["Name"])+'\n'+record["c2"]["Lifecycle"][0:5]

        xlabel = str(record["df"]["count"])  # edge_label+" ("+str(record["df"]["count"])+")"
        penwidth = 1 + record["df"]["count"] / 50000

        # dot.node(c1_id,c1_name)
        # dot.node(c2_id,c2_name)
        if record["df"]["count"] < 1000:
            constraint = "false"
        else:
            constraint = "true"
        dot.edge(c1_id, c2_id, xlabel=xlabel, fontcolor=edge_color, color=edge_color, penwidth=str(penwidth),
                 constraint=constraint)


def getResourcesDF(tx, dot, entity_name, edge_width):
    q = f'''
        match (r:Entity {{EntityType:"{entity_name}"}}) <-[:CORR]- (e1:Event) -[df:DF{{EntityType:"{entity_name}"}}]-> (e2:Event)
        WHERE {case_selector} AND {case_selector} 
        return e1,df,e2,r
        '''
    print(q)
    for record in tx.run(q):
        edge_label = ""
        xlabel = record["r"]["ID"]

        pen_width = str(edge_width)
        edge_color = c5_red
        dot.node(str(record["e1"].id), color=c5_red, penwidth="2")
        dot.node(str(record["e2"].id), color=c5_red, penwidth="2")
        dot.edge(str(record["e1"].id), str(record["e2"].id), label=edge_label, color=edge_color, penwidth=pen_width,
                 xlabel=xlabel, fontname="Helvetica", fontsize="8", fontcolor=edge_color, style='invis')


dot = Digraph(comment='Query Result')
dot.attr("graph", rankdir="LR", margin="0")


A4=True



if A4 == True:
    #cases = ['Patient_14606','Patient_4900']
    cases = ['Patient_4900']
    case_selector = "e1.case IN " + str(cases)
    print(case_selector)
    with driver.session() as session:




        session.read_transaction(getEventsDF, dot, "Logistic", c6_blue, c_white, 3, False)
        session.read_transaction(getEventsDF, dot, "Laboratory_Measurement", c3_cyan, c_white, 3, False)
        session.read_transaction(getEventsDF, dot, "Prescriptions", c3_orange, c_white, 3, False)
        session.read_transaction(getEventsDF, dot, "Diagnosis", c5_yellow, c_white, 3, False)


        session.read_transaction(getDF, dot, "Case_LM", "#999999", c_black, "1")
        session.read_transaction(getDF, dot, "Case_LP", "#999999", c_black, "1")
        session.read_transaction(getDF, dot, "Case_LD", "#999999", c_black, "1")
        session.read_transaction(getDF, dot, "Case_MP", "#999999", c_black, "1")
        session.read_transaction(getDF, dot, "Case_MD", "#999999", c_black, "1")
        session.read_transaction(getDF, dot, "Case_PD", "#999999", c_black, "1")

        session.read_transaction(getResourcesWithinCaseDF, dot, "Adm", c4_red)



        session.read_transaction(getEntityForFirstEvent, dot, "Logistic", c6_blue, c_white)
        session.read_transaction(getEntityForFirstEvent, dot, "Laboratory_Measurement", c3_cyan, c_white)
        session.read_transaction(getEntityForFirstEvent, dot, "Prescriptions", c3_orange, c_white)
        session.read_transaction(getEntityForFirstEvent, dot, "Diagnosis", c5_yellow, c_white)


        session.read_transaction(getEntityForFirstEvent2, dot, "Adm", c4_red, c_white)


        # print(dot.source)
    filename = "14606"
    DOTOutput = f'{filename}.dot'
    PDFOutput = f'dot -Tpdf {filename}.dot -o {filename}.pdf'
    TOutput = f'xdot {filename}.dot'

    file = open(DOTOutput, "w")
    file.write(dot.source)
    file.close()
    os.system(PDFOutput)
    os.system(TOutput)
    os.remove(DOTOutput)
        # dot.render('test-output/round-table.gv', view=True)





