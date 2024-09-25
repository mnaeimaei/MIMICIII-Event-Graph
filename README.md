# Using Graph Databases for MIMIC-III Event Data

**Description**

This repository consists of adopted code for discovering care pathways (using event graph representation) for patients with multi-morbidity and involves python script and Neo4j library.

Publication

- [Discovering care pathways for multi-morbid patients using event graphs](https://arxiv.org/abs/2110.00291) 

Copyright (C) 2020-2021

Milad Naeimaei Aali, Norwegian University of Science and Technology, Trondheim, Norway

Felix Mannhardt, Eindhoven University of Technology, Eindhoven, the Netherlands

Pieter Jelle Toussaint , Norwegian University of Science and Technology, Trondheim, Norway

**Installation (Python)**

Install python

Install neo4j package in python by python package manager (Pip or Conda)

Install other libraries pandas, time, os, CSV, graphviz

**Installation (Neo4j)**

Install Neo4j Desktop 1.4.4 [Neo4J Download Center](https://neo4j.com/download-center/#desktop)

**Installation (Graphviz)**

For ubuntu:

sudo apt-get update -y

sudo apt-get install -y graphviz

**Installation (xdot)**

For ubuntu:

sudo apt-get update -y

sudo apt-get install -y dot

**How to use**

First  Step1.py

Run Step1.py

Create a new project in Neo4J, Addi a Local DBMS to that project, and select a password (1234 recommended).
Then in the project setting, allocate enough memory to the database, such as DBMS.memory.heap.max_size=20G
import the output of the step1 in that Neo4j project (open folder/import)
start Neo4J Project

In Step2.py, adopt path_to_neo4j_import_directory as import location of Neo4J 
Rn Step2.py

Run Step3.py for creating an event graph



