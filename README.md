# Project1: Sparklifydb analitical database setup

### 0. Purpose
This code help you to Fill song and user activity data to postgres database in a structured way

### 1. Project structure 
This project consist of four main python entities:
* ```sql_queries.py``` - Configuration file. Consist of sql templates for creating, inserting and deletion of main project tables
* ```create_tables.py``` - Script to create database and configure database schema. Could be used to revert database to initial empty state.
* ```etl.py``` - Script to fill song and user information from ./data folder in project
* ```test.py``` - Run select queries from project tables

### 2. How to use 
From project directory run following steps:

#### 2.0 Copy data folder to project directory
Without this step you cant run further scripts

#### 2.1 Initialize
```bash
python create_tables.py
```
#### 2.2 Fill data
```bash
python etl.py
```
#### 2.3 Check main queries from data
```bash
python test.py
```
#### 2.4 Revert to initial state
Run again:
```bash
python create_tables.py
```

### 3. Notes
In notebooks directory you can find few test notebooks. <br>
Initial data with songs and users have only one intersected record: <br>
You can prove this fact by disovering: ```notebooks/usecase__2__check_data_is_valid.ipynb```
