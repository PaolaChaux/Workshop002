<h1 align="center"> Workshop 2: ETL process using airflow </h1>
<p align="left">
   <img src="https://img.shields.io/badge/STATUS-FINISHED-green">
   </p>

### Presented by Paola Andrea Chaux Campo, student of Autonoma de Occidente University

### In the Airflow Data Engineer code challenge. I show you my knowledge about data management and visualizations with the final objective of shows all the ETL process using the two different data sources (csv and Database) and chart visualizations in Power BI. In this workshop I used the spotify dataset (csv) to be readed in python and airflow, create some transformation and load into a database, on the other hand, I used the grammys dataset to be loaded into a database, then using Airflow I readed the data from the database , perform transformations, merge with the spotify dataset and load into the database.


## Table of contents
* [Description](#Description)

* [Objective](#Objective)

* [Requeriments](#Requeriments)

* [Features](#Features)

* [Installation](#Installation)

* [Considerations](#Considerations)

* [References](#References)


## Description

#### In this repository i worked an proyect's ETL process using airflow with two datasets (Spotify and Winner Grammys).

#### The EDA process was made by Jupyter Notebook, transfer the data to a relational database use PostgreSQL, the Airflow and also the Dashboard.

## Objective 
#### Demonstrate my knowledge about data management and visualizations with the final objective of shows all the ETL process using the two different data sources. Present in a report with significant conclusions.

## Requeriments
* Jupiter Notebook.
* Pandas.
* Psycopg2.
* Json.
* Datetime.
* Powerbiclient. 
* Numpy.
* Matplotlib.pyplot.
* Python.
* Postgres 
* Apache airflow
* CSV files
* Visualization tool: PowerBI

## Features
#### The most important thing that  I can be highlighted is that there were much several problems when using new tools. We were able to observe that the data was very versatile and they lent themselves to carrying out the analysis depending on what you wanted to achieve due to the number of attributes that both had. I was able to find that in the year 2019 had several awards records and in the others only 1 per year, which had to be taken into account when making precise conclusions.

## Installation Steps
#### 1. Clone the repository.
#### 2. Open the proyect with Visual Studio Code.
#### 3. Create a virtual environment from your terminal: "python -m venv [environment_name]"
#### 4. Activate your virtual environment: "[environment_name]/Scripts/activate"
#### 5. Install the required tools and modules in the environment.
#### 6.Set the created environment as kernel.
#### 7. Run the app and enjoy it.

## Considerations
#### In the visualization part in Power BI Client, what is done is a generalized report that we can edit by having an account in Power BI and logging in, this file is only shown to the user and the inclusion of this notebook is only as a guide so that they can create their own pre-made reports with this tool that facilitates interaction, after saving it we can continue editing from the picnicpal page and download it as a .pbix format or in pdf for greater flexibility to share it, it will be attached in the references of all sites to help you with this tool.

## References
### https://fuchsia-tin-839.notion.site/PyDrive2-442c895690304e75ab3d5c0a31ea55ac
### https://www.youtube.com/watch?v=ZI4XjwbpEwU
### https://console.cloud.google.com/welcome?hl
#### https://powerbi.microsoft.com/es-mx/blog/create-power-bi-reports-in-jupyter-notebooks/
#### https://pypi.org/project/powerbiclient/
#### https://www.neoguias.com/como-conectarse-postgresql-python/#Como_conectarte_a_una_base_de_datos
#### https://www.studocu.com/bo/document/universidad-mayor-de-san-andres/programacion-i/tarea-4python-ejercicios/33129056
#### https://es.stackoverflow.com/questions/185298/importar-una-funci%C3%B3n-de-otro-archivo-ipynb-en-jupyter-notebook
#### https://github.com/dventep/workshop001_etl_education/blob/main/notebooks/eda_report.ipynb
#### https://learn.microsoft.com/es-es/power-bi/consumer/end-user-change-sort
#### https://pypi.org/project/powerbiclient/
#### https://learn.microsoft.com/es-es/power-bi/create-reports/jupyter-quick-report
#### https://learn.microsoft.com/es-es/javascript/api/overview/powerbi/powerbi-jupyter
#### https://learn.microsoft.com/es-es/power-bi/connect-data/service-tutorial-connect-to-github
