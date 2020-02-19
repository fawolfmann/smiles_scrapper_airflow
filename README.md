# Smiles scrapper with airflow

Get data from smiles.com.ar and save it to future analyze.

- Pipeline -> Airflow
- Database -> PostgreSQL
- Web crawler -> Pyppeteer

## Instalation
- Clone the repo
- Install docker and docker-compose

	`docker-compose up -d`
 
- open web browser on localhost:8080
  
## Docker image
Docker image and docker-compose from [puckel](https://github.com/puckel/docker-airflow.git)
Modify to install Pyppeteer package
