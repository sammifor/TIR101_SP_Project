# Spotify Data Engineering
For spotify data engineering to analysis 2017 - 2024 music data.

## Notice:
For security reasons, the `cloud` folder has been hidden. After cloning this project, please create a folder named `cloud` and ask the project owner to provide you with the project JSON token key. You can then place the key inside the `cloud` folder.


The overall structure should look like this:

- **sp_project**
  - *config*
  - *dags*
    - *gx*
  - *cloud*
  - *logs*
  - *plugins*

## Usage:
Pre: After you cloned and finished above steps
1. Into project folder, open terminal and type
```bash
docker compose up airflow-init
```
Make sure the redis and postgres has been started.
2. Start the rest of services
```bash
docker compose up -d
```
Expected all services will up and run healthy.
If not please restart the service.
