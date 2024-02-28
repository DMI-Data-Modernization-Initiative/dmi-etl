# DMI dbt project
Contains dbt models for transformations for the SHIELD data warehouse

## Setting up locally
### Requirements 
1. Make sure you have python installed: Python 3.8 and above 
2. For Windows preferably use Git Bash as your terminal. Download the git package here https://gitforwindows.org/ (It will include git bash)
3. Make sure you have Microsoft Visual C++ 14.0 or greater installed. Get it with "Microsoft C++ Build Tools": https://visualstudio.microsoft.com/visual-cpp-build-tools/
4. Make sure you have **Microsoft ODBC Driver 17 for SQL Server (x64)** installed in your machine. Download from here: https://go.microsoft.com/fwlink/?linkid=2187214

### Steps
- Clone the repo from GitHub and cd to the root folder
- Create a python virtual environment by running: `python3.8 -m venv <name_of_environemt>` (e.g. `python3.8 -m venv venv`)
- Activate virtual environment by running: `source venv/Scripts/activate`
- Once virtual environment is activated install dbt adapter for sqlserver by running
     `pip install dbt-sqlserver`
- After installing run version check to confirm dbt is installed in your virtual environment
    `dbt --version`
- Create a `.env` file on the root folder and paste the following environment variables (make sure there is no space between):

    ```
        export DBT_USER=<sql server user>
        export DBT_PASSWORD=<sql server password>
        export DBT_DATABASE=<database to build models on>
        export DBT_SERVER=<server ip address>
        export DBT_PROFILES_DIR=./profiles/
    ```

    for `DBT_DATABASE` have a database in the Test SQL Server that you will use to build your models & datasets. Ideally call it
    *dbt_<name_of_dev>*
- Run `source .env` to load your environment variables.
- Make sure you have the config file `profiles.yml` inside the profiles folder with the following configarations:
    
```
dmi_dbt:
  target: dev
  outputs:
    dev:
      type: sqlserver
      driver: 'ODBC Driver 17 for SQL Server'
      server: "{{env_var('DBT_SERVER')}}"
      port: 1433
      database: "{{env_var('DBT_DATABASE')}}"
      schema: dbo
      user:  "{{env_var('DBT_USER')}}"
      password: "{{env_var('DBT_PASSWORD')}}"
      trust_cert: true
 ```

    ## Commands to interact with dbt
    
- `dbt compile` - generates executable SQL from source
- `dbt run` - runs all models in the models folder
- `dbt seed` - loads csv files (typically not for large files)
- `dbt test` - runs tests against your models and seeds
- `dbt docs generate` - generates your project's documentation
- `dbt docs serve` - starts a webserver on port 8000 to serve your documentation locally
- `dbt deps` -  pulls the most recent version of the dependencies listed in your packages.yml from git
- `dbt debug` - tests the database connection and displays information for debugging purposes
    ### For more info on commands see here: https://docs.getdbt.com/reference/dbt-commands



