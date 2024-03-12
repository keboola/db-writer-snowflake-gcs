_ToDo: Update_

# Snowflake DB Writer for GCP
[![GitHub Actions](https://github.com/keboola/db-writer-snowflake/actions/workflows/push.yml/badge.svg)](https://github.com/keboola/db-writer-snowflake/actions/workflows/push.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/keboola/db-writer-snowflake/blob/master/LICENSE.md)

Writes data to Snowflake Database.

## Example configuration

```json
    {
      "db": {        
        "host": "HOST",
        "port": "PORT",
        "database": "DATABASE",
        "user": "USERNAME",
        "password": "PASSWORD",
        "schema": "SCHEMA",
        "warehouse": "WAREHOUSE",
        "ssh": {
          "enabled": true,
          "keys": {
            "private": "ENCRYPTED_PRIVATE_SSH_KEY",
            "public": "PUBLIC_SSH_KEY"
          },
          "sshHost": "PROXY_HOSTNAME"
        }
      },
      "tables": [
        {
          "tableId": "simple",
          "dbName": "simple",
          "export": true, 
          "incremental": true,
          "primaryKey": ["id"],
          "items": [
            {
              "name": "id",
              "dbName": "id",
              "type": "int",
              "size": null,
              "nullable": null,
              "default": null
            },
            {
              "name": "name",
              "dbName": "name",
              "type": "nvarchar",
              "size": 255,
              "nullable": null,
              "default": null
            },
            {
              "name": "glasses",
              "dbName": "glasses",
              "type": "nvarchar",
              "size": 255,
              "nullable": null,
              "default": null
            }
          ]                                
        }
      ]
    }
```

## Development

Required snowflake resource for writer:
```sql
CREATE DATABASE "snowflake_writer_gcs";
USE DATABASE "snowflake_writer_gcs";
CREATE TRANSIENT SCHEMA "snowflake_writer_gcs";
CREATE WAREHOUSE "snowflake_writer_gcs" WITH 
  WAREHOUSE_SIZE = 'XSMALL' 
  WAREHOUSE_TYPE = 'STANDARD' 
  AUTO_SUSPEND = 900 
  AUTO_RESUME = TRUE;
CREATE ROLE "snowflake_writer_gcs";
GRANT USAGE ON WAREHOUSE "snowflake_writer_gcs" TO ROLE "snowflake_writer_gcs";
GRANT USAGE ON DATABASE "snowflake_writer_gcs" TO ROLE "snowflake_writer_gcs";
GRANT ALL ON SCHEMA "snowflake_writer_gcs" TO ROLE "snowflake_writer_gcs";
GRANT ALL ON FUTURE TABLES IN SCHEMA "snowflake_writer_gcs" TO ROLE "snowflake_writer_gcs";
GRANT ALL ON FUTURE VIEWS IN SCHEMA "snowflake_writer_gcs" TO ROLE "snowflake_writer_gcs";
CREATE USER "snowflake_writer_gcs" 
  PASSWORD = 'password' 
  DEFAULT_ROLE = "snowflake_writer_gcs" 
  DEFAULT_WAREHOUSE = "snowflake_writer_gcs" 
  DEFAULT_NAMESPACE = "snowflake_writer_gcs"."snowflake_writer_gcs" 
  MUST_CHANGE_PASSWORD = FALSE;
GRANT ROLE "snowflake_writer_gcs" TO USER "snowflake_writer_gcs";
```

App is developed on localhost using TDD.

1. Clone from repository: `git clone git@github.com:keboola/db-writer-snowflake.git`
2. Change directory: `cd db-writer-snowflake`
3. Install dependencies: `docker-compose run --rm php composer install -n`
4. Create `.env` file:
```bash
KBC_URL=
KBC_RUNID=
DB_HOST=
DB_PORT=
DB_DATABASE=
DB_USER=
DB_PASSWORD=
DB_SCHEMA=
DB_WAREHOUSE=
```
5. Run docker-compose, which will trigger phpunit: `docker-compose run --rm app`

## License

MIT licensed, see [LICENSE](./LICENSE) file.
