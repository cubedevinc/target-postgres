# target-postgres

This is a [Singer](https://singer.io) target for Postgres
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).
#### Samle config:

    {
      "host": "localhost",
      "user": "test",
      "port": "5432",
      "password": "",
      "dbname": "target_test",
      "schema": "test_schema"
    }

#### Mandatory properties:

`host`: The host name of the PostgreSQL server

`user`: The database user on whose behalf the connection is being made.

`port`: The port number the server is listening on

`password`: The database user's password.

`dbname`: The database name

`schema`: Destination schema name

#### Optional properties:

`grant_select_to`: String or Array. When a new schema or table is created, SELECT privilege will be granted to one or more ROLEs automatically.
