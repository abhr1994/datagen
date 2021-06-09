# datagen
```markdown

This code helps users to generate TPCDS data in any formats like CSV, Parquet, JSON, Delta Tables and 
also helps to write the TPCDS data to tables in Big Query, SQl Server and any RDBMS sources.

This connects to Snowflake database: SNOWFLAKE_SAMPLE_DATA and extracts the TPCDS data via Spark connector and writes to required target format

https://docs.snowflake.com/en/user-guide/sample-data-tpcds.html

```

# Using secrets in Databricks notebooks

```
Inorder to avoid explicitly providing credentials in the Databricks notebooks we can use the Databricks cli secrets feature to encode the secrets

Refrerence URL: https://docs.databricks.com/dev-tools/cli/secrets-cli.html

Commands:

databricks secrets create-scope --scope datagen-secret --initial-manage-principal users

databricks secrets write --key snowflake_user --string-value <your_user_name> --scope datagen-secret

databricks secrets write --key snowflake_pass --string-value <your_password> --scope datagen-secret

databricks secrets write --key snowflake_url --string-value <your_snowflake_url> --scope datagen-secret

```
