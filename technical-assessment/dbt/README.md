# DBT Analytics Project
This DBT project transforms raw player, affiliate, and transaction data into meaningful, aggregated tables.


### Project Structure
- models/staging: Contains staging models that clean and lightly transform the raw source data. These are typically materialized as views.

- models/marts: Contains the final dimensional and fact models that are presented to end-users for analytics. These are materialized as tables.


### How to Run
1. Configure your profiles.yml to connect to your data warehouse.

2. Load the source CSV data into your raw database/schema.

3. Run the command dbt run to build all the models.