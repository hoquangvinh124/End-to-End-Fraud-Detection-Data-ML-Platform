# dbt Example Style Guide

## SQL Style

- Use lowercase keywords.
- Use trailing commas.

## Model Organization

Our models (typically) fit into two main categories:\

- Staging &mdash; Contains models that clean and standardize data.        
- Marts &mdash; Contains models which combine or heavily transform data. 

Things to note:

- There are different types of models that typically exist in each of the above categories. See Model Layers for more information.
- Read How we structure our dbt projects for an example and more details around organization.

## Model Layers

- Only models in `staging` should select from sources.
- Models not in the `staging` folder should select from refs.

## Model File Naming and Coding

- All objects should be plural.  
  Example: `stg_stripe__invoices.sql` vs. `stg_stripe__invoice.sql`

- All models should use the naming convention `<type/dag_stage>_<source/topic>__<additional_context>`. See this article for more information.

  - Models in the **staging** folder should use the source's name as the `<source/topic>` and the entity name as the `additional_context`.

    Examples:

    - seed_snowflake_spend.csv
    - base_stripe\_\_invoices.sql
    - stg_stripe\_\_customers.sql
    - stg_salesforce\_\_customers.sql
    - int_customers\_\_unioned.sql
    - fct_orders.sql

- Schema, table, and column names should be in `snake_case`.

- Limit the use of abbreviations that are related to domain knowledge. An onboarding employee will understand `current_order_status` better than `current_os`.

- Use names based on the _business_ rather than the source terminology.

- Each model should have a primary key to identify the unique row and should be named `<object>_id`. For example, `account_id`. This makes it easier to know what `id` is referenced in downstream joined models.

- For `base` or `staging` models, columns should be ordered in categories, where identifiers are first and date/time fields are at the end.
- Date/time columns should be named according to these conventions:

  - Timestamps: `<event>_at`  
    Format: UTC  
    Example: `created_at`

  - Dates: `<event>_date`
    Format: Date  
    Example: `created_date`

- Booleans should be prefixed with `is_` or `has_`.
  Example: `is_active_customer` and `has_admin_access`

- Price/revenue fields should be in decimal currency (for example, `19.99` for $19.99; many app databases store prices as integers in cents). If a non-decimal currency is used, indicate this with suffixes. For example, `price_in_cents`.

- Avoid using reserved words (such as these for Snowflake) as column names.

- Consistency is key! Use the same field names across models where possible. For example, a key to the `customers` table should be named `customer_id` rather than `user_id`.

## Model Configurations

- Model configurations at the folder level should be considered (and if applicable, applied) first.
- More specific configurations should be applied at the model level using one of these methods.
- Models within the `marts` folder should be materialized as `table` or `incremental`.
  - By default, `marts` should be materialized as `table` within `dbt_project.yml`.
  - If switching to `incremental`, this should be specified in the model's configuration.

## Testing

- At a minimum, `unique` and `not_null` tests should be applied to the expected primary key of each model.

## CTEs

For more information about why we use so many CTEs, read this glossary entry.

- Where performance permits, CTEs should perform a single, logical unit of work.
- CTE names should be as verbose as needed to convey what they do.
- CTEs with confusing or noteable logic should be commented with SQL comments as you would with any complex functions and should be located above the CTE.
- CTEs duplicated across models should be pulled out and created as their own models.