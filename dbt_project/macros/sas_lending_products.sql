/*
  Product-class lists shared by credit_risk_scoring.sas and
  monthly_regulatory_reporting.sas. Kept as macros so every model uses the
  exact same IN-list the SAS programs hard-code.
*/

{# ACCOUNT_TYPE in ('MTG','AUTO','PERS','CC','LOC','HELC') #}
{% macro sas_lending_products() -%}
('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
{%- endmacro %}

{# secured products for LTV / LGD: ACCOUNT_TYPE in ('MTG','AUTO','HELC') #}
{% macro sas_secured_products() -%}
('MTG', 'AUTO', 'HELC')
{%- endmacro %}

{# revolving products for utilisation / EAD: ACCOUNT_TYPE in ('CC','LOC','HELC') #}
{% macro sas_revolving_products() -%}
('CC', 'LOC', 'HELC')
{%- endmacro %}

{# deposit products (NEG_BAL exception): ACCOUNT_TYPE in ('CHK','SAV','MMA','CD') #}
{% macro sas_deposit_products() -%}
('CHK', 'SAV', 'MMA', 'CD')
{%- endmacro %}
