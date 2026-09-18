{#
  Reporting period macros for monthly_regulatory_reporting.sas.

  The SAS program is called as %monthly_regulatory_reporting(report_month=&PREV_YM)
  and derives:
      month_end    = intnx(month, <first of report_month>, 0, E)
      rpt_label    = YYYY-MM
  It then reads STG_BANK.CUST_ACCOUNTS_DAILY where SNAPSHOT_DATE = "&month_end"d.

  The batch runs on the month-end snapshot, so the report month is the month of
  the business date (Config/autoexec_local.sas: CURR_DT=31JAN2024, PREV_YM=202401).
#}

{% macro sas_month_end() %}
    last_day({{ sas_run_date() }})
{%- endmacro %}

{% macro sas_report_month() %}
    date_format(last_day({{ sas_run_date() }}), 'yyyyMM')
{%- endmacro %}
