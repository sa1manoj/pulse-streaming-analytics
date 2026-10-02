# Power BI report

The `.pbix` file is not committed. Build it from the dbt marts:

1. **Get Data -> Google BigQuery**, sign in, pick dataset `pulse_analytics`
   (tables `fct_daily_funnel`, `ab_variant_summary`). Import mode is fine at this size.
2. Add a Date table (Modeling -> New table):
   `Date = CALENDAR ( MIN ( fct_daily_funnel[session_date] ), MAX ( fct_daily_funnel[session_date] ) )`,
   mark it as the date table, and relate `'Date'[Date]` -> `fct_daily_funnel[session_date]`.
3. Paste the measures from `measures.dax`.
4. Suggested pages:
   * **Funnel**: sessions -> cart -> checkout -> purchase by variant (funnel visual), Conversion Rate card.
   * **Experiment**: Conversion Control vs. Treatment, Conversion Lift %, p-value from the Airflow
     `ab_report` task (the significance test is done in Python, not DAX).
   * **Revenue**: Revenue and Revenue 7d line chart, Revenue per Session.

Local alternative without GCP: `duckdb pulse.duckdb "COPY fct_daily_funnel TO 'fct_daily_funnel.csv'"`
and load the CSV.
