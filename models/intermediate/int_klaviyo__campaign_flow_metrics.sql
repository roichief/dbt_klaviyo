{{ config(materialized='view') }}

with person_campaign_flow as (
    select *
    from {{ ref('klaviyo__person_campaign_flow') }}
),

/*
 Ensure source_relation is normalized *before* aggregate so
 blank/NULL values collapse to the same key your dim uses.
*/
pcf_norm as (
    select
        last_touch_campaign_id,
        last_touch_flow_id,
        variation_id,
        /* collapse '', null, and remove any leading catalog. */
        coalesce(
          nullif(
            regexp_replace(lower(trim(source_relation)), '^[^.]+\\.', ''),
            ''
          ),
          'klaviyo'
        ) as source_relation_norm,

        person_id,
        first_event_at,
        last_event_at,

        /* pass through the rest of the metrics columns as-is */
        *
    from person_campaign_flow
),

{%- set pcf_columns = adapter.get_columns_in_relation(ref('klaviyo__person_campaign_flow')) %}

/* Aggregate to campaign / flow / variation / normalized source */
agg_metrics as (
    select
        last_touch_campaign_id,
        last_touch_flow_id,
        variation_id,
        source_relation_norm as source_relation,
        count(distinct person_id) as total_count_unique_people,
        min(first_event_at) as first_event_at,
        max(last_event_at)  as last_event_at

        {% for col in pcf_columns
           if col.name|lower not in [
             'last_touch_campaign_id','person_id','last_touch_flow_id','source_relation',
             'campaign_name','flow_name','variation_id','first_event_at','last_event_at'
           ] %}
            -- sum person-level metrics
          , sum({{ col.name }}) as {{ col.name }}

          {% if 'sum_revenue' not in col.name|lower %}
            -- unique people who did the event
          , sum(case when {{ col.name }} > 0 then 1 else 0 end) as {{ 'unique_' ~ col.name }}
          {% endif %}
        {% endfor %}
    from pcf_norm
    group by 1,2,3,4
)

select *
from agg_metrics
