-- Aggregates person-level metrics to the (campaign|flow)-variation grain.

with person_campaign_flow as (
    select *
    from {{ ref('klaviyo__person_campaign_flow') }}
),

{%- set pcf_columns = adapter.get_columns_in_relation(ref('klaviyo__person_campaign_flow')) %}

agg_metrics as (
    select
        last_touch_campaign_id,
        last_touch_flow_id,
        variation_id,
        -- keep the raw source_relation here; we'll normalize in the consumer
        source_relation,
        count(distinct person_id) as total_count_unique_people,
        min(first_event_at) as first_event_at,
        max(last_event_at) as last_event_at

        {#-- Sum and unique counts for all metric columns #}
        {% for col in pcf_columns
           if col.name|lower not in [
             'last_touch_campaign_id','person_id','last_touch_flow_id','source_relation',
             'campaign_name','flow_name','variation_id','first_event_at','last_event_at'
           ] %}
          , sum({{ col.name }}) as {{ col.name }}
          {% if 'sum_revenue' not in col.name|lower %}
          , sum(case when {{ col.name }} > 0 then 1 else 0 end) as {{ 'unique_' ~ col.name }}
          {% endif %}
        {% endfor -%}

    from person_campaign_flow
    group by 1,2,3,4
)

select * from agg_metrics
