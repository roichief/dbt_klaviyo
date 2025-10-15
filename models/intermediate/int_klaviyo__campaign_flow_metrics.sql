-- Grain: campaign_id / flow_id / variation_id / source_relation_norm
-- Inputs:
--   - ref('klaviyo__person_campaign_flow')  (person-level metrics)
-- Output:
--   - one row per campaign/flow variation with sums + unique-person counts

with person_campaign_flow as (
    select
        *,
        /* Normalize source_relation by removing any catalog prefix, lowercasing,
           trimming, and defaulting to your var (usually 'klaviyo'). */
        coalesce(
          nullif(regexp_replace(lower(trim(source_relation)), '^[^.]+\\.', ''), ''),
          '{{ var("klaviyo__default_source_relation", "klaviyo") }}'
        ) as source_relation_norm
    from {{ ref('klaviyo__person_campaign_flow') }}
),

{% set pcf_columns = adapter.get_columns_in_relation(ref('klaviyo__person_campaign_flow')) %}

agg_metrics as (
    select
        last_touch_campaign_id,
        last_touch_flow_id,
        /* Keep only real variations (AB tests, etc.). */
        variation_id,
        /* Use the normalized source for joins downstream. */
        source_relation_norm,

        count(distinct person_id) as total_count_unique_people,
        min(first_event_at) as first_event_at,
        max(last_event_at)  as last_event_at

        {% for col in pcf_columns if col.name|lower not in [
            'last_touch_campaign_id','person_id','last_touch_flow_id',
            'source_relation','source_relation_norm',
            'campaign_name','flow_name','variation_id',
            'first_event_at','last_event_at'
        ] %}
          , sum({{ col.name }}) as {{ col.name }}

          {% if 'sum_revenue' not in col.name|lower %}
          , sum(case when {{ col.name }} > 0 then 1 else 0 end) as {{ 'unique_' ~ col.name }}
          {% endif %}
        {% endfor %}

    from person_campaign_flow
    /* IMPORTANT: enforce the variation-level grain */
    where variation_id is not null
    group by 1,2,3,4
)

select * from agg_metrics
