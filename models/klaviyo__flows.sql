with flow as (
  select
    *,
    /* collapse '', null, and remove a leading catalog. */
    coalesce(
      nullif(regexp_replace(lower(trim(source_relation)), '^[^.]+\\.', ''), ''),
      'klaviyo'
    ) as source_relation_norm
  from {{ var('flow') }}
),

flow_metrics as (
  -- Do NOT re-alias source_relation_norm here; it already exists in the metrics model
  select *
  from {{ ref('int_klaviyo__campaign_flow_metrics') }}
),

flow_join as (
  {% set exclude_fields = [
      'last_touch_campaign_id',
      'last_touch_flow_id',
      'source_relation',
      'source_relation_norm'   -- exclude to avoid duplicate with flow.*
  ] %}

  select
    flow.*,
    {{ dbt_utils.star(from=ref('int_klaviyo__campaign_flow_metrics'), except=exclude_fields) }}
  from flow
  left join flow_metrics
    on flow.flow_id = flow_metrics.last_touch_flow_id
   and flow.source_relation_norm = flow_metrics.source_relation_norm
),

final as (
  select
    *,
    {{ dbt_utils.generate_surrogate_key(['flow_id','variation_id']) }} as flow_variation_key
  from flow_join
)

select * from final
