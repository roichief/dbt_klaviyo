-- Base flow records from the staging model + a normalized source
with flow as (
  select
      created_at,
      flow_id,
      flow_name,
      status,
      updated_at,
      is_archived,
      trigger_type,
      source_relation,
      /* collapse '', null, and remove a leading catalog. */
      coalesce(
        nullif(
          regexp_replace(lower(trim(source_relation)), '^[^.]+\\.', ''),  -- drop catalog prefix
          ''
        ),
        'klaviyo'
      ) as source_relation_norm
  from {{ ref('stg_klaviyo__flow') }}
),

-- Metrics aggregated to flow/variation level
flow_metrics as (
  -- IMPORTANT: do NOT recompute source_relation_norm here to avoid duplicates.
  select *
  from {{ ref('int_klaviyo__campaign_flow_metrics') }}
),

final as (
  select
      -- stable key at flow + variation grain
      {{ dbt_utils.generate_surrogate_key(['f.flow_id','m.variation_id']) }} as flow_variation_key,

      -- flow columns (explicit list to avoid duplicates)
      f.created_at,
      f.flow_id,
      f.flow_name,
      f.status,
      f.updated_at,

      -- variation_id comes from metrics (staging may not have it)
      m.variation_id,

      -- all metric columns except key/source fields we don't want twice
      {{ dbt_utils.star(
          from=ref('int_klaviyo__campaign_flow_metrics'),
          except=[
            'last_touch_campaign_id',
            'last_touch_flow_id',
            'variation_id',
            'source_relation',
            'source_relation_norm'
          ],
          relation_alias='m'
      ) }},

      -- tail flow fields + sources
      f.is_archived,
      f.trigger_type,
      f.source_relation,
      f.source_relation_norm

  from flow as f
  left join flow_metrics as m
    on f.flow_id = m.last_touch_flow_id
   and f.source_relation_norm = m.source_relation_norm
)

select * from final;