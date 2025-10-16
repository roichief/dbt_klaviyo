with campaign as (
  select
    *,
    /* collapse '', null, and remove a leading catalog. */
    coalesce(
      nullif(regexp_replace(lower(trim(source_relation)), '^[^.]+\\.', ''), ''),
      'klaviyo'
    ) as source_relation_norm
  from {{ var('campaign') }}
),

campaign_metrics as (
  -- Do NOT re-alias source_relation_norm here; it already exists in the metrics model
  select *
  from {{ ref('int_klaviyo__campaign_flow_metrics') }}
),

campaign_join as (
  {% set exclude_fields = [
      'last_touch_campaign_id',
      'last_touch_flow_id',
      'source_relation',
      'source_relation_norm'   -- exclude to avoid duplicate with campaign.*
  ] %}

  select
    campaign.*,
    {{ dbt_utils.star(from=ref('int_klaviyo__campaign_flow_metrics'), except=exclude_fields) }}
  from campaign
  left join campaign_metrics
    on campaign.campaign_id = campaign_metrics.last_touch_campaign_id
   and campaign.source_relation_norm = campaign_metrics.source_relation_norm
),

final as (
  select
    *,
    {{ dbt_utils.generate_surrogate_key(['campaign_id','variation_id']) }} as campaign_variation_key
  from campaign_join
)

select * from final
