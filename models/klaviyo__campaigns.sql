with campaign as (
  select
    *,
    /* Normalize `source_relation` by dropping any leading catalog and defaulting when blank */
    coalesce(
      nullif(
        regexp_replace(lower(trim(source_relation)), '^[^.]+\\.', ''),  -- drop catalog prefix
        ''
      ),
      '{{ var("klaviyo__default_source_relation", "klaviyo") }}'
    ) as source_relation_norm
  from {{ var('campaign') }}
),

campaign_metrics as (
  select
    *,
    /* Normalize to match join key */
    coalesce(
      nullif(
        regexp_replace(lower(trim(source_relation)), '^[^.]+\\.', ''),
        ''
      ),
      '{{ var("klaviyo__default_source_relation", "klaviyo") }}'
    ) as source_relation_norm
  from {{ ref('int_klaviyo__campaign_flow_metrics') }}
),

campaign_join as (
  {# prevent duplicate columns when adding the metrics star #}
  {% set exclude_fields = [
      'last_touch_campaign_id',
      'last_touch_flow_id',
      'source_relation',
      'source_relation_norm'
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
