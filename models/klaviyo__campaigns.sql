-- Base campaign records from the staging model + a normalized source
with campaign as (
  select
      campaign_type,
      created_at,
      email_template_id,
      from_email,
      from_name,
      campaign_id,
      campaign_name,
      scheduled_to_send_at,
      sent_at,
      status,
      status_id,
      subject,
      updated_at,
      is_archived,
      scheduled_at,
      source_relation,
      /* collapse '', null, and remove a leading catalog. */
      coalesce(
        nullif(
          regexp_replace(lower(trim(source_relation)), '^[^.]+\\.', ''),  -- drop catalog prefix
          ''
        ),
        'klaviyo'
      ) as source_relation_norm
  from {{ ref('stg_klaviyo__campaign') }}
),

-- Metrics aggregated to campaign/variation level
campaign_metrics as (
  -- IMPORTANT: do NOT recompute source_relation_norm here to avoid duplicates.
  select *
  from {{ ref('int_klaviyo__campaign_flow_metrics') }}
),

final as (
  select
      -- stable key at campaign + variation grain
      {{ dbt_utils.generate_surrogate_key(['c.campaign_id','m.variation_id']) }} as campaign_variation_key,

      -- campaign columns (explicit list to avoid duplicates)
      c.campaign_type,
      c.created_at,
      c.email_template_id,
      c.from_email,
      c.from_name,
      c.campaign_id,
      c.campaign_name,
      c.scheduled_to_send_at,
      c.sent_at,
      c.status,
      c.status_id,
      c.subject,
      c.updated_at,

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

      -- tail campaign fields + sources
      c.is_archived,
      c.scheduled_at,
      c.source_relation,
      c.source_relation_norm

  from campaign as c
  left join campaign_metrics as m
    on c.campaign_id = m.last_touch_campaign_id
   and c.source_relation_norm = m.source_relation_norm
)

select * from final;