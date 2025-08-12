-- File: klaviyo_main/models/klaviyo/klaviyo__events.sql

{{
    config(
        materialized='incremental',
        unique_key='unique_event_id',
        incremental_strategy = 'merge' if target.type not in ('snowflake', 'postgres', 'redshift') else 'delete+insert',
        file_format = 'delta'
    )
}}

with events as (

    select *
    from {{ ref('int_klaviyo__event_attribution') }}

    {% if is_incremental() %}
      where _fivetran_synced >= cast(coalesce(
              (
                  select {{ dbt.dateadd(
                      datepart = 'hour',
                      interval = -1,
                      from_date_or_timestamp = 'max(_fivetran_synced)'
                  ) }}
                  from {{ this }}
              ),
              '2012-01-01'
          ) as {{ dbt.type_timestamp() }} )
    {% endif %}
),

event_fields as (

    {% set exclude_fields = [
        'touch_session','last_touch_id','session_start_at','session_event_type','type','session_touch_type',
        'campaign_name','campaign_type','campaign_subject_line','subject',
        'flow_name',
        'city','country','region','email','timezone',
        'person_city','person_country','person_region','person_email','person_timezone',
        'integration_id','integration_name','integration_category'
    ] %}

    select
        {{ dbt_utils.star(from=ref('int_klaviyo__event_attribution'), except=exclude_fields) }},
        type,
        case when session_touch_type = 'campaign' then last_touch_id end as last_touch_campaign_id,
        case when session_touch_type = 'flow'     then last_touch_id end as last_touch_flow_id,
        case when last_touch_id is not null then session_start_at   end as last_touch_at,
        case when last_touch_id is not null then session_event_type end as last_touch_event_type,
        case when last_touch_id is not null then session_touch_type end as last_touch_type
    from events
),

campaign as ( select * from {{ var('campaign') }} ),
flow    as ( select * from {{ var('flow') }} ),
person  as ( select * from {{ var('person') }} ),
metric  as ( select * from {{ var('metric') }} ),

join_fields as (
    select
        ef.*,
        c.campaign_name,
        c.campaign_type,
        c.subject as campaign_subject_line,
        f.flow_name,
        p.city     as person_city,
        p.country  as person_country,
        p.region   as person_region,
        p.email    as person_email,
        p.timezone as person_timezone,
        m.integration_id,
        m.integration_name,
        m.integration_category
    from event_fields ef
    left join campaign c
      on ef.last_touch_campaign_id = c.campaign_id
     and ef.source_relation       = c.source_relation
    left join flow f
      on ef.last_touch_flow_id    = f.flow_id
     and ef.source_relation       = f.source_relation
    left join person p
      on ef.person_id             = p.person_id
     and ef.source_relation       = p.source_relation
    left join metric m
      on ef.metric_id             = m.metric_id
     and ef.source_relation       = m.source_relation
)

select * from join_fields
