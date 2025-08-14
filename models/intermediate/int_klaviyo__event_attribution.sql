{{
    config(
        materialized='incremental',
        unique_key='unique_event_id',
        incremental_strategy = 'merge' if target.type not in ('postgres', 'redshift') else 'delete+insert',
        file_format = 'delta'
    )
}}

with events as (

    select 
        *,
        -- no event will be attributed to both a campaign and flow
        coalesce(campaign_id, flow_id) as touch_id,
        case 
            when campaign_id is not null then 'campaign' 
            when flow_id is not null then 'flow' 
        else null end as touch_type -- defintion: touch = interaction with campaign/flow

    from {{ var('event_table') }}

    {% if is_incremental() %}
    -- grab **ALL** events for users who have any events in this new increment
    where person_id in (

        select distinct person_id
        from {{ var('event_table') }}

        -- most events (from all kinds of integrations) at least once every hour
        -- https://help.klaviyo.com/hc/en-us/articles/115005253208
        where _fivetran_synced >= cast(coalesce( 
            (
                select {{ dbt.dateadd(datepart = 'hour', 
                                            interval = -1,
                                            from_date_or_timestamp = 'max(_fivetran_synced)' ) }}  
                from {{ this }}
            ), '2012-01-01') as {{ dbt.type_timestamp() }} ) -- klaviyo was founded in 2012, so let's default the min date to then
    )
    {% endif %}
),

-- sessionize events based on attribution eligibility -- is it the right kind of event, and does it have a campaign or flow?
create_sessions as (
    select
        *,
        sum(case when touch_id is not null
        {% if var('klaviyo__eligible_attribution_events') != [] %}
            and lower(type) in {{ "('" ~ (var('klaviyo__eligible_attribution_events') | join("', '")) ~ "')" }}
        {% endif %}
            then 1 else 0 end) over (
                partition by person_id, source_relation order by occurred_at asc rows between unbounded preceding and current row) as touch_session 

    from events

),

last_touches as (

    select 
        *,
        min(occurred_at) over(partition by person_id, source_relation, touch_session) as session_start_at,

        first_value(type) over(
            partition by person_id, source_relation, touch_session order by occurred_at asc rows between unbounded preceding and current row) as session_event_type

    from create_sessions
),

attribute as (

    select 
        *,
        coalesce(touch_id,
            case 
            when {{ dbt.datediff('session_start_at', 'occurred_at', 'hour') }} <= (
                case 
                when lower(session_event_type) like '%sms%' then {{ var('klaviyo__sms_attribution_lookback') }}
                else {{ var('klaviyo__email_attribution_lookback') }} end
            )
            then first_value(touch_id) over (
                partition by person_id, source_relation, touch_session order by occurred_at asc rows between unbounded preceding and current row)
            else null end) as last_touch_id

    from last_touches 
),

final as (

    select
        *,

        coalesce(touch_type, first_value(touch_type) over(
            partition by person_id, source_relation, touch_session order by occurred_at asc rows between unbounded preceding and current row)) 

            as session_touch_type

    from attribute 
)

select * from final
