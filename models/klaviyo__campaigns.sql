{{ config(materialized='table') }}

WITH campaigns AS (
    SELECT * 
    FROM {{ ref('stg_klaviyo_campaigns') }}
),

person_campaigns AS (
    SELECT 
        last_touch_campaign_id AS campaign_id,
        variation_id,
        {% for metric in var('klaviyo__count_metrics', []) %}
            SUM({{ 'count_' ~ metric|lower|replace(' ', '_') }}) AS count_{{ metric|lower|replace(' ', '_') }},
            SUM(CASE WHEN {{ 'count_' ~ metric|lower|replace(' ', '_') }} > 0 THEN 1 ELSE 0 END) AS unique_count_{{ metric|lower|replace(' ', '_') }},
        {% endfor %}
        {% for metric in var('klaviyo__sum_revenue_metrics', []) %}
            SUM({{ 'sum_revenue_' ~ metric|lower|replace(' ', '_') }}) AS sum_revenue_{{ metric|lower|replace(' ', '_') }},
            {% if metric not in var('klaviyo__count_metrics', []) %}
                SUM(CASE WHEN {{ 'sum_revenue_' ~ metric|lower|replace(' ', '_') }} > 0 THEN 1 ELSE 0 END) AS unique_count_{{ metric|lower|replace(' ', '_') }},
            {% endif %}
        {% endfor %}
        COUNT(DISTINCT person_id) AS total_count_unique_people,
        MIN(first_event_at) AS first_event_at,
        MAX(last_event_at) AS last_event_at,
        source_relation
    FROM {{ ref('klaviyo__person_campaign_flow') }}
    WHERE last_touch_campaign_id IS NOT NULL
    GROUP BY campaign_id, variation_id, source_relation
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            "cast(campaign_id as string)",
            "cast(variation_id as string)",
            "coalesce(source_relation, '')"
        ]) }} AS campaign_variation_key,
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
        c.variation_id,
        m.total_count_unique_people,
        m.first_event_at,
        m.last_event_at,
        {% for metric in var('klaviyo__count_metrics', []) -%}
            m.count_{{ metric|lower|replace(' ', '_') }},
            m.unique_count_{{ metric|lower|replace(' ', '_') }},
        {%- endfor %}
        {% for metric in var('klaviyo__sum_revenue_metrics', []) -%}
            m.sum_revenue_{{ metric|lower|replace(' ', '_') }},
            {% if metric not in var('klaviyo__count_metrics', []) %}
                m.unique_count_{{ metric|lower|replace(' ', '_') }},
            {% endif %}
        {%- endfor %}
        c.is_archived,
        c.scheduled_at,
        c.source_relation
    FROM campaigns AS c
    LEFT JOIN person_campaigns AS m
      ON c.campaign_id = m.campaign_id 
      AND c.variation_id = m.variation_id 
      AND c.source_relation = m.source_relation
)

SELECT * FROM final;
