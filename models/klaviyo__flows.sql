{{ config(materialized='table') }}

WITH flows AS (
    SELECT * 
    FROM {{ ref('stg_klaviyo_flows') }}
),

person_flows AS (
    SELECT 
        last_touch_flow_id AS flow_id,
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
    WHERE last_touch_flow_id IS NOT NULL
    GROUP BY flow_id, variation_id, source_relation
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            "cast(flow_id as string)",
            "cast(variation_id as string)",
            "coalesce(source_relation, '')"
        ]) }} AS flow_variation_key,
        f.created_at,
        f.flow_id,
        f.flow_name,
        f.status,
        f.updated_at,
        f.variation_id,
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
        f.is_archived,
        f.trigger_type,
        f.source_relation
    FROM flows AS f
    LEFT JOIN person_flows AS m
      ON f.flow_id = m.flow_id 
      AND f.variation_id = m.variation_id 
      AND f.source_relation = m.source_relation
)

SELECT * FROM final;
