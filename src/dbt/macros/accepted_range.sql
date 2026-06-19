{% test accepted_range(model, column_name, min_value=None, max_value=None, inclusive=True) %}
select *
from {{ model }}
where
    {% if min_value is not none %}
        {{ column_name }} {{ '<' if inclusive else '<=' }} {{ min_value }}
    {% elif max_value is not none %}
        {{ column_name }} {{ '>' if inclusive else '>=' }} {{ max_value }}
    {% else %}
        false
    {% endif %}
{% if min_value is not none and max_value is not none %}
    or {{ column_name }} {{ '>' if inclusive else '>=' }} {{ max_value }}
{% endif %}
{% endtest %}
