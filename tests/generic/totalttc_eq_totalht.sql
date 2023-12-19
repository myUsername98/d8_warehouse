{% test totalttc_eq_totalht(model, column_name, column_name1) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} = {{ column_name1 }}

{% endtest %}