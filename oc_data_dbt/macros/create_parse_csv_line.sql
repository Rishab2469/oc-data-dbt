{% macro create_parse_csv_line() %}
CREATE OR REPLACE FUNCTION {{ target.database }}.{{ target.schema }}.PARSE_CSV_LINE(line STRING)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'parse_csv'
AS
$$
import csv
from io import StringIO
def parse_csv(line):
    reader = csv.reader(StringIO(line))
    return next(reader)
$$;
{% endmacro %}
