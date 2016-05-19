create constraint on (a:Airport) assert a.IATA is unique;

using periodic commit

load csv with headers from
'file:///Users/peterm/git/thingspan-flights/data/airports/csv/airports.csv' as line
with line limit 1
return line
