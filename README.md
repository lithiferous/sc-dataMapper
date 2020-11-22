# Spark dynamic data conversion
Spark project that achieves the task of converting CSV data dynamically through logic from Json. Entire logic can be found within source code / pdf task description (./data/iDecisionGames/task.pdf).

## Dependencies
* Spark 2.4.5
* SBT 1.4.3
* Play-json 2.4.0
* Play-iteratees 2.4.0

## Running
To run tests with SBT.

```
sbt "run <path_to_csv> <path_to_json> <path_to_results>"
```

## Construct logic
* date formats
```
{"existing_col_name":"birthday","new_col_name":"d_o_b","new_data_type":"date", "fmt":"dd-MM-yyyy"}
```
* decimals
```
{"existing_col_name":"Weight (lbs)","new_col_name":"weight","new_data_type":"decimal","size":"4", "precision":"3"}
```
* sized strings
```
{"existing_col_name":"Name","new_col_name":"name","new_data_type":"varchar","size":"4"}
```

## Results
```
[{
  "Column" : "first_name",
  "Unique_values" : 2,
  "Values" : [ {
    "John" : 1
  }, {
    "Lisa" : 1
  } ]
},
{
  "Column" : "total_years",
  "Unique_values" : 1,
  "Values" : [ {
    "26" : 2
  } ]
},
{
  "Column" : "d_o_b",
  "Unique_values" : 1,
  "Values" : [ {
    "26-01-1995" : 3
  } ]
}]
```
