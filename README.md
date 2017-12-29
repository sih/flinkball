# FlinkBall

## Overview
This uses the [Teleprinter project on this repo](https://github.com/sih/teleprinter) to provide a stream of football results from the Premier League years - actually provides both Premiership and Championship results so we are able to track West Ham's illustrious progress over the years.

## Running the consumers
See above for the dependency on the Teleprinter project. Most of the examples in this demo rely on the full set of results being created. Read the teleprinter project to find out how to do this.

## To Do
- Basic stats on a team
- Date-based stats, e.g.
  - Goals per week / month
  - Goals per day of week
  - etc.   
-  What-if scenarios where results are altered (e.g. ABU weighting)

## Findings

### The TableAPI isn't shipped with the binary distro
This means that we need to add in the relevant files (otherwise we get NoClassdefFound errors when we try to run on the cluster).

This [out of date documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.1/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution) describes how to incude the relevant classes by unpacking in the maven task and creating an uber jar. However, still getting errors - now in codehaus classes. This is currently left as a todo.
### Understand the Table API
(see FootyTableSourceConsumer)
The results csv is read in to a Table via a CsvSource. This is then further filtered (a WHERE clause applied to the table SQL) in order to return results for a particular team.

This is then written to a CsvTableSink but because of how the TableAPI works this type of sink (an Append-only sink where previous rows can't be revised) will only support certain types of operations.

If the results are filtered only then the output to this Sink can be append only as there are no results that need revision.

Instead though, if the results are grouped (e.g.) and then a count made of the number of wins, losses, draws then this isn't an append-only action as the counts are being updated as the stream encounters new data matching the filter. This means that the table can't be written to a CsvSink (i.e. an append only sink)
