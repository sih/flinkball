# FlinkBall

## Overview
This uses the [Teleprinter project on this repo](https://github.com/sih/teleprinter) to provide a stream of football results from the Premier League years - actually provides both Premiership and Championship results so we are able to track West Ham's illustrious progress over the years.

## Running the consumers
See above for the dependency on the Teleprinter project. Most of the examples in this demo rely on the full set of results being created. Read the teleprinter project to find out how to do this.

1. Clone and build the teleprinter project ````https://github.com/sih/teleprinter.git ````
2. Run the SimFilePublisher class to generate a single results file at ````<teleprinter project dir>/output/results.csv````. This file is used to drive most of the examples
3. Change the absolute paths for the static file locations in  ````ResultFileNormalizer```` and ````FootyTableSourceConsumer```` to match your location
4. Take a look at the implementation examples below and run each

These will all run in the IDE and all of them will run on a Flink cluster apart from the TableAPI examples (mainly because the TableAPI isn't shipped with the binary distro and I haven't got an uber-jar working yet)

## What's implemented
### FootyResult
This is a normalized result object that holds the common fields from all of the data files.

### ResultFileNormalizer
This takes the data from the individual results files and creates a normalized output that contains the fields common to all of them.

### TeamSelecterConsumer
Takes an input from the command line of a team name, e.g. West Ham, and then returns the results for that team.

### GoalsByMonth
Outputs a tuple of month and the goals scored in that month, i.e.
````
(3,6406)
(1,4752)
(5,2521)
(11,6113)
(12,7487)
(2,5461)
(4,7264)
(8,5338)
(9,6136)
(10,6099)
```` 

### GoalsByMonthByGame
Refinement of the above to take in to account the number of games per month and hence work out the number of goals per game by month. The ulterior motive for this, apart from showing that actually December isn't an anomaly, it's just there are lots of fixtures then, is to show how two DataSets can be joined.
````
(3,2.57)
(1,2.53)
(5,2.81)
(11,2.64)
(12,2.61)
(2,2.59)
(4,2.61)
(8,2.53)
(9,2.7)
(10,2.64)
````


### FootyTableSourceConsumer
Uses the TableAPI to create and query datasets. At present this only works via the IDE rather than executing on the cluster as the TableAPI is not shipped with the binary distro.


## To Do
~~- Date-based stats, e.g.~~

  ~~- Goals per week / month~~
  
  ~~- Goals per day of week~~
  
  ~~- etc.~~
     
-  What-if scenarios where results are altered (e.g. ABU weighting)

## Findings

### The TableAPI isn't shipped with the binary distro
This means that we need to add in the relevant files (otherwise we get NoClassdefFound errors when we try to run on the cluster).

This [out of date documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.1/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution) describes how to incude the relevant classes by unpacking in the maven task and creating an uber jar. However, still getting errors - now in codehaus classes. This is currently left as a todo.

### The TableAPI writes to append-only sinks which will limit operations
(see FootyTableSourceConsumer)
The results csv is read in to a Table via a CsvSource. This is then further filtered (a WHERE clause applied to the table SQL) in order to return results for a particular team.

This is then written to a CsvTableSink but because of how the TableAPI works this type of sink (an Append-only sink where previous rows can't be revised) will only support certain types of operations.

If the results are filtered only then the output to this Sink can be append only as there are no results that need revision.

Instead though, if the results are grouped (e.g.) and then a count made of the number of wins, losses, draws then this isn't an append-only action as the counts are being updated as the stream encounters new data matching the filter. This means that the table can't be written to a CsvSink (i.e. an append only sink)

### Can't join if there are different execution contexts
(see GoalsByMonthByGame)
This originally reused the GoalsByMonth dataset and then sought to create a new dataset of games per month and join them to get the resulting goals per month per game.
However, is failed with an IllegalArgumentException as below:
````
java.lang.IllegalArgumentException: The two inputs have different execution contexts.
````
Going forward, could share the execution environment rather than what the class does (which is repeat the dataset query for goals per month)