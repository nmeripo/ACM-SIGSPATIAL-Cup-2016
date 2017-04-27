# ACM-SIGSPATIAL-Cup-2016
Focus on applying spatial statistics to spatio-temporal big data in order to identify statistically significant spatial hot spots using Apache Spark

### Instruction to run:
```
./spark-submit --class dataminers.giscup.HotSpots [path_to_jar] [path_to_input] [path_to_output]
```

## Problem Definition
### Input: 
A collection of New York City Yellow Cab taxi trip records spanning January 2009 to June 2015. The source data may be clipped to an envelope encompassing the five New York City boroughs in order to remove some of the noisy error data (e.g., latitude 40.5N – 40.9N, longitude 73.7W – 74.25W).

#### [NYC Taxi Trip dataset](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)

### Special Requirements:
To reduce the computation power: 
1. You only need to take the data of 2015 January Yellow Taxi (~1.8 GB in total). 
2. Each cell unit size is 0.01 * 0.01 in terms of latitude and longitude degrees. 
3. You should use 1 day as the Time Step Size. 
4. You only need to consider Pick-up Location. You can ignore the Drop-off Location. 
5. You may use GeoSpark Maven Coordinates to call GeoSpark API and help your coding. You even can use GeoSpark source code as a starting point and do some changes on it.

### Constraints:  
Time and space should be aggregated into cube cells as specified on command line. Together, this will form a space-time cube.

### Hints:
1. A higher Getis-Ord z score means this cell is hotter. 
2. If a cell has more taxi trip records, that means this cell is hotter and has higher z score. The number of trips in this cell is the attribute value of this cell. 
3. This spatial neighborhood is created for the preceding, current, and following time periods (i.e., each cell has 26 neighbors). For simplicity of computation, the weight of each neighbor cell is presumed to be equal. You can treat it as 1. If two cells are not neighbors, their weight is 0. 
