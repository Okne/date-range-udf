# date-range-udf
Hive user define function to generate array of dates from startDate to endDate passed as params

# How to use
- assemble jar with maven
- add date-range-1.0.jar from target folder to $HIVE_HOME/lib
- if you use HUE, then copy jar to hdfs and add it as resource to settings tab on query page
- add temporary function (e.g 'date_range') backed by com.epam.hive.udf.DateRange class
- user udf as date_range(startDate, endDate, dateFormat) - all params should be of string type, dateFormat define startDate and endDate formats, also it defines format of dates in output array with dates as string
