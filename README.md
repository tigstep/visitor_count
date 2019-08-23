# visitor_count
A sample Spark exercise to count the visitors, based on given conditions. 

# how to run
<ol>
  <li>Execute <b>mvn install</b>, this will produce <b>VisitorCounter.jar</b> uber jar that can be found in <b>visitor_count/target</b> directory </li>
  <li>Execute <b>spark-submit --total-executor-cores "number_of_executors" --class VisitorCounter "path_to_the_uber_jar"/VisitorCounter.jar "path_to_input_data"/input_data.csv "outhput_path"  "path_to_log4j"/log4j.properties "path_to_user_agent_map"/user_agent_map.csv</b>
</ol>

# some notes
  <ul>
    <li><b>--total-executor-cores</b> can be modified to end up with fewer or larger number of executors, hence output partitiions. This may be iseful from the solution's visibility perspective</li>
    <li><b>input_data.csv, user_agent_map.csv and log4j.properties</b> are attched with this project and can be found at <b>/visitor_counter/src/main</b> subdirectory, for easier access
    <li>output can be found at the output_path provided with spark_submit command and will have a <b>output_solution_1, output_solution_2, output_solution_3</b> subfloders for each of the questions respectivly. Each subfloder will contain partitions in .csv format.
    <li>The core SPARK SQL statements used can be found at 
      <ul>
        <li>https://github.com/tigstep/visitor_count/blob/master/visitor_counter/src/main/java/VisitorCounter.java#L68</li>
        <li>https://github.com/tigstep/visitor_count/blob/master/visitor_counter/src/main/java/VisitorCounter.java#L109</li>
        <li>https://github.com/tigstep/visitor_count/blob/master/visitor_counter/src/main/java/VisitorCounter.java#L143</li>
      </ul>
  </ul>
