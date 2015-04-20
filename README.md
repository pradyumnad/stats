# Stats:
-- Me are implementing Map Reduce on STATISTIC functions like Count,max,min,Mean,Standard deviation,25th,50th adn 70th percentile.

# Requirements:
-- IBM BigInsights

# Implementation:
Step 1: Open the eclipse in BigInsights.
Step 2: Get the FunStats code in eclipse.
Step 1: Statistics.Java has the required code for iplementaion.
Step 2: Get the jar file for Statistics.
Step 3: Open BigInsights terminal and go the path containing the package FunStats
Step 4: Execute the following command
        >hadoop jar Statistics.jar Statistics InputPath OutputPath.
        Ex: >hadoop jar stats.jar Statistics Statistics_Input/ Output_logs/
        Here our Input file path is in Statistics_Input and Output file path is Output_logs

        Note : Make sure You are using unique Output file (Which is not already in the path).
Step 5: Wait for Map Reduce to implement.
Step 6: Check the output file in Hadoop File Broser.

# Resources:
-- http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html