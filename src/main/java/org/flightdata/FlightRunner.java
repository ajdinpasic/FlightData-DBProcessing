package org.flightdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;


public class FlightRunner {

    public static void main(String[] args) throws Exception {
        //set up configurations
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        //Comment or uncomment based on the task you wish to perform
        Job j = new Job(c, "Total base pay");
        j.setJarByClass(FlightRunner.class);
        j.setMapperClass(FlightMapper.class);
        j.setReducerClass(FlightReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        String dbURL = "jdbc:sqlserver://localhost\\";
        Properties properties = new Properties();
        properties.put("user", "sa");
        properties.put("password", "secret");
        Connection conn = DriverManager.getConnection(dbURL, properties);
/*

        SELECT StartingAirport AS DepartureAirport, SUM(TotalFare) AS TicketPrice FROM [dbo]. Flight GROUP BY DepartureAirport ORDER BY DepartureAirport

        SELECT EndingAirport AS ArrivingAirport, MAX(TotalFare) AS TicketPrice FROM [dbo]. Flight GROUP BY ArrivingAirport ORDER BY ArrivingAirport

        SELECT EndingAirport AS ArrivingAirport, MIN(TotalFare) AS TicketPrice FROM [dbo]. Flight GROUP BY ArrivingAirport ORDER BY ArrivingAirport

        SELECT StartingAirport AS DepartureAirport, AVG(TotalFare) AS TicketPrice FROM [dbo]. Flight GROUP BY DepartureAirport ORDER BY DepartureAirport

        DECLARE @cur_name NVARCHAR (100)
        DECLARE @cur_tax FLOAT
        DECLARE @cur_range CURSOR
        SET @cur_range = CURSOR FOR
        SELECT
                StartingAirport,
        SUM(TotalFare)
        FROM [dbo].Flight
        GROUP BY StartingAirport
        ORDER BY StartingAirport


        OPEN @cur_range
        FETCH NEXT FROM @cur_range
                INTO
        @cur_name,@cur_tax
        WHILE @@FETCH_STATUS = 0
        BEGIN
                --PRINT(@cur_name + ': ' + CAST(@cur_tax AS NVARCHAR(100)))
        IF (@cur_tax > 406866)
        BEGIN
        PRINT(@cur_name + ': ' + 'Sales of tickets were great!')
        END
        IF (@cur_tax > 204363)
        BEGIN
        PRINT(@cur_name + ': ' + 'Sales of tickets were average!')
        END
                ELSE
        BEGIN
        PRINT(@cur_name + ': ' + 'Sales of tickets were poor!')
        END
        FETCH NEXT FROM @cur_range INTO
        @cur_name,@cur_tax
        END;
        CLOSE @cur_range ;
        DEALLOCATE @cur_range; */


        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        long startTime = System.currentTimeMillis();
        j.waitForCompletion(true);
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("Time Elapsed for first job : " + estimatedTime);
/*
        Job j2 = new Job(c,"Count satisfied records");
        j2.setJarByClass(FlightRunner.class);
        j2.setMapperClass(FlightMapperCount.class);
        j2.setReducerClass(FlightReducerCount.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j2, new Path("/ticket-prices"));
        FileOutputFormat.setOutputPath(j2, new Path("/ticket-report"));

        long startTimeJob2 = System.currentTimeMillis();
        j2.waitForCompletion(true);
        long estimatedTimeJob2 = System.currentTimeMillis() - startTimeJob2;
        System.out.println("Time Elapsed for second job : " + estimatedTimeJob2); */

        System.exit(0);
    }
}