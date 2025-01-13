package org.myorg.quickstart;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.IntSummaryStatistics;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

// {"key":"B","window_start":1669748895000,"window_end":1669748896000,"min_value":10,"count":10,"average":55.55,"max_value":100}
public class SensorDataStatistics {
    private String key;
    private long window_start, window_end, count;
    private int min_value, max_value;
    private double average;

    public SensorDataStatistics() {}
    public SensorDataStatistics(IntSummaryStatistics s, String k, TimeWindow tw) {
        this.key = k;
        this.window_start = tw.getStart();
        this.window_end = tw.getEnd();
        this.min_value = s.getMin();
        this.max_value = s.getMax();
        this.count = s.getCount();
        this.average = s.getAverage();
    }

    @Override
    public String toString() {
        return "SensorDataStatistics{" +
                "key='" + key + '\'' +
                ", window_start=" + window_start +
                ", window_end=" + window_end +
                ", count=" + count +
                ", min_value=" + min_value +
                ", max_value=" + max_value +
                ", average=" + average +
                '}';
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getWindow_start() {
        return window_start;
    }

    public void setWindow_start(long window_start) {
        this.window_start = window_start;
    }

    public long getWindow_end() {
        return window_end;
    }

    public void setWindow_end(long window_end) {
        this.window_end = window_end;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public int getMin_value() {
        return min_value;
    }

    public void setMin_value(int min_value) {
        this.min_value = min_value;
    }

    public int getMax_value() {
        return max_value;
    }

    public void setMax_value(int max_value) {
        this.max_value = max_value;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }
}
