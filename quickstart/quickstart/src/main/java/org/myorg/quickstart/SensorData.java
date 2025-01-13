package org.myorg.quickstart;

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

// {"key": "B", "value": 370, "timestamp": 1669748895562}
public class SensorData {
    private String key;
    private int value;
    private long timestamp;

    public SensorData() {}

    public SensorData(String key, int value) {
        this.key = key;
        this.value = value;
//        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "SensorData{" + "key='" + key + '\'' + ", value=" + value + ", timestamp=" + timestamp + '}';
    }

    public String getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
