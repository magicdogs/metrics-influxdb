package metrics_influxdb.merlin;

import com.codahale.metrics.Metric;

public class MerlinTag implements Metric {

    private String tagName;

    private String tagVal;


    public String getTagName() {
        return tagName;
    }

    public void setTagName(String tagName) {
        this.tagName = tagName;
    }

    public String getTagVal() {
        return tagVal;
    }

    public void setTagVal(String tagVal) {
        this.tagVal = tagVal;
    }
}
