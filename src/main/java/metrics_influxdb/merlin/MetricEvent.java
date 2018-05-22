package metrics_influxdb.merlin;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricEvent implements MetricSet {

    private MerlinMetricRegistry merlinMetricRegistry;

    private String name;

    private Long count;

    Map<String, Metric> metricMap = new ConcurrentHashMap<>();

    public MetricEvent(String name, MerlinMetricRegistry merlinMetricRegistry) {
        this.merlinMetricRegistry = merlinMetricRegistry;
        this.name = name;
    }

    public void send(){
        this.merlinMetricRegistry.registerEvents(this);
    }

    public MetricEvent putTags(String name,String value){
        MerlinTag tag = new MerlinTag();
        tag.setTagName(name);
        tag.setTagVal(value);
        metricMap.put(name,tag);
        return this;
    }

    public MetricEvent putValue(String name,String val){
        MerlinValue merlinValue = new MerlinValue();
        merlinValue.setName(name);
        merlinValue.setValue(val);
        metricMap.put(name,merlinValue);
        return this;
    }

    public String getName() {
        return name;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public Map<String, Metric> getMetrics() {
        return metricMap;
    }
}
