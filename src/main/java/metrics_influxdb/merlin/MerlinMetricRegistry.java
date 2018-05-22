package metrics_influxdb.merlin;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import metrics_influxdb.measurements.Measure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author magic
 * @date 2018/5/18 18:37
 * @version 1.0
 * Description MerlinMetricRegistry
 */
public class MerlinMetricRegistry extends MetricRegistry {

    Logger logger = LoggerFactory.getLogger(MerlinMetricRegistry.class);

    private LinkedBlockingQueue<MetricEvent> metricEvents = new LinkedBlockingQueue(64);

    private Object lock = new Object();

    private ConcurrentHashMap<String,MetricEvent> hasEvents = new ConcurrentHashMap<>(64);

    private ConcurrentHashMap<String,AtomicInteger> hasAtomInteger = new ConcurrentHashMap<>(64);

    public MetricEvent metric(String name) {
        MetricEvent event = new MetricEvent(name,this);
        return event;
    }

    public  LinkedBlockingQueue<MetricEvent> getMetricEvents() {
        return metricEvents;
    }

    public void flushEvents(){
        try {

            ConcurrentHashMap<String,MetricEvent> copyEvents;
            ConcurrentHashMap<String,AtomicInteger> copyAtomVal;
            synchronized (lock){
                copyEvents = hasEvents;
                copyAtomVal = hasAtomInteger;
                hasEvents = new ConcurrentHashMap<>(64);
                hasAtomInteger = new ConcurrentHashMap<>(64);
            }
            for (Map.Entry<String,MetricEvent> entry : copyEvents.entrySet()){
                Long count = copyAtomVal.get(entry.getKey()).longValue();
                entry.getValue().setCount(count);
                metricEvents.put(entry.getValue());
            }
            copyEvents.clear();
            copyAtomVal.clear();
        } catch (InterruptedException e) {
            logger.error("registerEvents error: {}",e);
        }
    }


    public void registerEvents(MetricEvent event) {
        String hashCode = counterHashCode(event);
        System.out.println(hashCode);
        synchronized (lock){
            if(hasEvents.containsKey(hashCode)){
                hasAtomInteger.get(hashCode).incrementAndGet();
            }else{
                hasEvents.put(hashCode,event);
                hasAtomInteger.put(hashCode,new AtomicInteger(1));
            }
        }
    }

    private String counterHashCode(MetricEvent event) {
        StringBuilder builder = new StringBuilder(event.getName());
        for (Map.Entry<String,Metric> entry : event.getMetrics().entrySet()){
            if(entry.getValue() instanceof MerlinTag){
                MerlinTag merlinTag = (MerlinTag) entry.getValue();
                builder
                        .append(",")
                        .append(merlinTag.getTagName())
                        .append("=")
                        .append(merlinTag.getTagVal());
            }
        }
        return builder.toString();
    }
}
