package com.sudiyi.flink.data.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author sdy
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Metric {

    private String name;

    private long timestamp;

    private Map<String, Object> fields;

    private Map<String, String> tags;

    @Override
    public String toString() {
        return "Metric{" +
                "name='" + name + '\'' +
                ", timestamp=" + timestamp +
                ", fields=" + fields +
                ", tags=" + tags +
                '}';
    }


}
