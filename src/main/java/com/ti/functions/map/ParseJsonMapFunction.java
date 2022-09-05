package com.ti.functions.map;

import com.ti.domain.SecurityInfo;
import org.apache.flink.api.common.functions.MapFunction;

public class ParseJsonMapFunction implements MapFunction<String,SecurityInfo> {

    public static void main(String[] args) {

    }

    public SecurityInfo map(String value) throws Exception {
        return new SecurityInfo("test","test");
    }
}
