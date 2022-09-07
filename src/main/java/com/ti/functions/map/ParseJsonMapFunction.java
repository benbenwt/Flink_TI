package com.ti.functions.map;

import com.ti.domain.SecurityInfo;
import com.ti.utils.FilterAttribute;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

public class ParseJsonMapFunction implements MapFunction<String,SecurityInfo> {

    public static void main(String[] args) throws Exception {
        File jsonFile=new File("D:\\DevInstall\\IdeaProjects\\Flink_TI\\src\\main\\resources\\temp\\calypso-apt-2019-eng(10-31-2019).json");
        BufferedReader bufferedReader=new BufferedReader(new FileReader(jsonFile));
        String value=bufferedReader.readLine();
        new ParseJsonMapFunction().map(value);
    }
//
    public SecurityInfo map(String value) throws Exception {
        FilterAttribute filterAttribute=new FilterAttribute();
        Map<String,String> infoMap=filterAttribute.getAttributeString(value);

//        set value
//        查看该类有哪些属性，根据属性名取map中取值，然后调用该属性的set方法设置值。
        SecurityInfo info=new SecurityInfo();

        System.out.println(infoMap);
        for(Field field:info.getClass().getDeclaredFields())
        {
            String name=field.getName();
            if(infoMap.containsKey(name)){
                String methodName="set"+name.substring(0,1).toUpperCase()+name.substring(1);
//               name=sampletime  -> setSampletime
                Method method=info.getClass().getMethod(methodName,String.class);
                method.invoke(info,infoMap.get(name));
            }
        }
        System.out.println(info);
        return info;
    }
}
