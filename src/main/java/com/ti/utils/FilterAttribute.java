package com.ti.utils;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilterAttribute {
    GenerateLocationJson generateLocationJson=new GenerateLocationJson();
    Map<String,String> convertMap=new HashMap<>();

    public Map<String,String> getAttributeString(String value)
    {
        Map<String,String> result=new HashMap<>();
        /*部分key没有，为null*/
        List<String> keyList=new ArrayList<String>(){{add("md5");add("SHA256");add("sha1");add("size");add("architecture");add("languages");add("endianess");add("type");add("sampletime");add("ip");add("url");add("cveid");add("location");add("identity");add("hdfs");}};
        for(String var:keyList)
        {
            result.put(var,"");
        }

        convertMap=generateLocationJson.generateMap();
        /*一个文件多个cve，一个漏洞事件*/
//        System.out.println("value :------------------------------------------"+ value);
        JSONObject content=new JSONObject(value);

//        System.out.println("test  this jsonstr no value for objects : "+content);
        JSONArray objects=content.getJSONArray("objects");
        Map<String,String> malware_map=new HashMap<>();
        Map<String,String> vulnerability_map=new HashMap<>();
        Map<String,String> report_map=new HashMap<>();
        Map<String,String> indicator_map=new HashMap<>();
        Map<String,String> identity_map=new HashMap<>();
        Map<String,String> addr_map=new HashMap<>();
        Map<String,String> location_map=new HashMap<>();

        for(int index=0;index<objects.length();index++)
        {
            JSONObject element=objects.getJSONObject(index);
            String objectType=element.getString("type");

//            String sampletime=element.getString("created");
//            sampletime=sampletime.substring(0,10);
//            result.put("sampletime",sampletime);

            switch (objectType){
                case "malware":
                    malware_map=getMalwareInfo(element);
                    break;
                case "vulnerability":
                    vulnerability_map=getVulnerabilityInfo(element);
                    break;
                case "report":
                    report_map=getReportInfo(element);
                    break;
                case "indicator":
                    indicator_map=getIndicatorInfo(element);
                    break;
                case "identity":
                    identity_map=getIdentityInfo(element);
                    break;
                case "addr":
                    addr_map=getAddrInfo(element);
                    break;
                case "location":
                    location_map=getLocationInfo(element);
            }
        }

        for(Map.Entry<String,String> entry:malware_map.entrySet())
        {
            result.put(entry.getKey(),entry.getValue());
        }
        for(Map.Entry<String,String> entry:vulnerability_map.entrySet())
        {
            result.put(entry.getKey(),entry.getValue());
        }
        for(Map.Entry<String,String> entry:report_map.entrySet())
        {
            result.put(entry.getKey(),entry.getValue());
        }
        for(Map.Entry<String,String> entry:indicator_map.entrySet())
        {
            result.put(entry.getKey(),entry.getValue());
        }
        for(Map.Entry<String,String> entry:identity_map.entrySet())
        {
            result.put(entry.getKey(),entry.getValue());
        }
        for(Map.Entry<String,String> entry:addr_map.entrySet())
        {
            result.put(entry.getKey(),entry.getValue());
        }
        for(Map.Entry<String,String> entry:location_map.entrySet())
        {
            result.put(entry.getKey(),entry.getValue());
        }
        return result;
    }

    private Map<String,String> getLocationInfo(JSONObject location)
    {
        Map<String,String> result=new HashMap<>();
        if(location.has("country"))
        {
            String country="";
            country=location.getString("country");

            if(convertMap.containsKey(country))
            {
                country=convertMap.get(country);
            }

            result.put("location",country);
        }
        return result;
    }
    private Map<String,String> getMalwareInfo(JSONObject malware)
    {
        JSONArray architecture,languages;
        String architecture_execution_envs="",implementation_languages="";
        if(malware.has("architecture_execution_envs"))
        {
             architecture=malware.getJSONArray("architecture_execution_envs");

            if(architecture.length()>0)
            {
                architecture_execution_envs=architecture.getString(0);
            }
        }
        if(malware.has("implementation_languages"))
        {
             languages=malware.getJSONArray("implementation_languages");
             implementation_languages="";
            if(languages.length()>0)
            {
                implementation_languages=languages.getString(0);
            }
        }


        Map<String,String> result=new HashMap<>();
        result.put("architecture",architecture_execution_envs);
        result.put("languages",implementation_languages);

        String malware_types="";
        try{

            JSONArray type=malware.getJSONArray("malware_types");
            if(type.length()>0)
            {
                malware_types=type.getString(0);
            }
        }catch (JSONException e)
        {
            System.out.println("no malware_types");
        }
        result.put("type",malware_types);

        /*endianess和size放这里*/
        String description=malware.getString("description");
        String endianess="";
        Pattern p=Pattern.compile("endianess-(.*)-");
        Matcher m=p.matcher(description);
        if(m.find())
        {
            endianess=m.group(1);
        }
        result.put("endianess",endianess);
        return result;
    }
    
    private Map<String,String> getVulnerabilityInfo(JSONObject vulnerability)
    {
        Map<String,String> result=new HashMap<>();
        String name=vulnerability.getString("name");
        result.put("cveid",name);
        result.put("type","漏洞");

//        String sampletime="";
//        sampletime=vulnerability.getString("created");
//        sampletime=sampletime.substring(0,10);
//        result.put("sampletime",sampletime);

        return result;
    }
    private Map<String,String> getReportInfo(JSONObject report)
    {
        Map<String,String> result=new HashMap<>();
        String name=report.getString("name");

        String location="[]";
        String types="";
        String sampletime="";

        String pattern="LOC-(.*)',";
        String regexResult=regexHelp(pattern,name);
        location=regexResult.equals("")?location:regexResult;
        JSONArray locationArray=new JSONArray(location);
        if(locationArray.length()>0)
        {
            System.out.println("location : "+locationArray.get(0));
            result.put("location",locationArray.get(0).toString());
        }

        String pattern1="Type-(.*)";
        types=regexHelp(pattern1,name);
        JSONArray jsonArray=new JSONArray(types);
        if(jsonArray.length()>0)
        {
//            System.out.println("type : "+jsonArray.get(0));
            result.put("type",jsonArray.get(0).toString());
        }


        sampletime=report.getString("created");
        sampletime=sampletime.substring(0,10);
        result.put("sampletime",sampletime);

        return result;
    }

    private String regexHelp(String regex,String str)
    {
        String sha256="";
        try {
            Pattern p1=Pattern.compile(regex,Pattern.DOTALL);
            Matcher m1=p1.matcher(str);
            if(m1.find())
            {
                 sha256=m1.group(1);
            }
        }catch(JSONException e)
        {
            System.out.println("no "+regex);
        }
        return  sha256;
    }
    private Map<String,String> getIndicatorInfo(JSONObject indicator)
    {
        Map<String,String> result=new HashMap<>();
        String pattern = indicator.getString("pattern");

        String regex="file:hashes.'SHA-256'='(.*)'";
        String sha256=regexHelp(regex,pattern);
        result.put("sha256",sha256);

        String regex1="file:hashes.'md5'='(.*)'";
        String md5=regexHelp(regex1,pattern);
        result.put("md5",md5);

        String regex2="file:hashes.'SHA-1'='(.*)'";
        String sha1=regexHelp(regex2,pattern);
        result.put("sha1",sha1);

        String regex3="file:size = (.*)";
        String size=regexHelp(regex3,pattern);
        result.put("size",size);
        return result;


    }
    public Map<String,String> getIdentityInfo(JSONObject addr) {
        String name=addr.getString("name");
        Map<String,String> result=new HashMap<>();
        result.put("identity",name);
        return result;
    }


    public Map<String,String> getAddrInfo(JSONObject addr)
    {
        String ip=addr.getString("value");
        Map<String,String> result=new HashMap<>();
        result.put("ip",ip);
        return result;
    }
    public static void main(String[] args) throws IOException {
        FilterAttribute filterAttribute=new FilterAttribute();
//
        File jsonFile=new File("D:\\DevInstall\\IdeaProjects\\Flink_TI\\src\\main\\resources\\temp\\calypso-apt-2019-eng(10-31-2019).json");
        BufferedReader bufferedReader=new BufferedReader(new FileReader(jsonFile));
        String value=bufferedReader.readLine();
        Map<String,String> result=filterAttribute.getAttributeString(value);
        System.out.println(result);
    }
}
