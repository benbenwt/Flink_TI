package com.ti.domain;

public class SecurityInfo {
    public String sampletime;
    public String location;
    public String type;
    public String architecture;

    public SecurityInfo() {
    }

    public SecurityInfo(String sampletime, String location, String type, String architecture) {
        this.sampletime = sampletime;
        this.location = location;
        this.type = type;
        this.architecture = architecture;
    }

    public String getSampletime() {
        return sampletime;
    }

    public void setSampletime(String sampletime) {
        this.sampletime = sampletime;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getArchitecture() {
        return architecture;
    }

    public void setArchitecture(String architecture) {
        this.architecture = architecture;
    }

    @Override
    public String toString() {
        return "SecurityInfo{" +
                "sampletime='" + sampletime + '\'' +
                ", location='" + location + '\'' +
                ", type='" + type + '\'' +
                ", architecture='" + architecture + '\'' +
                '}';
    }
}
