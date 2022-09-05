package com.ti.domain;

public class SecurityInfo {
    private String date;
    private String location;

    public SecurityInfo() {
    }

    public SecurityInfo(String date, String location) {
        this.date = date;
        this.location = location;
    }

    @Override
    public String toString() {
        return "SecurityInfo{" +
                "date='" + date + '\'' +
                ", location='" + location + '\'' +
                '}';
    }
}
