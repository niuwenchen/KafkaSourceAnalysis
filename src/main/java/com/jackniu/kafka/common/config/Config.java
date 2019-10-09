package com.jackniu.kafka.common.config;

import com.sun.applet2.preloader.event.ConfigEvent;

import java.util.List;

public class Config  {
    private  final List<ConfigValue> configValues;
    public Config(List<ConfigValue> configValues) {this.configValues = configValues ;}
    public List<ConfigValue> configValues() {return this.configValues ;}
}
