package com.github.vitalibo.a3c.provisioner;

import lombok.Getter;

import java.util.Map;

public class Factory {

    @Getter(lazy = true)
    private static final Factory instance = new Factory(System.getenv());

    Factory(Map<String, String> env) {

    }

}
