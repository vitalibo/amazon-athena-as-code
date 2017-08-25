package com.github.vitalibo.a3c.provisioner.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;

public class Rules<T> implements Verify<T> {

    private final Collection<Consumer<T>> rules;

    @SafeVarargs
    public Rules(Consumer<T>... rules) {
        this.rules = Arrays.asList(rules);
    }

    @Override
    public void verify(T o) {
        rules.forEach(rule -> rule.accept(o));
    }

}
