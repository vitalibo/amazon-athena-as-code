package com.github.vitalibo.a3c.provisioner.util;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.Consumer;

public class RulesTest {

    @Mock
    private Consumer<Object> mockConsumer;

    private Rules<Object> rules;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        rules = new Rules<>(mockConsumer);
    }

    @Test
    public void testVerify() {
        Object o = new Object();

        rules.verify(o);

        Mockito.verify(mockConsumer).accept(o);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testThrowException() {
        Object o = new Object();
        Mockito.doThrow(RuntimeException.class).when(mockConsumer).accept(o);

        rules.verify(o);

        Mockito.verify(mockConsumer).accept(o);
    }

}