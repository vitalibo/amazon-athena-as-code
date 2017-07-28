package com.github.vitalibo.a3c.provisioner;

public interface Facade<Request, Response> {

    Response process(Request request) throws Exception;

}
