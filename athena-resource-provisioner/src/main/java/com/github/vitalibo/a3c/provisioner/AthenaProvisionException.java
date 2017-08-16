package com.github.vitalibo.a3c.provisioner;

public class AthenaProvisionException extends RuntimeException {

    public AthenaProvisionException() {
        super();
    }

    public AthenaProvisionException(String message) {
        super(message);
    }

    public AthenaProvisionException(String message, Throwable cause) {
        super(message, cause);
    }

    public AthenaProvisionException(Throwable cause) {
        super(cause);
    }

}
