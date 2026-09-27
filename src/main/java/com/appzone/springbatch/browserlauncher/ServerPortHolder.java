package com.appzone.springbatch.browserlauncher;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Holds the port the embedded server actually bound to.
 *
 * This is needed because when the preferred port is busy we ask the server
 * to bind to port 0 ("pick any free port"), so the real port is only known
 * once the server has actually started - it can't be read from
 * @Value("${server.port}") anymore (that would just be "0").
 */
@Component
public class ServerPortHolder {

    private final AtomicInteger port = new AtomicInteger(-1);

    public void setPort(int port) {
        this.port.set(port);
    }

    public int getPort() {
        return port.get();
    }
}
