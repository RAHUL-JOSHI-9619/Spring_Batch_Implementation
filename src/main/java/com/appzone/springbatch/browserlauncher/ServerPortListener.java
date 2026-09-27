package com.appzone.springbatch.browserlauncher;

import org.springframework.boot.web.server.context.WebServerInitializedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Fires as soon as the embedded server (Tomcat) has actually bound to a port -
 * this happens BEFORE ApplicationReadyEvent, so by the time
 * LocalBrowserLauncher opens the browser, ServerPortHolder is guaranteed
 * to already have the real port.
 */
@Component
public class ServerPortListener {

    private final ServerPortHolder serverPortHolder;

    public ServerPortListener(ServerPortHolder serverPortHolder) {
        this.serverPortHolder = serverPortHolder;
    }

    @EventListener
    public void onWebServerInitialized(WebServerInitializedEvent event) {
        int actualPort = event.getWebServer().getPort();
        serverPortHolder.setPort(actualPort);
        System.out.println("Application is running on port: " + actualPort);
    }
}
