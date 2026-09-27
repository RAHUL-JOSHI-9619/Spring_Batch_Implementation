package com.appzone.springbatch.browserlauncher;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@Component
public class LocalBrowserLauncher {

    private final ServerPortHolder serverPortHolder;

    public LocalBrowserLauncher(ServerPortHolder serverPortHolder) {
        this.serverPortHolder = serverPortHolder;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void openBrowserOnStartup() {
        System.setProperty("java.awt.headless", "false");

        // Use the port the server actually bound to, not the configured
        // server.port value - those differ whenever the preferred port was
        // busy and PortAvailabilityCustomizer fell back to a free one.
        int port = serverPortHolder.getPort();
        String url = "http://localhost:" + port;

        // Preferred cross-platform method using Desktop API
        if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
            try {
                Desktop.getDesktop().browse(new URI(url));
                return;
            } catch (IOException | URISyntaxException e) {
                e.printStackTrace();
            }
        }

        // Fallback using ProcessBuilder with explicit array arguments (non-deprecated)
        String os = System.getProperty("os.name").toLowerCase();
        ProcessBuilder processBuilder = new ProcessBuilder();

        if (os.contains("win")) {
            // Windows: rundll32 url.dll,FileProtocolHandler <url>
            processBuilder.command("rundll32", "url.dll,FileProtocolHandler", url);
        } else if (os.contains("mac")) {
            // macOS: open <url>
            processBuilder.command("open", url);
        } else if (os.contains("nix") || os.contains("nux")) {
            // Linux: xdg-open <url>
            processBuilder.command("xdg-open", url);
        }

        try {
            processBuilder.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}