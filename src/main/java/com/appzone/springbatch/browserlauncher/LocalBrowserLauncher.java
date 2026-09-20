package com.appzone.springbatch.browserlauncher;





import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@Component
public class LocalBrowserLauncher {

    @Value("${server.port:8080}")
    private String port;

    @EventListener(ApplicationReadyEvent.class)
    public void openBrowserOnStartup() {
        System.setProperty("java.awt.headless", "false");
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