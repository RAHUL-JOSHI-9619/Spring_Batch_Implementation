package com.appzone.springbatch.controllers;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicLong;

@Service
@EnableScheduling
@RestController
public class HeartBeatController {

    @Autowired
    private ConfigurableApplicationContext context;

    private final AtomicLong lastHeartbeat = new AtomicLong(System.currentTimeMillis());

    @PostMapping("/api/ping")
    public void receivePing() {
        lastHeartbeat.set(System.currentTimeMillis());
    }

    // Check every 5 seconds if the app has received a ping recently
    @Scheduled(fixedRate = 5000)
    public void checkHeartbeat() {
        long timeSinceLastPing = System.currentTimeMillis() - lastHeartbeat.get();
        
        // If no ping for more than 10 seconds, close the app
        if (timeSinceLastPing > 10000) {
            System.out.println("No browser tab detected. Shutting down application...");
            context.close();
            System.exit(0);
        }
    }
}
