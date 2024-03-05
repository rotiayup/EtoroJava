package com.etorogroup.etoroartifact;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.etorogroup.etoroartifact.controllers.GlobalController;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class EtoroartifactApplication {

    public static void main(String[] args) {
        // For Running
       // SpringApplication.run(EtoroartifactApplication.class, args);

        //For debugger
        ConfigurableApplicationContext context = SpringApplication.run(EtoroartifactApplication.class, args);
        GlobalController globalController = context.getBean(GlobalController.class);
        globalController.runGlobalProcess();
    }
}
