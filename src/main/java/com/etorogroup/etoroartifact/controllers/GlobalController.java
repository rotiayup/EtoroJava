package com.etorogroup.etoroartifact.controllers;

import com.etorogroup.etoroartifact.services.GlobalProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class GlobalController {

    private final GlobalProcessService service;

    @Autowired
    public GlobalController(GlobalProcessService service) {
        this.service = service;
    }

    @PostMapping("runGlobalProcess")
    public ResponseEntity<String> runGlobalProcess() {
        try {
            service.runGlobalProcess();
            //System.out.println("aitorrrSQLControllerok");
            return ResponseEntity.ok("Process finishes succesfully");
        } catch (Exception e) {
            System.out.println("Exception occurred: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to run proccess");
        }
    }
}
