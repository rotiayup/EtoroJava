package com.etorogroup.etoroartifact.controllers;

import com.etorogroup.etoroartifact.services.Etoro2023MovimientosService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class Etoro2023MovimientosController {

    private final Etoro2023MovimientosService service;

    @Autowired
    public Etoro2023MovimientosController(Etoro2023MovimientosService service) {
        this.service = service;
    }

    @PostMapping("insertFromExcel")
    public ResponseEntity<String> insertFromExcel() {
        try {
            service.insertFromExcel();
            //System.out.println("aitorrrSQLControllerok");
            return ResponseEntity.ok("Data inserted successfully");
        } catch (Exception e) {
            System.out.println("Exception occurred: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to insert data from Excel");
        }
    }
    // Add more controller methods as needed

}
