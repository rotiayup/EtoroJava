package com.etorogroup.etoroartifact.services;

import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Service
public class Etoro2023MovimientosService {

    private final DataSource dataSource;

    @Autowired
    public Etoro2023MovimientosService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Transactional
    public void insertFromExcel() {
        String vTablaBase = "etoro_2024_movimientos";
        String vTablaBaseExcel = "etoro_2024_movimientos_desde_excel";

        String strSql = "INSERT INTO " + vTablaBase + " (fecha, tipo, detalles, importe, unidades, tipoactivo) " +
                "SELECT STR_TO_DATE(fecha, '%d/%m/%Y'), tipo, detalles, SUM(importe), " +
                "SUM(CAST(unidades AS DECIMAL(30,10))), tipoactivo FROM " + vTablaBaseExcel + " " +
                "GROUP BY fecha, tipo, detalles, tipoactivo";
//System.out.println("aitorrrSQL:"+strSql);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(strSql)) {
//            System.out.println("aitorrrSQLServiceok");
            statement.executeUpdate();
        } catch (SQLException e) {
//            System.out.println("aitorrrSQLerror");
            // Handle exception
            e.printStackTrace();
        }
    }}
