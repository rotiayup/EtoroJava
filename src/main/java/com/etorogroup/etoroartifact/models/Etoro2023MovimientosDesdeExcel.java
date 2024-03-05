package com.etorogroup.etoroartifact.models;

import jakarta.persistence.*;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import java.math.BigDecimal;

@Entity
@Table(name = "etoro_2023_movimientos_desde_excel")
@ToString
@EqualsAndHashCode
public class Etoro2023MovimientosDesdeExcel {

    @Id
    @Getter
    @Setter
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_")
    private Long id;

    @Getter
    @Setter
    @Column(name = "fecha")
    private String fecha;

    @Getter
    @Setter
    @Column(name = "hora")
    private String hora;

    @Getter
    @Setter
    @Column(name = "tipo")
    private String tipo;

    @Getter
    @Setter
    @Column(name = "detalles")
    private String detalles;

    @Getter
    @Setter
    @Column(name = "unidades")
    private String unidades;

    @Getter
    @Setter
    @Column(name = "importe")
    private BigDecimal importe;

    @Getter
    @Setter
    @Column(name = "idposicion")
    private String idPosicion;

    @Getter
    @Setter
    @Column(name = "tipoactivo")
    private String tipoActivo;
}
