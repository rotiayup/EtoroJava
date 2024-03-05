package com.etorogroup.etoroartifact.models;

import jakarta.persistence.*;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;
import java.util.Date;

@Entity
@Table(name = "etoro_movimientos")
@ToString
@EqualsAndHashCode
public class EtoroMovimientos {

    @Id
    @Getter
    @Setter
    @Column(name = "id_")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Getter
    @Setter
    @Column(name = "fechatope")
    private Date fechaTope;

    @Getter
    @Setter
    @Column(name = "fecha")
    private Date fecha;

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
    @Column(name = "importe")
    private BigDecimal importe;

    @Getter
    @Setter
    @Column(name = "unidades")
    private BigDecimal unidades;

    @Getter
    @Setter
    @Column(name = "tipoactivo")
    private String tipoActivo;

    @Getter
    @Setter
    @Column(name = "declaracion")
    private String declaracion;

    @Getter
    @Setter
    @Column(name = "indicebloqueo")
    private Integer indiceBloqueo;

    @Getter
    @Setter
    @Column(name = "idcompraventa")
    private Long idCompraVenta;
}
