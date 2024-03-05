package com.etorogroup.etoroartifact.services;

import com.etorogroup.etoroartifact.Struc_SplitCantImporte;
import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import com.etorogroup.etoroartifact.GlobalVariables;

@Service
public class GlobalProcessService {

    private final DataSource dataSource;

    @Autowired
    public GlobalProcessService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Transactional
    public void runGlobalProcess() throws SQLException {
        //GlobalVariables.vFechaTope = LocalDate.parse("2024-12-31");
        //GlobalVariables.vTablaTratar = "etoro_movimientos";
        //LocalDate vFechaTope = GlobalVariables.vFechaTope;
        //String vTablaTratar = GlobalVariables.vTablaTratar;
        LocalDate vFechaTope = LocalDate.parse("2023-12-31");
        String vTablaTratar = "etoro_movimientos";

        Connection connection = null; // Initialize connection outside try-with-resources
        PreparedStatement deleteStatement = null;
        PreparedStatement insertStatement = null;
        PreparedStatement selectStatement = null;

        System.out.println("aitor_hasiera");
        try {
            connection = dataSource.getConnection(); // Establish connection in a dedicated try-with-resources block

            String deleteSql = "DELETE FROM " + vTablaTratar.trim() + " WHERE fechatope = ?";
            deleteStatement = connection.prepareStatement(deleteSql);
            deleteStatement.setDate(1, Date.valueOf(vFechaTope));
            deleteStatement.executeUpdate();

            String insertSql = "INSERT INTO " + vTablaTratar.trim() +
                    "(fechatope, fecha, tipo, detalles, importe, unidades, tipoactivo, declaracion, indicebloqueo, idcompraventa)" +
                    "SELECT ?, fecha, tipo, detalles, importe, unidades, tipoactivo, declaracion, indicebloqueo, idcompraventa FROM " +
                    vTablaTratar.trim() + "_sintratar" +
                    " WHERE fecha <= ?";
            insertStatement = connection.prepareStatement(insertSql);
            insertStatement.setDate(1, Date.valueOf(vFechaTope));
            insertStatement.setDate(2, Date.valueOf(vFechaTope));

            // Execute INSERT statement
            insertStatement.executeUpdate();


            String selectSql = "SELECT detalles, tipoactivo FROM " + vTablaTratar.trim() +
                    " WHERE fechatope = ? AND tipo IN (?, ?)" +// AND tipoactivo = ?" +

                    //" and detalles='AUD/JPY'"+//aitorrrrrrrrrr

                    " GROUP BY detalles, tipoactivo ORDER BY detalles, tipoactivo";

            selectStatement = connection.prepareStatement(selectSql);
            selectStatement.setDate(1, Date.valueOf(vFechaTope)); // Use Date for MySQL compatibility
            selectStatement.setString(2, "Posicion abierta");
            selectStatement.setString(3, "Ganancias/perdidas de la operacion");
            //selectStatement.setString(4, "Acciones");

            try (ResultSet dr = selectStatement.executeQuery()) {
                while (dr.next()) {
                    String detalles = dr.getString("detalles");
                    String tipoactivo = dr.getString("tipoactivo");
                    System.out.println("aitor00:"+detalles.trim()+":"+tipoactivo.trim()+"|");
if (detalles.trim().equals("AAL/USD") && tipoactivo.trim().equals("Acciones")){
                    System.out.println("aitor01:"+detalles+":"+tipoactivo);
}


                    // Process the retrieved data using detalles and tipoactivo
                    //System.out.println("Detalles: " + detalles);
                    //System.out.println("Tipo Activo: " + tipoactivo);

                    while (true) {
                        String strSql2 = "SELECT * FROM " + vTablaTratar.trim() +
                                " WHERE fechatope = ? AND detalles = ? AND tipoactivo = ? AND (declaracion IS NULL OR LENGTH(declaracion) = 0)" +
                                " ORDER BY fecha, tipo DESC LIMIT 1";

                        // Use a prepared statement for security and efficiency
                        PreparedStatement pstmt2 = connection.prepareStatement(strSql2);
                        pstmt2.setDate(1, Date.valueOf(vFechaTope));
                        pstmt2.setString(2, detalles.trim());
                        pstmt2.setString(3, tipoactivo.trim());
System.out.println("aitorr1:"+Date.valueOf(vFechaTope)+":"+detalles.trim()+":"+tipoactivo.trim());
                        //System.out.println("Aitorr 0a:"+strSql2);
                        ResultSet dr2 = pstmt2.executeQuery();

                        //System.out.println("Aitorr 0b");
                        if (!dr2.next()) {

                            //System.out.println("Aitorr NoData");

                            // Reached the end, perform cleanup and exit nested loop
                            fgl_etoro_QuitarBloqueosFinalizados(connection, vFechaTope, vTablaTratar, detalles, tipoactivo);
                            fgl_etoro_liberar2M(connection, vFechaTope, vTablaTratar, detalles, tipoactivo, vFechaTope);

                            dr2.close();
                            pstmt2.close();
                            break; // exit while true
                        } else {

                            //System.out.println("Aitorr:" + dr2.getDate("fecha"));


                            fgl_etoro_QuitarBloqueosFinalizados(connection, vFechaTope, vTablaTratar, detalles, tipoactivo);
                            fgl_etoro_liberar2M(connection, vFechaTope, vTablaTratar, detalles, tipoactivo, dr2.getDate("fecha").toLocalDate());
                            //fgl_etoro_liberar2M(detalles, tipoactivo, dr2.getDate("fecha").toLocalDate());

                            // Si Compra
                            if ("Posicion abierta".equals(dr2.getString("tipo").trim())) {

                                //' Vamos a buscar si existen posiciones venta 2mesespostventa.
                                //' Si existe, marcaremos la compra como Bloqueador, y la posicion 2mesespostventa como Bloqueado
                                //' si quedara cantidad restante, le aplicamos simplemente como Comprado

                                //' Pero lo lógico es que la cantidad de compra y venta no coincidan, por lo que tenemos que hacer
                                //' un split de la cantidad más grande para tratar con cantidades coincidentes
                                BigDecimal cantCompra = dr2.getBigDecimal("unidades");

                                BigDecimal importeCompra = dr2.getBigDecimal("importe");

                                String sql = "SELECT * FROM " + vTablaTratar.trim() +
                                        " WHERE fechatope = '" + vFechaTope.format(DateTimeFormatter.ISO_LOCAL_DATE) + "'" +
                                        " AND detalles = '" + dr.getString("detalles").trim() + "'" +
                                        " AND tipoactivo = '" + dr.getString("tipoactivo").trim() + "'" +
                                        " AND tipo = 'Ganancias/perdidas de la operacion'" +
                                        " AND declaracion = '2mesespostventa'" +
                                        " ORDER BY fecha LIMIT 1";

                                try (PreparedStatement pstmt3 = connection.prepareStatement(sql)) {
                                    ResultSet rs3 = pstmt3.executeQuery();
                                    if (rs3.next()) {
                                        BigDecimal cantVenta = rs3.getBigDecimal("unidades");
                                        BigDecimal importeVenta = rs3.getBigDecimal("importe");

                                        Struc_SplitCantImporte splitCantImporte = fgl_SplitCantImporte(connection, vTablaTratar, vFechaTope, "C", dr2.getInt("id_"), rs3.getInt("id_"), cantCompra, importeCompra, cantVenta, importeVenta, 0);

                                        int indiceBloqueoNuevo = fgl_Contador_IndiceBloqueo(connection, vTablaTratar, vFechaTope);
                                        String updateSqlCompra = "UPDATE " + vTablaTratar.trim() +
                                                " SET declaracion = ?, indicebloqueo = ?" +
                                                " WHERE id_ = ?";

                                        PreparedStatement pstmtUpdateCompra = connection.prepareStatement(updateSqlCompra);
                                        pstmtUpdateCompra.setString(1, "Bloqueador");
                                        pstmtUpdateCompra.setInt(2, indiceBloqueoNuevo);
                                        pstmtUpdateCompra.setInt(3, dr2.getInt("id_"));
                                        pstmtUpdateCompra.executeUpdate();

                                        String updateSqlVenta = "UPDATE " + vTablaTratar.trim() +
                                                " SET declaracion = ?, indicebloqueo = ?" +
                                                " WHERE id_ = ?";

                                        PreparedStatement pstmtUpdateVenta = connection.prepareStatement(updateSqlVenta);
                                        pstmtUpdateVenta.setString(1, "Bloqueado");
                                        pstmtUpdateVenta.setInt(2, indiceBloqueoNuevo);
                                        pstmtUpdateVenta.setInt(3, rs3.getInt("id_"));
                                        pstmtUpdateVenta.executeUpdate();
                                    } else {
                                        // No hay nada por bloquear. Marcar como compra normal
                                        String updateSql = "UPDATE " + vTablaTratar.trim() +
                                                " SET declaracion = ?" +
                                                " WHERE id_ = ?";

                                        PreparedStatement pstmtUpdate = connection.prepareStatement(updateSql);
                                        pstmtUpdate.setString(1, "Comprado");
                                        pstmtUpdate.setInt(2, dr2.getInt("id_"));
                                        pstmtUpdate.executeUpdate();
                                    }
                                    rs3.close();
                                }
                            }


                            // Si venta
                            //aitorrrr
                            if ("Ganancias/perdidas de la operacion".equals(dr2.getString("tipo").trim())) {

                                BigDecimal cantVenta = dr2.getBigDecimal("unidades");
                                BigDecimal importeVenta = dr2.getBigDecimal("importe");

                                if (importeVenta.compareTo(BigDecimal.ZERO) >= 0 || "Criptomonedas".equals(dr2.getString("tipoactivo").trim())) {

                                    // No hay nada que controlar para ventas no negativas o criptomonedas
                                    // Deberíamos marcar la venta origen asociada como tratada!!!???

                                    String sql = "SELECT * FROM " + vTablaTratar.trim() +
                                            " WHERE fechatope = '" + vFechaTope.format(DateTimeFormatter.ISO_LOCAL_DATE) + "'" +
                                            " AND detalles = '" + dr.getString("detalles").trim() + "'" +
                                            " AND tipoactivo = '" + dr.getString("tipoactivo").trim() + "'" +
                                            " AND tipo = 'Posicion abierta'" +
                                            " AND declaracion = 'Comprado'" +
                                            " ORDER BY fecha LIMIT 1";

                                    try (PreparedStatement pstmt3 = connection.prepareStatement(sql)) {
                                        ResultSet rs3 = pstmt3.executeQuery();
                                        if (rs3.next()) {
                                            BigDecimal cantCompra = rs3.getBigDecimal("unidades");
                                            BigDecimal importeCompra = rs3.getBigDecimal("importe");

                                            int id3 = rs3.getInt("id_");
                                            int id2 = dr2.getInt("id_");
                                            Struc_SplitCantImporte splitCantImporte = fgl_SplitCantImporte(connection, vTablaTratar, vFechaTope, "V", rs3.getInt("id_"), dr2.getInt("id_"), cantCompra, importeCompra, cantVenta, importeVenta, 0);

                                            String updateSql = "UPDATE " + vTablaTratar.trim() +
                                                    " SET declaracion = 'Tratado'" +
                                                    " WHERE id_ = ?";

                                            PreparedStatement pstmtUpdate = connection.prepareStatement(updateSql);
                                            pstmtUpdate.setInt(1, rs3.getInt("id_"));
                                            pstmtUpdate.executeUpdate();
                                        }
                                    }
                                    sql = "UPDATE " + vTablaTratar.trim() +
                                            " SET declaracion = 'Ok'" +
                                            " WHERE id_ = ?";

                                    PreparedStatement pstmt4 = connection.prepareStatement(sql);
                                    pstmt4.setInt(1, dr2.getInt("id_")); // Assuming "id_" is an integer column
                                    pstmt4.executeUpdate();
                                } else {

                                    boolean isPending = true; // Assuming "vPendiente" translates to "isPending"


// Search for a matching "Posicion abierta" bought within the last 2 months
                                    String sql = "SELECT * FROM " + vTablaTratar.trim() +
                                            " WHERE fechatope ='" + vFechaTope.format(DateTimeFormatter.ISO_LOCAL_DATE) + "'" +
                                            " AND detalles = '" + dr.getString("detalles").trim() + "'" +
                                            " AND tipoactivo = '" + dr.getString("tipoactivo").trim() + "'" +
                                            " AND tipo = 'Posicion abierta'" +
                                            " AND declaracion = 'Comprado'" +
                                            " AND PERIOD_DIFF(DATE_FORMAT('" + dr2.getDate("fecha").toLocalDate() + "', '%Y%m'), DATE_FORMAT(fecha, '%Y%m')) < 2" +
                                            " ORDER BY fecha LIMIT 1";
//                                    " AND DATEDIFF(MONTH, fecha, '" + dr2.getDate("fecha").toLocalDate() + "') < 2" +
//System.out.println("aitorrrx1:"+sql);//aitorrr
                                    ResultSet rs3 = null;
                                    try (PreparedStatement pstmt3 = connection.prepareStatement(sql)) {
                                        rs3 = pstmt3.executeQuery();
                                        if (rs3.next()) {
                                            BigDecimal cantCompra = rs3.getBigDecimal("unidades");
                                            BigDecimal importeCompra = rs3.getBigDecimal("importe");
                                            //int idCompraBloqueador = rs3.getInt("id_"); // Assuming "id_" is an integer column

                                            // Call fgl_SplitCantImporte function with retrieved values
                                            Struc_SplitCantImporte splitCantImporte = fgl_SplitCantImporte(connection, vTablaTratar, vFechaTope, "V", rs3.getInt("id_"), dr2.getInt("id_"), cantCompra, importeCompra, cantVenta, importeVenta, 0);

                                            int newBlockIndex = fgl_Contador_IndiceBloqueo(connection, vTablaTratar, vFechaTope);

                                            // Update "Compra" (bought) record
                                            String updateCompraSql = "UPDATE " + vTablaTratar.trim() +
                                                    " SET declaracion = 'Bloqueador', indicebloqueo = ?" +
                                                    " WHERE id_ = ?";
                                            PreparedStatement updateCompraPstmt = connection.prepareStatement(updateCompraSql);
                                            updateCompraPstmt.setInt(1, newBlockIndex);
                                            updateCompraPstmt.setInt(2, rs3.getInt("id_"));
                                            updateCompraPstmt.executeUpdate();

                                            // Update "Venta" (sold) record
                                            String updateVentaSql = "UPDATE " + vTablaTratar.trim() +
                                                    " SET declaracion = 'Bloqueado', indicebloqueo = ?" +
                                                    " WHERE id_ = ?";
                                            PreparedStatement updateVentaPstmt = connection.prepareStatement(updateVentaSql);
                                            updateVentaPstmt.setInt(1, newBlockIndex);
                                            updateVentaPstmt.setInt(2, dr2.getInt("id_")); // Assuming "id_" is an integer column
                                            updateVentaPstmt.executeUpdate();

                                            isPending = false;
                                        }
                                    } catch (SQLException e) {
                                        // Handle potential SQL exceptions
                                        e.printStackTrace();
                                    } finally {
                                        // Close the result set (if not closed in the try-with-resources statement)
                                        if (rs3 != null) {
                                            try {
                                                rs3.close();
                                            } catch (SQLException e) {
                                                e.printStackTrace();
                                            }
                                        }
                                    }

                                    if (isPending) {
                                        // No pending items found to block.
                                        // Mark the sale as completed for 2M pending, and mark the purchase as okay.
                                        // In case the purchase was blocking, we also release the sale that was blocking 2M.

                                        sql = "SELECT * FROM " + vTablaTratar.trim() +
                                                " WHERE fechatope ='" + vFechaTope.format(DateTimeFormatter.ISO_LOCAL_DATE) + "'" +
                                                " AND detalles = '" + dr.getString("detalles").trim() + "'" +
                                                " AND tipoactivo = '" + dr.getString("tipoactivo").trim() + "'" +
                                                " AND tipo = 'Posicion abierta'" +
                                                " AND (declaracion = 'Comprado' OR declaracion = 'Bloqueador')" +
                                                " ORDER BY fecha LIMIT 1";

                                        try (PreparedStatement pstmt3 = connection.prepareStatement(sql)) {
                                            rs3 = pstmt3.executeQuery();
                                            if (rs3.next()) {
                                                BigDecimal cantCompra = rs3.getBigDecimal("unidades");
                                                BigDecimal importeCompra = rs3.getBigDecimal("importe");

                                                // If the split we need to make corresponds to a block, we need to assign it a different block index than the rest.
                                                int nuevoIndiceBloqueoSplit = 0;
                                                int indiceBloqueo = rs3.getInt("indicebloqueo");
                                                if (indiceBloqueo > 0) {
                                                    nuevoIndiceBloqueoSplit = fgl_Contador_IndiceBloqueo(connection, vTablaTratar, vFechaTope);
                                                }

                                                Struc_SplitCantImporte splitCantImporte = fgl_SplitCantImporte(connection, vTablaTratar, vFechaTope, "V", rs3.getInt("id_"), dr2.getInt("id_"), cantCompra, importeCompra, cantVenta, importeVenta, nuevoIndiceBloqueoSplit);

                                                // Purchase
                                                String updateCompraSql = "UPDATE " + vTablaTratar.trim() +
                                                        " SET declaracion = 'Tratado'" +
                                                        " WHERE id_ = " + rs3.getInt("id_");
                                                PreparedStatement updateCompraPstmt = connection.prepareStatement(updateCompraSql);
                                                updateCompraPstmt.executeUpdate();

                                                // Sale
                                                String updateVentaSql = "UPDATE " + vTablaTratar.trim() +
                                                        " SET declaracion = '2mesespostventa'" +
                                                        " WHERE id_ = " + dr2.getInt("id_");
                                                PreparedStatement updateVentaPstmt = connection.prepareStatement(updateVentaSql);
                                                updateVentaPstmt.executeUpdate();

                                                // If this last purchase was blocking, we need to remove the mark
                                                // and release the corresponding blocked sale.

                                                if (indiceBloqueo > 0) {
                                                    // We need the id of the associated blocked sale.

                                                    String sql2 = "SELECT * FROM " + vTablaTratar.trim() +
                                                            " WHERE fechatope ='" + vFechaTope.format(DateTimeFormatter.ISO_LOCAL_DATE) + "'" +
                                                            " AND indicebloqueo = " + indiceBloqueo +
                                                            " AND declaracion = 'Bloqueado'";

                                                    PreparedStatement pstmt4 = connection.prepareStatement(sql2);
                                                    ResultSet rs4 = pstmt4.executeQuery();
                                                    rs4.next();

                                                    splitCantImporte = fgl_SplitCantImporte(connection, vTablaTratar, vFechaTope, "V", rs3.getInt("id_"), rs4.getInt("id_"), splitCantImporte.getCompraCantidadAplicar(), splitCantImporte.getCompraImporteAplicar(), rs4.getBigDecimal("unidades"), rs4.getBigDecimal("importe"), nuevoIndiceBloqueoSplit);

                                                    int vidVentaBloqueada = rs4.getInt("id_");
                                                    rs4.close();

                                                    // Purchase
                                                    String updateCompraBloqueadaSql = "UPDATE " + vTablaTratar.trim() +
                                                            " SET declaracion = 'Tratado'" +
                                                            " ,indicebloqueo = null" +
                                                            " WHERE id_ = " + rs3.getInt("id_");
                                                    PreparedStatement updateCompraBloqueadaPstmt = connection.prepareStatement(updateCompraBloqueadaSql);
                                                    updateCompraBloqueadaPstmt.executeUpdate();

                                                    // Sale
                                                    String updateVentaBloqueadaSql = "UPDATE " + vTablaTratar.trim() +
                                                            " SET declaracion = '2mesespostventa'" +
                                                            " ,indicebloqueo = null" +
                                                            " WHERE id_ = " + vidVentaBloqueada;
                                                    PreparedStatement updateVentaBloqueadaPstmt = connection.prepareStatement(updateVentaBloqueadaSql);
                                                    updateVentaBloqueadaPstmt.executeUpdate();
                                                }
                                                isPending = false;
                                            }
                                        }
                                    }

                                    if (isPending) {
                                        // No associated purchase found!?
                                        // In theory, this should be an error. We treat it as a successfully completed sale.
                                        double importeAux = dr2.getDouble("importe");
                                        String updateVentaSql;
                                        if (importeAux >= 0) {
                                            updateVentaSql = "UPDATE " + vTablaTratar.trim() +
                                                    " SET declaracion = 'Ok'" +
                                                    " WHERE id_ = " + dr2.getInt("id_");
                                        } else {
                                            updateVentaSql = "UPDATE " + vTablaTratar.trim() +
                                                    " SET declaracion = '2mesespostventa'" +
                                                    " WHERE id_ = " + dr2.getInt("id_");
                                        }

                                        PreparedStatement updateVentaPstmt = connection.prepareStatement(updateVentaSql);
                                        updateVentaPstmt.executeUpdate();
                                    }
                                    //aitorrrrrrrrrrrrrrrrrrrr
                                }
                            }


                            dr2.close();
                            pstmt2.close();
                        }

                    }
                    //System.out.println("Aitorr 1");

                    // Process the data from the nested query
                    // ...

                }
            }


        } catch (SQLException e) {
            e.printStackTrace(); // Handle SQL exceptions appropriately
        } finally {
            // Close statements and connection in their own dedicated try-with-resources blocks for robustness
            try {
                if (deleteStatement != null) {
                    deleteStatement.close();
                }
                if (insertStatement != null) {
                    insertStatement.close();
                }
                if (selectStatement != null) {
                    selectStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace(); // Handle closing SQL objects exceptions
            }

            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace(); // Handle closing connection exception
            }
        }


        ////////////////////////////////////////////////////////////
/*
        String vTablaBase = "etoro_2023_movimientosxxx";
        String vTablaBaseExcel = "etoro_2023_movimientos_desde_excel";

        String strSql = "INSERT INTO " + vTablaBase + " (fecha, tipo, detalles, importe, unidades, tipoactivo) " +
                "SELECT STR_TO_DATE(fecha, '%d/%m/%Y'), tipo, detalles, SUM(importe), " +
                "SUM(CAST(unidades AS DECIMAL(30,10))), tipoactivo FROM " + vTablaBaseExcel + " " +
                "GROUP BY fecha, tipo, detalles, tipoactivo";
//System.out.println("aitorrrSQL:"+strSql);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(strSql)) {
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
 */
        System.out.println("aitor_hamaiera");
    }


    private void fgl_etoro_liberar2M(Connection connection, LocalDate fechatope, String vTablaTratar, String detalles, String tipoactivo, LocalDate fecha) throws SQLException {

        String sql = "UPDATE " + vTablaTratar.trim() +
                " SET declaracion = 'Ok'" +
                " WHERE fechatope = ? AND detalles = ? AND tipoactivo = ?" +
                " AND PERIOD_DIFF(DATE_FORMAT(?, '%Y%m'), DATE_FORMAT(fecha, '%Y%m')) >= 2 AND declaracion = '2mesespostventa'";
/*
        String sqlxxx = "UPDATE " + vTablaTratar.trim() +
                " SET declaracion = 'Ok'" +
                " WHERE fechatope = '"+Date.valueOf(fechatope)+"' AND detalles = '"+detalles.trim()+"' AND tipoactivo = '"+tipoactivo.trim()+"'" +
                " AND PERIOD_DIFF(DATE_FORMAT('"+Date.valueOf(fecha)+"', '%Y%m'), DATE_FORMAT(fecha, '%Y%m')) >= 2 AND declaracion = '2mesespostventa'";

        System.out.println("aitorrrx2:"+sql);//aitorrr
*/
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setDate(1, Date.valueOf(fechatope));
            pstmt.setString(2, detalles.trim());
            pstmt.setString(3, tipoactivo.trim());
            pstmt.setDate(4, Date.valueOf(fecha));
            pstmt.executeUpdate();
        }
    }


    private void fgl_etoro_QuitarBloqueosFinalizados(Connection connection, LocalDate fechaTope, String vTablaTratar, String detalles, String tipoactivo) throws SQLException {

        String sql = "SELECT * FROM " + vTablaTratar.trim() +
                " WHERE fechatope = ? AND fecha <= ? AND detalles = ? AND tipoactivo = ? AND declaracion = 'Bloqueador'" +
                " AND idcompraventa > 0";// ORDER BY fecha, tipo DESC";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setDate(1, Date.valueOf(fechaTope));
            pstmt.setDate(2, Date.valueOf(fechaTope));
            pstmt.setString(3, detalles.trim());
            pstmt.setString(4, tipoactivo.trim());

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    // Update "Posicion abierta" transactions
                    updateTransaction(connection, vTablaTratar, fechaTope, "Posicion abierta", "Tratado", rs.getInt("idcompraventa"));

                    // Update "Ganancias/perdidas de la operacion" transactions
                    updateTransaction(connection, vTablaTratar, fechaTope, "Ganancias/perdidas de la operacion", "2mesespostventa", rs.getInt("idcompraventa"));
                }
            }
        }
    }

    private void updateTransaction(Connection connection, String vTablaTratar, LocalDate fecha, String tipo, String declaracion, Integer idcompraventa) throws SQLException {
        String sql = "UPDATE " + vTablaTratar.trim() +
                " SET declaracion = ? WHERE fechatope = ? AND tipo = ? AND idcompraventa = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, declaracion.trim());
            pstmt.setDate(2, Date.valueOf(fecha));
            pstmt.setString(3, tipo.trim());
            pstmt.setInt(4, idcompraventa);
            pstmt.executeUpdate();
        }
    }


    private Struc_SplitCantImporte fgl_SplitCantImporte(Connection connection, String vTablaTratar, LocalDate fechaTope,
                                                        String vCompraVenta, Integer vIdCompra, Integer vIDVenta,
                                                        BigDecimal vCantCompraTotal, BigDecimal vImporteCompraTotal,
                                                        BigDecimal vCantVentaTotal, BigDecimal vImporteVentaTotal,
                                                        Integer vNuevoIndiceBloqueoSplit) throws SQLException {

        Struc_SplitCantImporte vSplitCantImporte = new Struc_SplitCantImporte();
        PreparedStatement pstmtCompra = null, pstmtVenta = null, pstmtInsertCompra = null, pstmtInsertVenta = null, pstmtUpdateCompra = null, pstmtUpdateVenta = null;
        ResultSet rsCompra = null, rsVenta = null;

        String sql;

        try {
            // Fetch purchase data
            sql = "SELECT * FROM " + vTablaTratar.trim() + " WHERE id_ = " + vIdCompra;
            pstmtCompra = connection.prepareStatement(sql);
            rsCompra = pstmtCompra.executeQuery();
            if (rsCompra.next()) {

                // Fetch sale data
                sql = "SELECT * FROM " + vTablaTratar.trim() + " WHERE id_ = " + vIDVenta;
                pstmtVenta = connection.prepareStatement(sql);
                rsVenta = pstmtVenta.executeQuery();
                if (rsVenta.next()) {

                    if (vCantCompraTotal.compareTo(vCantVentaTotal) == 0) {
                        // Equal quantities, assign directly
                        vSplitCantImporte.setCompraCantidadAplicar(vCantCompraTotal);
                        vSplitCantImporte.setCompraImporteAplicar(vImporteCompraTotal);
                        vSplitCantImporte.setCompraCantidadResto(BigDecimal.ZERO);
                        vSplitCantImporte.setCompraImporteResto(BigDecimal.ZERO);

                        vSplitCantImporte.setVentaCantidadAplicar(vCantVentaTotal);
                        vSplitCantImporte.setVentaImporteAplicar(vImporteVentaTotal);
                        vSplitCantImporte.setVentaCantidadResto(BigDecimal.ZERO);
                        vSplitCantImporte.setVentaImporteResto(BigDecimal.ZERO);

                    } else if (vCantCompraTotal.compareTo(vCantVentaTotal) > 0) {
                        // Purchase quantity greater, apply to purchase and calculate remaining
                        vSplitCantImporte.setCompraCantidadAplicar(vCantVentaTotal);
                        vSplitCantImporte.setCompraCantidadResto(vCantCompraTotal.subtract(vCantVentaTotal));

                        //BigDecimal aaa =vSplitCantImporte.getCompraCantidadAplicar();
                        //BigDecimal bbb =vCantCompraTotal;
                        //BigDecimal ccc =aaa.divide(bbb,6, BigDecimal.ROUND_HALF_UP);
                        //vSplitCantImporte.setCompraImporteAplicar(vImporteCompraTotal.multiply(ccc));
                        vSplitCantImporte.setCompraImporteAplicar(vImporteCompraTotal.multiply(vSplitCantImporte.getCompraCantidadAplicar().divide(vCantCompraTotal,8, BigDecimal.ROUND_HALF_UP)));
                        vSplitCantImporte.setCompraImporteResto(vImporteCompraTotal.subtract(vSplitCantImporte.getCompraImporteAplicar()));

                        vSplitCantImporte.setVentaCantidadAplicar(vCantVentaTotal);
                        vSplitCantImporte.setVentaImporteAplicar(vImporteVentaTotal);
                        vSplitCantImporte.setVentaCantidadResto(BigDecimal.ZERO);
                        vSplitCantImporte.setVentaImporteResto(BigDecimal.ZERO);

                    } else {// vCantCompraTotal < vCantVentaTotal Then
                        // Sale quantity greater, apply to sale and calculate remaining
                        vSplitCantImporte.setVentaCantidadAplicar(vCantCompraTotal);
                        vSplitCantImporte.setVentaCantidadResto(vCantVentaTotal.subtract(vCantCompraTotal));
                        vSplitCantImporte.setVentaImporteAplicar(vImporteVentaTotal.multiply(vSplitCantImporte.getVentaCantidadAplicar().divide(vCantVentaTotal,8, BigDecimal.ROUND_HALF_UP)));
                        vSplitCantImporte.setVentaImporteResto(vImporteVentaTotal.subtract(vSplitCantImporte.getVentaImporteAplicar()));

                        vSplitCantImporte.setCompraCantidadAplicar(vCantCompraTotal);
                        vSplitCantImporte.setCompraImporteAplicar(vImporteCompraTotal);
                        vSplitCantImporte.setCompraCantidadResto(BigDecimal.ZERO);
                        vSplitCantImporte.setCompraImporteResto(BigDecimal.ZERO);
                    }

                    // Insert remaining purchase if applicable
                    if (vSplitCantImporte.getCompraCantidadResto().compareTo(BigDecimal.ZERO) > 0) {
                        sql = buildInsertSql(connection, vTablaTratar, fechaTope, rsCompra, vSplitCantImporte, "C", vNuevoIndiceBloqueoSplit);
                        pstmtInsertCompra = connection.prepareStatement(sql);
                        pstmtInsertCompra.executeUpdate();
                    }

                    // Insert remaining sale if applicable
                    if (vSplitCantImporte.getVentaCantidadResto().compareTo(BigDecimal.ZERO) > 0) {
                        sql = buildInsertSql(connection, vTablaTratar, fechaTope, rsVenta, vSplitCantImporte, "V", vNuevoIndiceBloqueoSplit);
                        pstmtInsertVenta = connection.prepareStatement(sql);
                        pstmtInsertVenta.executeUpdate();
                    }

                    // Update purchase with applied quantities
                    Integer vIdCompraVenta = fgl_Contador_IdCompraVenta(connection, vCompraVenta, vTablaTratar, fechaTope);

                    sql = buildUpdateSql(vCompraVenta, vTablaTratar, rsCompra, vSplitCantImporte, vIdCompraVenta,"C");
                    pstmtUpdateCompra = connection.prepareStatement(sql);
                    pstmtUpdateCompra.executeUpdate();

                    // Update sale with applied quantities
                    sql = buildUpdateSql(vCompraVenta, vTablaTratar, rsVenta, vSplitCantImporte, vIdCompraVenta,"V");
                    pstmtUpdateVenta = connection.prepareStatement(sql);
                    pstmtUpdateVenta.executeUpdate();

                }
            }
        } finally {
            // Close result sets and prepared statements
            try {
                closeResources(rsCompra, rsVenta, pstmtCompra, pstmtVenta, pstmtInsertCompra, pstmtInsertVenta, pstmtUpdateCompra, pstmtUpdateVenta);
            } catch (SQLException e) {
                // Handle closing error
            }
        }

        return vSplitCantImporte;
    }

// Helper methods for building SQL statements and handling resources

    private String buildInsertSql(Connection connection, String vTablaTratar, LocalDate fechaTope,
                                  ResultSet rsCV, Struc_SplitCantImporte vSplitCantImporte, String CV, Integer vNuevoIndiceBloqueoSplit) throws SQLException {

        StringBuilder sql = new StringBuilder("INSERT INTO " + vTablaTratar.trim() + " (");

        // Add common columns:
        sql.append("fechatope, fecha, tipo, detalles, importe, unidades, tipoactivo, declaracion, idcompraventa, indicebloqueo) VALUES (");

        // Add values based on CV and data:

        //String aaa = rsCV.getString("fecha");
        String aaa = "2022-05-04 00:00:00";
        LocalDate bbb = LocalDate.parse(aaa.substring(0,10));

        sql.append("'" + fechaTope.format(DateTimeFormatter.ISO_LOCAL_DATE) + "', ");
        sql.append("'" + LocalDate.parse(rsCV.getString("fecha").substring(0,10)).format(DateTimeFormatter.ISO_LOCAL_DATE) + "', ");
        sql.append("'" + rsCV.getString("tipo").trim() + "', ");
        sql.append("'" + rsCV.getString("detalles").trim() + "', ");

        // Add values based on CV ("C" or "V"):
        if (CV.trim().equals("C")) {
            sql.append(vSplitCantImporte.getCompraImporteResto() + ", ");
            sql.append(vSplitCantImporte.getCompraCantidadResto() + ", ");
        } else if (CV.trim().equals("V")) {
            sql.append(vSplitCantImporte.getVentaImporteResto() + ", ");
            sql.append(vSplitCantImporte.getVentaCantidadResto() + ", ");
        }

        // Add remaining values:
        sql.append("'" + rsCV.getString("tipoactivo").trim() + "', ");
        if (rsCV.getString("declaracion") != null) {
            sql.append("'" + rsCV.getString("declaracion").trim() + "', ");
        } else {
            sql.append("null, ");
        }
        if (rsCV.getString("idcompraventa") != null && rsCV.getInt("idcompraventa") != 0) {
            //if (rsCV.getString("idcompraventa") != null) {
            sql.append(rsCV.getInt("idcompraventa") + ", ");
        } else {
            sql.append("null, ");
        }
        if (vNuevoIndiceBloqueoSplit > 0) {
            sql.append(vNuevoIndiceBloqueoSplit);
        } else {
            sql.append("null");
        }
        sql.append(")");

        // Prepare and execute the INSERT statement:
        //PreparedStatement pstmt = connection.prepareStatement(sql.toString());
        //pstmt.executeUpdate();
        //pstmt.close(); // Assuming you handle resource closing elsewhere

        return sql.toString(); // Optional, return the built SQL string if needed
    }

    public Integer fgl_Contador_IdCompraVenta(Connection connection, String vcompraventa, String vTablaTratar, LocalDate vfechaTope) throws SQLException {

        Integer vIdCompraVenta = 0;

        if ("V".equals(vcompraventa.trim())) {
            String sql = "SELECT MAX(IdCompraVenta) AS indice FROM " + vTablaTratar.trim() +
                    " WHERE fechatope = ?";

            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setDate(1, Date.valueOf(vfechaTope));
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        // Check for both value and `NULL`
                        if (rs.getObject("indice") != null) {
                            vIdCompraVenta = rs.getInt("indice");
                        }
                    }
                }
            }
            vIdCompraVenta++;
        }

        return vIdCompraVenta;
    }

    private String buildUpdateSql(String vCompraVenta, String vTablaTratar, ResultSet rs, Struc_SplitCantImporte vSplitCantImporte, Integer vIdCompraVenta,String vUpdCV) throws SQLException {

        StringBuilder sql = new StringBuilder("UPDATE " + vTablaTratar.trim() + " SET ");

if (vUpdCV.trim().equals("C")) {
    sql.append("importe = " + vSplitCantImporte.getCompraImporteAplicar() + ", ");
    sql.append("unidades = " + vSplitCantImporte.getCompraCantidadAplicar());
}else{
    sql.append("importe = " + vSplitCantImporte.getVentaImporteAplicar() + ", ");
    sql.append("unidades = " + vSplitCantImporte.getVentaCantidadAplicar());
}
        // Add idcompraventa if vCompraVenta is "V":
        if ("V".equals(vCompraVenta.trim())) {
            sql.append(", idcompraventa = " + vIdCompraVenta);
        }

        // Add where clause based on id_:
        sql.append(" WHERE id_ = " + rs.getInt("id_"));

        return sql.toString();
    }

    public void closeResources(ResultSet rsCompra, ResultSet rsVenta, PreparedStatement pstmtCompra, PreparedStatement pstmtVenta,
                               PreparedStatement pstmtInsertCompra, PreparedStatement pstmtInsertVenta, PreparedStatement pstmtUpdateCompra, PreparedStatement pstmtUpdateVenta) throws SQLException {

        // Close result sets first (if not null)
        if (rsCompra != null) {
            rsCompra.close();
        }
        if (rsVenta != null) {
            rsVenta.close();
        }

        // Close prepared statements (in reverse order of creation)
        if (pstmtUpdateVenta != null) {
            pstmtUpdateVenta.close();
        }
        if (pstmtUpdateCompra != null) {
            pstmtUpdateCompra.close();
        }
        if (pstmtInsertVenta != null) {
            pstmtInsertVenta.close();
        }
        if (pstmtInsertCompra != null) {
            pstmtInsertCompra.close();
        }
        if (pstmtVenta != null) {
            pstmtVenta.close();
        }
        if (pstmtCompra != null) {
            pstmtCompra.close();
        }
    }

    public Integer fgl_Contador_IndiceBloqueo(Connection connection, String vTablaTratar, LocalDate vFechaTope) throws SQLException {

        int indiceBloqueo = 0;
        String sql = "SELECT MAX(indiceBloqueo) AS indice FROM " + vTablaTratar.trim() +
                " WHERE fechatope = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setDate(1, Date.valueOf(vFechaTope));
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                // Check for both value and null
                if (rs.getObject("indice") != null) {
                    indiceBloqueo = rs.getInt("indice");
                }
            }
            rs.close();
        }

        indiceBloqueo++;
        return indiceBloqueo;
    }
}
