/*
    Name: Stefan Leithenmayr
    Projekt: Gebrauchtwagenplatzverwaltung
 */

package at.htl.usedcars;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UsedCarsDealerTest {

    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db"; //;create=true";
    public static final String USER = "app", PASSWORD = "app";
    public static Connection conn;

    @BeforeClass
    public static void initJdbc() {
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank nicht möglich\n"
                    + e.getMessage() + "\n");
            System.exit(1);
        }

        //Erstellen der Tabelle VEHICLE & CUSTOMERS
        try {
            PreparedStatement pStmt = null;

            String sql = "CREATE TABLE vehicle(" +
                    " id INT CONSTRAINT vehicle_pk PRIMARY KEY" +
                    " GENERATED ALWAYS AS IDENTITY(START WITH 1, INCREMENT BY 1)," +
                    " brand VARCHAR(255) NOT NULL," +
                    " type VARCHAR(255) NOT NULL," +
                    " price INT," +
                    " drivenkm INT," +
                    " horsepower INT," +
                    " productionYear INT," +
                    " color VARCHAR(255))";

            pStmt = conn.prepareStatement(sql);
            pStmt.execute();

            sql = "CREATE TABLE customer(" +
                    " id INT CONSTRAINT customer_pk PRIMARY KEY" +
                    " GENERATED ALWAYS AS IDENTITY(START WITH 1, INCREMENT BY 1)," +
                    " name VARCHAR(255)," +
                    " phonenumber VARCHAR(255)," +
                    " email VARCHAR(255)," +
                    " address VARCHAR(255))";

            pStmt = conn.prepareStatement(sql);
            pStmt.execute();

        } catch (SQLException e) {
            System.out.println("Tabelle VEHICEL oder Customers konnte nicht erstellt werden.");
            System.err.println(e.getMessage());
        }
    }

    //region testMethods

    @Test
    public void t01_dml() {

        //Daten einfügen
        int countInserts = 0;
        countInserts += insertVehicle("Audi", "A4", 15000, 100000, 120, 2010, "black");
        countInserts += insertVehicle("BMW", "320d", 12000, 120000, 140, 2007, "white");
        countInserts += insertVehicle("Opel", "Astra", 4000, 110000, 101, 2001, "black");
        assertThat(countInserts, is(3));
        countInserts = 0;

        countInserts += insertCustomer("Stefan Leithenmayr", "0123 / 123456", "sleit@test.at", "testAddress 1, 1234 testtown");
        countInserts += insertCustomer("Fritz Mustermann", "0123 / 1234567", "fritzmustermann@test.at", "testAddress 2, 1234 testtown");
        countInserts += insertCustomer("Max Mustermann", "0123 / 12345678", "maxmustermann@test.at", "testAddress 3, 1234 testtown");
        assertThat(countInserts, is(3));

        //Daten abfragen
        try {
            PreparedStatement pStmt = conn.prepareStatement("SELECT brand, type, price FROM vehicle");
            ResultSet rs = pStmt.executeQuery();

            testVehicleRecord(rs,"Audi", "A4", 15000);
            testVehicleRecord(rs,"BMW", "320d", 12000);
            testVehicleRecord(rs,"Opel", "Astra", 4000);

            pStmt = conn.prepareStatement("SELECT name, email FROM customer");
            rs = pStmt.executeQuery();

            testCustomerRecord(rs,"Stefan Leithenmayr", "sleit@test.at");
            testCustomerRecord(rs,"Fritz Mustermann", "fritzmustermann@test.at");
            testCustomerRecord(rs,"Max Mustermann", "maxmustermann@test.at");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t02_checkCorrectTableNames() throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();

        ResultSet rs = dbMetaData.getTables(null, null, null, new String[]{"TABLE"});
        rs.next();
        assertThat(rs.getString("TABLE_NAME"), is("CUSTOMER"));
        rs.next();
        assertThat(rs.getString("TABLE_NAME"), is("VEHICLE"));
    }

    @Test
    public void t03_testVehicleTable() throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();

        //Primary Key Check
        ResultSet rs = dbMetaData.getPrimaryKeys(null,null, "VEHICLE");
        rs.next();
        assertThat(rs.getString("COLUMN_NAME").toUpperCase(), is("ID"));

        ResultSet columns = dbMetaData.getColumns(null,null, "VEHICLE", null);
        testColumn("ID", "4", "NO", "YES", columns); //Datentyp 4 steht für INT
        testColumn("BRAND", "12", "NO", "NO", columns); //Datentyp 12 steht für STRING
        testColumn("TYPE", "12", "NO", "NO", columns);
        testColumn("PRICE", "4", "YES", "NO", columns);
        testColumn("DRIVENKM", "4", "YES", "NO", columns);
        testColumn("HORSEPOWER", "4", "YES", "NO", columns);
        testColumn("PRODUCTIONYEAR", "4", "YES", "NO", columns);
        testColumn("COLOR", "12", "YES", "NO", columns);
    }

    @Test
    public void t04_testCustomerTable() throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();

        //Primary Key Check
        ResultSet rs = dbMetaData.getPrimaryKeys(null,null, "CUSTOMER");
        rs.next();
        assertThat(rs.getString("COLUMN_NAME").toUpperCase(), is("ID"));

        ResultSet columns = dbMetaData.getColumns(null,null, "CUSTOMER", null);
        testColumn("ID", "4", "NO", "YES", columns);
        testColumn("NAME", "12", "YES", "NO", columns);
        testColumn("PHONENUMBER", "12", "YES", "NO", columns);
        testColumn("EMAIL", "12", "YES", "NO", columns);
        testColumn("ADDRESS", "12", "YES", "NO", columns);
    }

    @AfterClass
    public static void teardownJdbc(){

        //Tabelle VEHICLE löschen
        try {
            conn.createStatement().execute("DROP TABLE vehicle");
            System.out.println("Tabelle VEHICLE gelöscht");
            conn.createStatement().execute("DROP TABLE customer");
            System.out.println("Tabelle CUSTOMER gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabelle VEHICLE oder CUSTOMER konnte nicht gelöscht werden:\n"
                    + e.getMessage());
        }

        // Connection schließen
        try {
            if (conn != null && !conn.isClosed()){
                conn.close();
                System.out.println("Good Bye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //endregion


    //region Helpmethods

    private int insertVehicle(String brand, String type, int price, int drivenkm, int horsepower, int productionYear, String color){

        String sql = "INSERT INTO vehicle(brand, type, price, drivenkm, horsepower, productionYear, color) " +
                "VALUES(?,?,?,?,?,?,?)";
        try {
            PreparedStatement pStmt = conn.prepareStatement(sql);

            pStmt.setString(1, brand);
            pStmt.setString(2, type);
            pStmt.setInt(3, price);
            pStmt.setInt(4, drivenkm);
            pStmt.setInt(5, horsepower);
            pStmt.setInt(6, productionYear);
            pStmt.setString(7, color);
            return pStmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return 0;
    }

    private int insertCustomer(String name, String phoneNumber, String email, String address){

        String sql = "INSERT INTO customer(name, phonenumber, email, address) " +
                "VALUES(?,?,?,?)";
        try {
            PreparedStatement pStmt = conn.prepareStatement(sql);
            pStmt.setString(1, name);
            pStmt.setString(2, phoneNumber);
            pStmt.setString(3, email);
            pStmt.setString(4, address);
            return pStmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return 0;
    }

    private void testColumn(String columnname, String data_type, String isNullable, String isAutoincremented, ResultSet columns) throws SQLException {
        columns.next();
        assertThat(columns.getString("COLUMN_NAME"), is(columnname));
        assertThat(columns.getString("DATA_TYPE"), is(data_type));
        assertThat(columns.getString("IS_NULLABLE"), is(isNullable));
        assertThat(columns.getString("IS_AUTOINCREMENT"), is(isAutoincremented));
    }

    private void testVehicleRecord(ResultSet rs, String brand, String type, int price) throws SQLException {
        rs.next();
        assertThat(rs.getString("BRAND"), is(brand));
        assertThat(rs.getString("TYPE"), is(type));
        assertThat(rs.getInt("PRICE"), is(price));
    }

    private void testCustomerRecord(ResultSet rs, String name, String email) throws SQLException {
        rs.next();
        assertThat(rs.getString("NAME"), is(name));
        assertThat(rs.getString("EMAIL"), is(email));
    }

    //endregion
}