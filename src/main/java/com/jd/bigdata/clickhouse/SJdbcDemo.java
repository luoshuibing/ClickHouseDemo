package com.jd.bigdata.clickhouse;

import java.sql.*;

/**
 * @author FM
 * @Description
 * @create 2022-01-05 21:51
 */
public class SJdbcDemo {

    private static Connection connection= null;
    static {
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            String url = "jdbc:clickhouse://hadoop144:8123/default";
            String user = "default";
            String password = "";
            connection = DriverManager.getConnection(url,user,password);
        } catch (Exception e) {
            e.printStackTrace( );
        }
    }

    public static void main(String[] args) throws SQLException {
        Statement statement = connection.createStatement( );
        ResultSet resultSet = statement.executeQuery("select * from default.dis_table");
        ResultSetMetaData metaData = resultSet.getMetaData( );
        int columnCount = metaData.getColumnCount( );
        while (resultSet.next()){
            for (int i = 1; i <= columnCount; i++) {
                System.out.println(metaData.getColumnName(i)+":"+resultSet.getString(i));
            }
        }
        resultSet.close();
        statement.close();
        connection.close();
    }

}
