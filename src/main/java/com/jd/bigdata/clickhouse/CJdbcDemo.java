package com.jd.bigdata.clickhouse;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/**
 * @author FM
 * @Description
 * @create 2022-01-05 22:00
 */
public class CJdbcDemo {

    public static void main(String[] args) throws Exception{
        String jdbcUrl = "jdbc:clickhouse://hadoop144:8123,hadoop145:8123,hadoop146:8123/default";
        ClickHouseConnection connection = new BalancedClickhouseDataSource(jdbcUrl).getConnection();
        Statement statement = connection.createStatement( );
        ResultSet resultSet = statement.executeQuery("select * from default.t");
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
