package com.bjsxt.testudf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.sql.*;

public class TestUDF extends UDF {
    public Text evaluate(String str) throws SQLException, ClassNotFoundException {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        conn = DriverManager.getConnection("jdbc:hive2://192.168.179.13:10000/default");

//        pstmt = conn.prepareStatement(" create table hive_test(id int, name string) row format delimited fields terminated by '\t' ");
//        pstmt.execute();

//        pstmt = conn.prepareStatement(" select * from hive_test ");
        pstmt = conn.prepareStatement(" " +
                "SELECT " +
                    "CASE WHEN NVL(ID,1000) = 1 THEN 'XXX'ELSE ID END ID,NAME " +
                "FROM HIVE_TEST"
        );
        rs = pstmt.executeQuery();
        String sss = "";
        while (rs.next()) {
            sss += rs.getString(1) +"\t" + rs.getString(2)+",";
        }
        System.out.println("111");


        int i = str.length()+10;
//        return new Text(i+"");
        return new Text(sss);

    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        TestUDF udf = new TestUDF();
        Text end = udf.evaluate("hello");
        System.out.println(end.toString());
    }


}
