package org.example.JDBC;

import java.io.*;

import java.sql.*;

import java.util.Properties;

public class MySQL {
    private Connection conn = null;
    private Statement testStatement = null;
    private ResultSet result = null;

    public MySQL(){
        Properties props = new Properties();

        try {
            // 从文件中读取配置信息
            FileInputStream fis = new FileInputStream("config.properties");
            props.load(fis);
            fis.close();

            // 获取属性值
            String ip = props.getProperty("ip");
            String port = props.getProperty("port");
            String userName = props.getProperty("userName");
            String password = props.getProperty("password");
            String databaseName = props.getProperty("databaseName");


            // 输出属性值
            System.out.println("ip: " + ip);
            System.out.println("port: " + port);
            System.out.println("userName: " + userName);
            System.out.println("password: " + password);
            System.out.println("database: " + databaseName);

            // connect mysql
            String url = "jdbc:mysql://" + ip + ":" + port + "/" + databaseName + "?useSSL=false&serverTimezone=UTC";
            Class.forName("com.mysql.cj.jdbc.Driver");
            //获取数据库连接
            conn = DriverManager.getConnection(url,userName,password);


        } catch (IOException | SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }


    }

    public String Execute(String sql){
        ByteArrayOutputStream TrueStream = new ByteArrayOutputStream();
        PrintStream MyStream = new PrintStream(TrueStream);


        try{
            testStatement = conn.createStatement();
            boolean hasResult = testStatement.execute(sql);

            if(hasResult)
            {
                //获取结果集
                result = testStatement.getResultSet();
                //ResultSetMetaData是用于分析结果集的元数据接口
                ResultSetMetaData resultMeta = result.getMetaData();

                int columnCount = resultMeta.getColumnCount();

                // 将获取到的字段用循环输出，注意：(下标是从1开始)
                for (int i = 1; i <= columnCount; i++) {
                    MyStream.printf("%-25s\t",resultMeta.getColumnName(i));
                }

                MyStream.printf("\n");

                //迭代输出ResultSet对象
                while(result.next())
                {
                    //依次输出每列的值
                    for(int i=0;i<columnCount;i++)
                    {
                        MyStream.printf("%-25s\t",result.getString(i+1));
                    }
                    MyStream.printf("\n");
                }

            }
            else
            {
                System.out.println("该SQL语句影响的记录有"+testStatement.getUpdateCount()+"条");
            }
        } catch (SQLException e) {
            return e.getMessage();
        }

        try {
            if (result != null)
            {
                result.close();
            }
            if (testStatement != null)
            {
                testStatement.close();
            }
        } catch (SQLException e) {
            return e.getMessage();
        }

        return TrueStream.toString();
    }

    public void close(){
        try {
            if (conn != null)
            {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
