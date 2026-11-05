package com.djt.servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TestServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        this.doPost(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // 设置响应编码，防止网页乱码
        response.setContentType("text/html;charset=utf-8");
        PrintWriter pw = response.getWriter();

        pw.write("<html><head><title>Sogou Analysis</title></head><body>");
        pw.write("<h2>实时热搜词 Top 10 (来自 MySQL)</h2>");
        pw.write("<table border='1' style='text-align:center;'><tr><th>关键词</th><th>搜索次数</th></tr>");

        // 连接 MySQL（注意替换成你的真实 IP 和密码）
        String url = "jdbc:mysql://192.168.2.130:3306/test?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&characterEncoding=utf8";
        String user = "hive";
        String password = "hive";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);
            String sql = "SELECT name, count FROM newscount ORDER BY count DESC LIMIT 10";
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                String name = rs.getString("name");
                int count = rs.getInt("count");
                pw.write("<tr><td>" + name + "</td><td>" + count + "</td></tr>");
            }

            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            pw.write("<tr><td colspan='2'>数据库查询异常: " + e.getMessage() + "</td></tr>");
        }

        pw.write("</table>");
        pw.write("</body></html>");

        pw.flush();
        pw.close();
    }
}