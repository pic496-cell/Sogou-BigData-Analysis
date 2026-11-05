package com.djt.servlet;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.djt.JDBCHelper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;

public class NewsSvlt extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // 设置响应类型为 JSON，并处理中文乱码
        resp.setContentType("application/json;charset=utf-8");
        PrintWriter out = resp.getWriter();

        // 创建一个 JSON 数组用于存放查询结果
        JSONArray jsonArray = new JSONArray();
        try {
            // 查数据库（查前 10 名）
            String sql = "SELECT name, count FROM newscount ORDER BY count DESC LIMIT 10";
            ResultSet rs = JDBCHelper.executeQuery(sql);
            while (rs.next()) {
                JSONObject json = new JSONObject();
                json.put("name", rs.getString("name"));
                json.put("count", rs.getInt("count"));
                jsonArray.add(json);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 把 JSON 数组转成字符串发给前端
        out.print(jsonArray.toJSONString());
        out.flush();
        out.close();
    }
}