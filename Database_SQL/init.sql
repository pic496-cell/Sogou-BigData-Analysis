-- 创建业务数据库
CREATE DATABASE IF NOT EXISTS test DEFAULT CHARACTER SET utf8mb4;
USE test;

-- 热搜词统计表
CREATE TABLE IF NOT EXISTS newscount (
    name VARCHAR(255) PRIMARY KEY,
    count INT DEFAULT 0
);

-- 时段统计表
CREATE TABLE IF NOT EXISTS periodcount (
    logtime DATETIME PRIMARY KEY,
    count INT DEFAULT 0
);