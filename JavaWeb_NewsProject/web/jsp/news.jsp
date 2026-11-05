<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>大数据实时分析大屏 - 终端</title>
    <!-- 引入 jQuery 和 ECharts -->
    <script src="https://cdn.bootcdn.net/ajax/libs/jquery/3.2.1/jquery.min.js"></script>
    <script src="https://cdn.bootcdn.net/ajax/libs/echarts/5.4.2/echarts.min.js"></script>
    <style>
        /* 全局暗黑科技风 */
        body {
            margin: 0;
            padding: 0;
            background-color: #0b0f19; /* 深邃暗夜蓝 */
            color: #fff;
            font-family: 'Microsoft YaHei', sans-serif;
            overflow: hidden; /* 防止出现滚动条 */
        }

        /* 顶部标题栏 */
        .header {
            height: 60px;
            line-height: 60px;
            text-align: center;
            font-size: 28px;
            font-weight: bold;
            color: #00d2ff;
            text-shadow: 0 0 10px rgba(0, 210, 255, 0.8);
            background: url('https://img.alicdn.com/tfs/TB1Z0XqBXP7gK0jSZFjXXc5aXXa-1920-80.png') no-repeat center;
            background-size: cover;
            letter-spacing: 3px;
            margin-bottom: 10px;
        }

        /* 主体网格布局 */
        .main-container {
            display: flex;
            flex-wrap: wrap;
            padding: 0 10px;
            box-sizing: border-box;
            height: calc(100vh - 80px);
        }

        /* 各个图表的容器 */
        .chart-box {
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(0, 210, 255, 0.3);
            border-radius: 8px;
            box-shadow: 0 0 10px rgba(0, 210, 255, 0.1) inset;
            box-sizing: border-box;
            padding: 10px;
            margin-bottom: 10px;
        }

        .box-title {
            font-size: 16px;
            color: #00d2ff;
            font-weight: bold;
            padding-bottom: 10px;
            border-bottom: 1px solid rgba(0, 210, 255, 0.2);
            margin-bottom: 10px;
        }

        /* 左上：横向柱状图 */
        .top-left { width: 50%; height: 48%; }
        /* 右上：仪表盘 */
        .top-right { width: 50%; height: 48%; padding-left: 10px; }
        /* 下方：折线图 */
        .bottom { width: 100%; height: 48%; }

        /* 隐藏滚动条 */
        ::-webkit-scrollbar { display: none; }
    </style>
</head>
<body>

<!-- 顶部标题 -->
<div class="header">📊 搜狗用户行为分析大屏</div>

<!-- 图表区域 -->
<div class="main-container">
    <!-- 左上角 -->
    <div class="chart-box top-left" style="width: calc(50% - 5px);">
        <div class="box-title">热搜关键词 Top 10</div>
        <div id="barChart" style="width: 100%; height: calc(100% - 30px);"></div>
    </div>

    <!-- 右上角 -->
    <div class="chart-box top-right" style="width: calc(50% - 5px);">
        <div class="box-title">全网搜索总量统计 (仪表盘)</div>
        <div id="gaugeChart" style="width: 100%; height: calc(100% - 30px);"></div>
    </div>

    <!-- 正下方 -->
    <div class="chart-box bottom" style="width: 100%;">
        <div class="box-title">用户时段活跃度走势 (折线图)</div>
        <div id="lineChart" style="width: 100%; height: calc(100% - 30px);"></div>
    </div>
</div>

<script type="text/javascript">
    // 初始化三个 ECharts 实例
    var barChart = echarts.init(document.getElementById('barChart'));
    var gaugeChart = echarts.init(document.getElementById('gaugeChart'));
    var lineChart = echarts.init(document.getElementById('lineChart'));

    // 发送 Ajax 请求拿数据
    $.ajax({
        type: "GET",
        url: "/newwebb/news", // 注意路径，如果是独立 Tomcat 就是 /newwebb/news
        dataType: "json",
        success: function (data) {
            if (!data || data.length === 0) {
                return;
            }

            var names = [];
            var counts = [];
            var totalCount = 0;

            // 解析 JSON 数据
            for (var i = 0; i < data.length; i++) {
                names.push(data[i].name);
                counts.push(data[i].count);
                totalCount += data[i].count;
            }

            // ================= 1. 左上角横向柱状图 =================
            var barOption = {
                tooltip: { trigger: 'axis' },
                grid: { left: '3%', right: '8%', bottom: '3%', top: '5%', containLabel: true },
                xAxis: {
                    type: 'value',
                    axisLabel: { color: '#fff' },
                    splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } }
                },
                yAxis: {
                    type: 'category',
                    data: names.reverse(), // 反向排序，第一名在上面
                    axisLabel: { color: '#fff' },
                    axisLine: { lineStyle: { color: '#00d2ff' } }
                },
                series: [{
                    name: '搜索次数',
                    type: 'bar',
                    data: counts.reverse(),
                    itemStyle: {
                        // 给柱子加渐变色
                        color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                            { offset: 0, color: '#00d2ff' },
                            { offset: 1, color: '#3a7bd5' }
                        ])
                    },
                    barWidth: '50%'
                }]
            };
            barChart.setOption(barOption);

            // ================= 2. 右上角仪表盘 =================
            var gaugeOption = {
                tooltip: { formatter: '{a} <br/>{b} : {c}' },
                series: [{
                    name: '总搜索量',
                    type: 'gauge',
                    progress: { show: true, width: 18 },
                    axisLine: { lineStyle: { width: 18, color: [[1, 'rgba(0, 210, 255, 0.2)']] } },
                    axisTick: { show: false },
                    splitLine: { length: 15, lineStyle: { width: 2, color: '#fff' } },
                    axisLabel: { color: '#fff', distance: 25 },
                    pointer: { icon: 'path://M12.8,0.7l12,40.1H0.7L12.8,0.7z', width: 12, length: '60%', offsetCenter: [0, '-20%'], itemStyle: { color: '#00d2ff' } },
                    detail: {
                        valueAnimation: true,
                        formatter: '{value}',
                        color: '#00d2ff',
                        fontSize: 30,
                        offsetCenter: [0, '70%']
                    },
                    data: [{ value: totalCount, name: '总量' }]
                }]
            };
            gaugeChart.setOption(gaugeOption);

            // ================= 3. 下方折线图 (前端模拟时段数据) =================
            var timeData = ['00:00', '00:01', '00:02', '00:03', '00:04', '00:05', '00:06', '00:07', '00:08'];
            var timeCounts = [150, 230, 224, 218, 135, 147, 260, 300, 180]; // 模拟活跃度

            var lineOption = {
                tooltip: { trigger: 'axis' },
                grid: { left: '3%', right: '4%', bottom: '3%', top: '15%', containLabel: true },
                xAxis: {
                    type: 'category',
                    boundaryGap: false,
                    data: timeData,
                    axisLabel: { color: '#fff' },
                    axisLine: { lineStyle: { color: '#00d2ff' } }
                },
                yAxis: {
                    type: 'value',
                    axisLabel: { color: '#fff' },
                    splitLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } }
                },
                series: [{
                    name: '搜索量',
                    type: 'line',
                    smooth: true, // 平滑曲线
                    areaStyle: {
                        // 折线图下方的阴影渐变
                        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                            { offset: 0, color: 'rgba(0, 210, 255, 0.8)' },
                            { offset: 1, color: 'rgba(0, 210, 255, 0.1)' }
                        ])
                    },
                    itemStyle: { color: '#00d2ff' },
                    data: timeCounts
                }]
            };
            lineChart.setOption(lineOption);
        },
        error: function() {
            alert("请求数据失败，请检查后端或虚拟机 MySQL 是否启动！");
        }
    });

    // 窗口大小自适应
    window.addEventListener('resize', function() {
        barChart.resize();
        gaugeChart.resize();
        lineChart.resize();
    });
</script>
</body>
</html>