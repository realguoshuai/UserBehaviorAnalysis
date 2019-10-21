# UserBehaviorAnalysis
使用Flink实现用户行为分析 包含csv格式数据源 基于flink-1.7实现
包含三个模块功能:
基于用户行为日志:分析最近5分钟热门商品 30s更新一次
基于服务器日志:分析最近一分钟访问最频繁的url topN 5秒更新一次
基于用户登录日志:实现登录风控,预警


2019-10-17 16:16:24 新增 读取socket(模拟kafka输入)

只提供调试好的源码,详细步骤可以参考这个博主博客:https://www.cnblogs.com/cerofang/p/11327036.html
