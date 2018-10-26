package cn.itdeer.utils;

/**
 * Directions: 测试输出
 * PackageName: cn.itdeer.utils.
 * ProjectName: KafkaProducer.
 * Creator: itdeer.
 * CreationTime: 2018/10/24 14:54.
 */
public class Print {

    /**
     * 打印测试结果
     * @param startTime
     * @param endTime
     * @param startDate
     * @param endDate
     * @param threadName
     */
    public static void outPrint(long startTime,long endTime,String startDate,String endDate,String threadName){
        Integer totle_nums = Constants.LOOP_NUMS * Constants.LOOP_DATA_NUMS;
        double totle_time = (endTime - startTime) / 1000;
        double totle_size = totle_nums * Constants.MESSAGE_SIZE_NUMS / 1024 / 1024;
        double speed_size = totle_size / totle_time;
        double speed = totle_nums / totle_time;
        System.out.println("当前线程：" + threadName);
        System.out.printf("\n线程数 | " + "分区数 | " + "副本数 | " + "开始时间 | " + "结束时间 | " + "共用（秒） | " + "压缩 | " + "消息大小（字符） | " + "批大小（条） | " + "总数据大小（MB） | " + "总消息数量（条） | " + "吞吐量（MB/S） | " + "速度（条/秒） | "  + "\n" + "---|---|---|---|---|---|---|---|---|---|---|---|---" + "\n" + "%s | " + "%s | " + "%s | " + "%s | " + "%s | " + "%s | " + "%s | " + "%s | " + "%s | " + "%s | " + "%s | " + "%s | " + "%s \n" ,Constants.THREAD_NUMS,Constants.PARTITION_NUMS,Constants.REPLICATION_NUMS,startDate,endDate,totle_time,Constants.COMPRESSION_TYPE_VALUE,Constants.MESSAGE_SIZE_NUMS,Constants.BATCH_SIZE_VALUE,totle_size,totle_nums,speed_size,speed);
        System.out.println();
    }

}
