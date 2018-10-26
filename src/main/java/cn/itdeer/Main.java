package cn.itdeer;

import cn.itdeer.utils.Constants;
import cn.itdeer.utils.Producer;

/**
 * Directions: 程序的入口
 * PackageName: cn.itdeer.
 * ProjectName: KafkaProducer.
 * Creator: itdeer.
 * CreationTime: 2018/10/23 9:33.
 */
public class Main {

    public static void main(String[] args) {

        /**
         * 参数1：需要启动的线程数
         * 参数2：每个线程生产数据的循环次数
         * 参数3：每次循环产生的数据条数
         * 参数4：批处理的大小（k）
         * 参数5：消息体的长度大小（字符）
         * 参数6：主题名称
         *
         * 例子：java -jar KafkaProducer.jar 6 1000 10000 100 150 kafka_producer_demo
         */
        if(args.length > 0) {
            Constants.THREAD_NUMS = Integer.parseInt(args[0]);
            Constants.LOOP_NUMS = Integer.parseInt(args[1]);
            Constants.LOOP_DATA_NUMS = Integer.parseInt(args[2]);
            Constants.BATCH_SIZE_VALUE = Integer.parseInt(args[3]) * 1024;
            Constants.MESSAGE_SIZE_NUMS = Integer.parseInt(args[4]);
            Constants.TOPIC_NAME = args[5];
        }

        /**
         * 启动线程开始执行
         */
        for (int i = 0; i < Constants.THREAD_NUMS; i++) {
            Thread thread = new Producer(Constants.TOPIC_NAME);
            thread.start();
        }

    }
}
