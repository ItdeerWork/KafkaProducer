package cn.itdeer.utils;

import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

import static cn.itdeer.utils.Constants.format;

/**
 * Directions: 生产者
 * PackageName: cn.itdeer.utils.
 * ProjectName: KafkaProducer.
 * Creator: itdeer.
 * CreationTime: 2018/10/23 9:56.
 */

@Log4j
public class FullCodeProducer extends Thread{

    private final String topic_name;
    private final KafkaProducer<String, String> producer;
    private Integer loop_num = Constants.LOOP_NUMS;
    private Integer loop_data_size;


    /**
     * 构造函数，初始化属性配置
     */
    public FullCodeProducer(String topic_name) {
        Properties props = new Properties();
        props.put(Constants.BOOTSTRAP_SERVERS,Constants.BOOTSTRAP_SERVERS_VALUE);
        props.put(Constants.ACKS,Constants.ACKS_VALUE);
        props.put(Constants.RETRIES,Constants.RETRIES_VALUE);
        props.put(Constants.LINGER_MS,Constants.LINGER_MS_VALUE);
        props.put(Constants.BATCH_SIZE,Constants.BATCH_SIZE_VALUE);
        props.put(Constants.BUFFER_MEMORY,Constants.BUFFER_MEMORY_VALUE);
        props.put(Constants.MAX_REQUEST_SIZE,Constants.MAX_REQUEST_SIZE_VALUE);
        props.put(Constants.COMPRESSION_TYPE,Constants.COMPRESSION_TYPE_VALUE);
        props.put(Constants.REQUEST_TIMEOUT_MS,Constants.REQUEST_TIMEOUT_MS_VALUE);
        props.put(Constants.KEY_SERIALIZER_CLASS,Constants.KEY_SERIALIZER_CLASS_STRING);
        props.put(Constants.VALUE_SERIALIZER_CLASS,Constants.VALUE_SERIALIZER_CLASS_STRING);
        props.put(Constants.MAX_IN_FLIGHT_REQUESTS_PER,Constants.MAX_IN_FLIGHT_REQUESTS_PER_VALUE);

        this.topic_name = topic_name;
        producer = new KafkaProducer(props);
    }

    /**
     * 覆盖父类的run方法
     */
    @Override
    public void run() {
        try {
            long startTime = System.currentTimeMillis();
            String startDate = format.format(new Date());

            while (loop_num > 0){
                loop_num--;
                loop_data_size = Constants.LOOP_DATA_NUMS;
                while (loop_data_size > 0){
                    loop_data_size--;
                    String message = "{\"Value\":" + getRandomValue() + ",\"IsGood\":true,\"TimeStamp\":\""+ format.format(new Date()) +"\",\"SendTimeStamp\":\""+ format.format(new Date()) +"\",\"FullCode\":\"2121.C10HAH51CT101A\",\"OriginalCompanyCode\":\"2121\"}";

                    producer.send(new ProducerRecord(topic_name,message));
                }
            }

            long endTime = System.currentTimeMillis();
            String endDate = format.format(new Date());

            Print.outPrint(startTime,endTime,startDate,endDate,Thread.currentThread().getName());
        }finally {
            if(producer != null){
                producer.close();
            }
        }
    }

    /**
     * 随机产生一个0--100的一个数字
     * @return
     */
    private static int getRandomValue(){
        return (int)(Math.random() * 1000);
    }


    public static void main(String[] args) {

        String data = "{\"Value\":" + getRandomValue() + ",\"IsGood\":true,\"TimeStamp\":\""+ format.format(new Date()) +"\",\"SendTimeStamp\":\""+ format.format(new Date()) +"\",\"FullCode\":\"2121.C10HAH51CT101A\",\"OriginalCompanyCode\":\"2121\"}";

        System.out.println(data);
    }
}
