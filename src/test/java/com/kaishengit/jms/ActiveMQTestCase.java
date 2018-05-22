package com.kaishengit.jms;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import javax.jms.*;
import java.io.IOException;

public class ActiveMQTestCase {

    @Test
    public void sendMessageToQueue() throws JMSException {
        //创建ConnectionFactory
        String brokerUrl = "tcp://localhost:61616";
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        //创建Connection
        Connection connection = connectionFactory.createConnection();
        //开始连接
        connection.start();
        //创建session
        //客户端手动签收
        //Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        //创建destination
        Destination destination = session.createQueue("queue");
        //创建消息生产者
        MessageProducer messageProducer = session.createProducer(destination);
        //设置持久化模式为不持久化
        messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        //消息优先级
        for(int i = 4;i<=9;i++) {
            //创建消息
            TextMessage textMessage = session.createTextMessage("Hello, MQ" + i);
            //发送消息
            //messageProducer.send(textMessage);
            messageProducer.send(textMessage, DeliveryMode.PERSISTENT, i, 0);
        }
        //释放资源
        messageProducer.close();
        //手动提交或回滚事务
        //session.commit();
        //session.rollback();
        session.close();
        connection.close();
    }

    @Test
    public void consumerMessageFromQueue() throws JMSException, IOException {
        //创建connectionFactory
        String brokerUrl = "tcp://localhost:61616";
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        //创建连接
        Connection connection = connectionFactory.createConnection();
        //开启连接
        connection.start();
        //创建回话
        //客户端手动签收
        //Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        //目的地
        Destination destination = session.createQueue("queue");
        //创建消费者
        MessageConsumer messageConsumer = session.createConsumer(destination);
        //消费消息,如果这个消息队列里面有新的消息则会执行onMessage方法
        messageConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                TextMessage textMessage = (TextMessage) message;
                try {
                    //客户端手动签收
                    //textMessage.acknowledge();
                    System.out.println(textMessage.getText());

                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
        System.in.read();
        //释放资源
        messageConsumer.close();
        session.close();
        connection.close();
    }

    /**
     * 触发重试机制 rollback
     * @throws JMSException
     * @throws IOException
     */
    @Test
    public void consumerMessageFromQueue1() throws JMSException, IOException {
        //创建connectionFactory
        String brokerUrl = "tcp://localhost:61616";
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        //创建连接
        Connection connection = connectionFactory.createConnection();
        //开启连接
        connection.start();
        //创建回话
        //客户端手动签收
        //Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        //目的地
        Destination destination = session.createQueue("queue");
        //创建消费者
        MessageConsumer messageConsumer = session.createConsumer(destination);
        //消费消息,如果这个消息队列里面有新的消息则会执行onMessage方法
        messageConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                TextMessage textMessage = (TextMessage) message;
                try {
                    String text = textMessage.getText();
                    if("Hello, MQ8".equals(text)) {
                        throw new JMSException("故意抛出的异常");
                    }
                    //客户端手动签收
                    //textMessage.acknowledge();
                    System.out.println(textMessage.getText());
                    session.commit();
                } catch (JMSException e) {
                    e.printStackTrace();
                    try {
                        session.rollback();
                    } catch (JMSException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        });
        System.in.read();
        //释放资源
        messageConsumer.close();
        session.close();
        connection.close();
    }

    /**
     * 触发重试机制 recover
     * @throws JMSException
     * @throws IOException
     */
    @Test
    public void consumerMessageFromQueue2() throws JMSException, IOException {
        //创建connectionFactory
        String brokerUrl = "tcp://localhost:61616";
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        //创建连接
        Connection connection = connectionFactory.createConnection();
        //开启连接
        connection.start();
        //创建回话
        //客户端手动签收
        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        //目的地
        Destination destination = session.createQueue("queue");
        //创建消费者
        MessageConsumer messageConsumer = session.createConsumer(destination);
        //消费消息,如果这个消息队列里面有新的消息则会执行onMessage方法
        messageConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                TextMessage textMessage = (TextMessage) message;
                try {
                    String text = textMessage.getText();
                    if("Hello, MQ8".equals(text)) {
                        throw new JMSException("故意抛出的异常");
                    }
                    //客户端手动签收
                    //textMessage.acknowledge();
                    System.out.println(textMessage.getText());
                    textMessage.acknowledge();
                } catch (JMSException e) {
                    e.printStackTrace();
                    try {
                        session.recover();
                    } catch (JMSException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        });
        System.in.read();
        //释放资源
        messageConsumer.close();
        session.close();
        connection.close();
    }

    /**
     * 触发重试机制 no catch
     * @throws JMSException
     * @throws IOException
     */
    @Test
    public void consumerMessageFromQueue3() throws JMSException, IOException {
        //创建connectionFactory
        String brokerUrl = "tcp://localhost:61616";
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        //创建连接
        Connection connection = connectionFactory.createConnection();
        //开启连接
        connection.start();
        //创建回话
        //客户端手动签收
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        //目的地
        Destination destination = session.createQueue("queue");
        //创建消费者
        MessageConsumer messageConsumer = session.createConsumer(destination);
        //消费消息,如果这个消息队列里面有新的消息则会执行onMessage方法
        messageConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                TextMessage textMessage = (TextMessage) message;
                try {
                    String text = textMessage.getText();
                    if("Hello, MQ8".equals(text)) {
                        throw new JMSException("故意抛出的异常");
                    }
                    System.out.println(textMessage.getText());
                } catch (JMSException e) {
                    throw new RuntimeException("运行时异常");
                }
            }
        });
        System.in.read();
        //释放资源
        messageConsumer.close();
        session.close();
        connection.close();
    }
}
