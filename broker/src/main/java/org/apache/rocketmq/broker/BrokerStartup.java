/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.broker.longpolling.PullRequestHoldService;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.processor.ConsumerManageProcessor;
import org.apache.rocketmq.broker.processor.EndTransactionProcessor;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.TransactionalMessageCheckService;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService;
import org.apache.rocketmq.client.impl.consumer.PullAPIWrapper;
import org.apache.rocketmq.client.impl.consumer.RebalanceImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexFile;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;

/**
 *
 *  "messageQueue":{
 * 		"brokerName":"zhengsj_1VYV1S2",
 * 		"queueId":0,
 * 		"topic":"TopicTest"
 *  }
 *   messageQueue 在client 段选取
 *
 *
 *
 *  result :{
 * 	"messageQueue":{
 * 		"brokerName":"zhengsj_1VYV1S2",
 * 		"queueId":3,        store\consumequeue\{topicName}\queueId   客户端发送给服务端
 * 		"topic":"TopicTest"
 *  },
 *
 *    {@link MQClientAPIImpl#processSendResponse(java.lang.String, org.apache.rocketmq.common.message.Message, org.apache.rocketmq.remoting.protocol.RemotingCommand)}
 * 	 "msgId":"A9FE3740000018B4AAC208E4B8050000",  客户端生成
 * 	 {@link CommitLog.DefaultAppendMessageCallback#doAppend(long, java.nio.ByteBuffer, int, org.apache.rocketmq.store.MessageExtBrokerInner)}
 * 	 "offsetMsgId":"0A00804F00002AC8000000000000B49C", 服务端生成
 * 	 "queueOffset":57,    queueOffset & 20 = 真正的偏移量
 * 	 "regionId":"DefaultRegion",
 * 	"sendStatus":"SEND_OK",
 * 	"traceOn":true
 * }
 *
 *
 *
 *  store
 *
 *  {@link ConsumeQueue#ConsumeQueue(java.lang.String, int, java.lang.String, int, org.apache.rocketmq.store.DefaultMessageStore)}
 *
 *  {@link CommitLog}
 *
 *  {@link IndexFile}
 *
 *
 *  处理发送消息
 * @see SendMessageProcessor
 *
 * {@link MessageStoreConfig}
 *
 *
 *   一条消息跨文件存储
 *   {@link AppendMessageStatus#END_OF_FILE}
 *
 *   如何知道该文件的偏移量 {@link DefaultMessageStore#recover(boolean)} {@link CommitLog#recoverNormally(long)}
 *
 *   刷盘 jvm之外内存
 *
 *   为什么要空闲八个字节 {@link CommitLog.DefaultAppendMessageCallback#END_FILE_MIN_BLANK_LENGTH}
 *
 *   所有系统 topic 意义 ？
 *
 *  处理发送结果
 * @see  SendMessageProcessor#handlePutMessageResult(org.apache.rocketmq.store.PutMessageResult, org.apache.rocketmq.remoting.protocol.RemotingCommand, org.apache.rocketmq.remoting.protocol.RemotingCommand, org.apache.rocketmq.common.message.MessageExt, org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader, org.apache.rocketmq.broker.mqtrace.SendMessageContext, io.netty.channel.ChannelHandlerContext, int)
 *
 *
 *    {@link MappedFileQueue#getMaxOffset()} 为什么
 *
 *   {@link DefaultMessageStore#doDispatch(org.apache.rocketmq.store.DispatchRequest)}
 *
 *
 *
 * {@link DefaultMessageStore.ReputMessageService}
 *
 * {@link DefaultMessageStore#putMessagePositionInfo(org.apache.rocketmq.store.DispatchRequest)}
 *
 *     {@link ConsumeQueue#putMessagePositionInfoWrapper(org.apache.rocketmq.store.DispatchRequest)}
 *
 *
 *  index
 *
 *  {@link DefaultMessageStore.CommitLogDispatcherBuildIndex}
 *
 *
 *
 *  {@link CommitLog#checkMessageAndReturnSize(java.nio.ByteBuffer, boolean, boolean)} find {@link DispatchRequest}
 *
 * 刷盘操作
 *
 *    /** 异步刷盘 && 开启内存字节缓冲区
 *   {@link CommitLog.CommitRealTimeService}
 *   /** 异步刷盘
 *
 *   {@link CommitLog.FlushRealTimeService}
 *
 *   /** 同步刷盘
 *   {@link CommitLog.GroupCommitService}
 *
 *   {@link CommitLog#handleDiskFlush(org.apache.rocketmq.store.AppendMessageResult, org.apache.rocketmq.store.PutMessageResult, org.apache.rocketmq.common.message.MessageExt)}
 *
 *
 *
 *  ConsumQueue
 *
 *  {@link PullMessageProcessor#processRequest(io.netty.channel.Channel, org.apache.rocketmq.remoting.protocol.RemotingCommand, boolean)}
 *
 *  {@link PullMessageProcessor#executeRequestWhenWakeup(io.netty.channel.Channel, org.apache.rocketmq.remoting.protocol.RemotingCommand)}
 *
 *
 *  consumerOffset.json 文件
 *   修改
 *  {@link ConsumerOffsetManager#commitOffset(java.lang.String, java.lang.String, java.lang.String, int, long)}
 *     {@link BrokerController#getQueryThreadPoolQueue()} 调度器
 *
 *
 *  延迟队列调度
 *  {@link ScheduleMessageService.DeliverDelayedMessageTimerTask}
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *  Consumer {@link PullMessageProcessor#processRequest(ChannelHandlerContext, RemotingCommand)}
 *
 *     {@link DefaultMessageStore#getMessage(java.lang.String, java.lang.String, int, long, int, org.apache.rocketmq.store.MessageFilter)}
 *
 *  {@link PullRequestHoldService} 每隔5s一次
 *
 *
 *
 *  消息队列负载 与 重新  分布机制
 *
 *   {@link RebalanceImpl#doRebalance(boolean)}
 *
 *   {@link AllocateMessageQueueStrategy} 队列分配算法接口
 *     分配规则和 {@link ClientConfig#setInstanceName(java.lang.String)} 有关系如果是一样的则两个都会分配一样的queueId
 *
 *
 *   消费过程
 *
 *   {@link ConsumeMessageConcurrentlyService#submitConsumeRequest(java.util.List, org.apache.rocketmq.client.impl.consumer.ProcessQueue, org.apache.rocketmq.common.message.MessageQueue, boolean)}
 *
 *  处理消费结果
 *  {@link ConsumeMessageConcurrentlyService#processConsumeResult(org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus, org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext, org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest)}
 *
 *  发送ack
 *  {@link ConsumeMessageConcurrentlyService#sendMessageBack(org.apache.rocketmq.common.message.MessageExt, org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext)}
 *
 *  服务端 接收
 *  {@link SendMessageProcessor#asyncConsumerSendMsgBack(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)}
 *
 *  消费进度管理
 *  {@link OffsetStore}
 *
 *  %RETRY%please_rename_unique_group_name
 *
 *  客户端发送
 *  {@link ConsumerManageProcessor#processRequest(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)}
 *
 *
 *  定时消息机制
 *
 *  {@link ScheduleMessageService}
 *
 *
 *  刷盘
 * {@link CommitLog#handleDiskFlush(org.apache.rocketmq.store.AppendMessageResult, org.apache.rocketmq.store.PutMessageResult, org.apache.rocketmq.common.message.MessageExt)}
 * {@link CommitLog.GroupCommitService}
 *
 *
 *  主从复制原理
 *  {@link CommitLog#submitReplicaRequest(org.apache.rocketmq.store.AppendMessageResult, org.apache.rocketmq.store.PutMessageResult, org.apache.rocketmq.common.message.MessageExt)}
 * {@link CommitLog#handleHA(org.apache.rocketmq.store.AppendMessageResult, org.apache.rocketmq.store.PutMessageResult, org.apache.rocketmq.common.message.MessageExt)}
 *    {@link HAService} # {@link HAService.AcceptSocketService} 监听客户端连接实现类
 *                      # {@link HAService.GroupTransferService} 主从同步通知实现类
 *                      # {@link HAService.HAClient} 客户端实现类
 *    {@link HAConnection} Ha master 服务端HA 连接对象的封装 与 broker 从服务器的网关读写实现类
 *                      # {@link HAConnection.ReadSocketService} 网络读实现类
 *                      # {@link HAConnection.WriteSocketService} 网络写实现类
 *
 *
 *    {@link HAService.GroupTransferService} 处理同步结果
 *
 *    slave 连接 master {@link HAService.HAClient#connectMaster()}
 *
 *    master 处理 slave 请求 {@link HAConnection}
 *
 *
 *  RocketMq 读写分离机制
 *  {@link MQClientInstance#findBrokerAddressInSubscribe(java.lang.String, long, boolean)}
 *
 *      根据消费队列获取 brokerId 实现
 *      {@link PullAPIWrapper#recalculatePullFromWhichNode(org.apache.rocketmq.common.message.MessageQueue)}
 *
 *
 * 事务消息
 *
 *  {@link EndTransactionProcessor#processRequest(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)}
 *
 *  回查事务
 *  {@link TransactionalMessageCheckService}
 *
 *  回查频率默认是1分钟一次 {@link BrokerConfig#transactionCheckMax} 单位毫秒
 *
 *  回查最大次数 {@link BrokerConfig#getTransactionCheckMax()}
 *
 *  {@link TransactionalMessageServiceImpl#check(long, int, org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener)}
 *
 *  操作过程
 *
 *   当返回是commit 时删除
 *
 *   当回滚
 *
 *   当返回未知
 *    {@link TransactionalMessageCheckService#onWaitEnd()} 系统会以一分钟的频率调用检查
 *      首先会获取 RMQ_SYS_TRANS_HALF_TOPIC 消费进度进行查询消息如果获取不到代表没有需要进行事务回调则跳过
 *        如果获取有则会获取 RMQ_SYS_TRANS_OP_HALF_TOPIC 消费进度获取 消费进度获取所有操作记录
 *            RMQ_SYS_TRANS_OP_HALF_TOPIC 对应的commitLog body 对应的是 RMQ_SYS_TRANS_HALF_TOPIC 偏移量 操作commit数据
 *
 *       调用chechek 的时候会再次发起 {@link TransactionListener#checkLocalTransaction(org.apache.rocketmq.common.message.MessageExt)}
 *       检测如果是 commit RMQ_SYS_TRANS_OP_HALF_TOPIC 偏移量会加1 ，如果未知的话 会再新增一个 RMQ_SYS_TRANS_HALF_TOPIC 消息 偏移量加1
 *          检查数{@link MessageConst#PROPERTY_TRANSACTION_CHECK_TIMES} 会加1
 *
 *
 *       判断检查数超过了则消息会被消费掉， RMQ_SYS_TRANS_HALF_TOPIC 然后偏移量+1
 *        7 代表下一次偏移量   6已被消费  {回滚或提交}
 *        26 下一次偏移量      25已被消费
 *        {
 * 	"offsetTable":{
 * 		"RMQ_SYS_TRANS_OP_HALF_TOPIC@CID_RMQ_SYS_TRANS":{0:7
 *                },
 * 		"RMQ_SYS_TRANS_HALF_TOPIC@CID_RMQ_SYS_TRANS":{0:26
 *        }    * 	}
 * }
 *
 *        没新增一个事务或者检查一次 会新增 一条啊 RMQ_SYS_TRANS_HALF_TOPIC消息 检查会修改检查数
 *        提交或回滚会新增RMQ_SYS_TRANS_OP_HALF_TOPIC 消息 提交会新增一条原始消息插入到commitlog内
 *
 *    Rocketmq 采用的是顺序写 去修改内容无法保证高性能
 *
 *    发起回查
 *    {@link AbstractTransactionalMessageCheckListener#sendCheckMessage(org.apache.rocketmq.common.message.MessageExt)}
 *
 *    回查次数超限及时间过期的消息会存入到 TRANS_CHECK_MAX_TIME_TOPIC
 *
 *    {@link DefaultMessageStore#cleanFilesPeriodically()} 清除策略
 *
 */
public class BrokerStartup {

    public final static String NAME_SERVER_ADDRESS = "127.0.0.1:9878";
    public final static String HOME_PATH = "D:/workspace/home";
    public final static int PORT = 10952;



    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;
    public static InternalLogger log;

    public static void main(String[] args) {
        System.setProperty(MixAll.ROCKETMQ_HOME_PROPERTY,HOME_PATH);
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY,NAME_SERVER_ADDRESS);
        System.setProperty("user.home",HOME_PATH);
        start(createBrokerController(args));
    }

    public static BrokerController start(BrokerController controller) {
        try {

            controller.start();

            String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                + controller.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    public static BrokerController createBrokerController(String[] args) {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            NettySystemConfig.socketSndbufSize = 131072;
        }

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            NettySystemConfig.socketRcvbufSize = 131072;
        }

        try {
            //PackageConflictDetect.detectFastjson();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
                new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
            }

            final BrokerConfig brokerConfig = new BrokerConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();

            nettyClientConfig.setUseTLS(Boolean.parseBoolean(System.getProperty(TLS_ENABLE,
                String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING))));
            nettyServerConfig.setListenPort(PORT);
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }

            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    properties2SystemEnv(properties);
                    MixAll.properties2Object(properties, brokerConfig);
                    MixAll.properties2Object(properties, nettyServerConfig);
                    MixAll.properties2Object(properties, nettyClientConfig);
                    MixAll.properties2Object(properties, messageStoreConfig);

                    BrokerPathConfigHelper.setBrokerConfigPath(file);
                    in.close();
                }
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

            if (null == brokerConfig.getRocketmqHome()) {
                System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
                System.exit(-2);
            }

            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {
                try {
                    String[] addrArray = namesrvAddr.split(";");
                    for (String addr : addrArray) {
                        RemotingUtil.string2SocketAddress(addr);
                    }
                } catch (Exception e) {
                    System.out.printf(
                        "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                        namesrvAddr);
                    System.exit(-3);
                }
            }

            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= 0) {
                        System.out.printf("Slave's brokerId must be > 0");
                        System.exit(-3);
                    }

                    break;
                default:
                    break;
            }

            if (messageStoreConfig.isEnableDLegerCommitLog()) {
                brokerConfig.setBrokerId(-1);
            }

            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
//            configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");
            configurator.doConfigure(Objects.requireNonNull(BrokerStartup.class.getClassLoader().getResource("logback.xml")));

            if (commandLine.hasOption('p')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig);
                MixAll.printObjectProperties(console, nettyServerConfig);
                MixAll.printObjectProperties(console, nettyClientConfig);
                MixAll.printObjectProperties(console, messageStoreConfig);
                System.exit(0);
            } else if (commandLine.hasOption('m')) {
                InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, brokerConfig, true);
                MixAll.printObjectProperties(console, nettyServerConfig, true);
                MixAll.printObjectProperties(console, nettyClientConfig, true);
                MixAll.printObjectProperties(console, messageStoreConfig, true);
                System.exit(0);
            }

            log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
            MixAll.printObjectProperties(log, brokerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            MixAll.printObjectProperties(log, nettyClientConfig);
            MixAll.printObjectProperties(log, messageStoreConfig);

            final BrokerController controller = new BrokerController(
                brokerConfig,
                nettyServerConfig,
                nettyClientConfig,
                messageStoreConfig);
            // remember all configs to prevent discard
            controller.getConfiguration().registerConfig(properties);

            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);

                @Override
                public void run() {
                    synchronized (this) {
                        log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long beginTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
