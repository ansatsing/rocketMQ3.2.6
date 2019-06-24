/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.client.ClientHousekeepingService;
import com.alibaba.rocketmq.broker.client.ConsumerIdsChangeListener;
import com.alibaba.rocketmq.broker.client.ConsumerManager;
import com.alibaba.rocketmq.broker.client.DefaultConsumerIdsChangeListener;
import com.alibaba.rocketmq.broker.client.ProducerManager;
import com.alibaba.rocketmq.broker.client.net.Broker2Client;
import com.alibaba.rocketmq.broker.client.rebalance.RebalanceLockManager;
import com.alibaba.rocketmq.broker.filtersrv.FilterServerManager;
import com.alibaba.rocketmq.broker.longpolling.PullRequestHoldService;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageHook;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageHook;
import com.alibaba.rocketmq.broker.offset.ConsumerOffsetManager;
import com.alibaba.rocketmq.broker.out.BrokerOuterAPI;
import com.alibaba.rocketmq.broker.processor.AdminBrokerProcessor;
import com.alibaba.rocketmq.broker.processor.ClientManageProcessor;
import com.alibaba.rocketmq.broker.processor.EndTransactionProcessor;
import com.alibaba.rocketmq.broker.processor.PullMessageProcessor;
import com.alibaba.rocketmq.broker.processor.QueryMessageProcessor;
import com.alibaba.rocketmq.broker.processor.SendMessageProcessor;
import com.alibaba.rocketmq.broker.slave.SlaveSynchronize;
import com.alibaba.rocketmq.broker.subscription.SubscriptionGroupManager;
import com.alibaba.rocketmq.broker.topic.TopicConfigManager;
import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.DataVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.namesrv.RegisterBrokerResult;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.MessageStore;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import com.alibaba.rocketmq.store.stats.BrokerStats;
import com.alibaba.rocketmq.store.stats.BrokerStatsManager;

import java.net.InetSocketAddress;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class BrokerController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private final MessageStoreConfig messageStoreConfig;

    private final DataVersion configDataVersion = new DataVersion();

    private final ConsumerOffsetManager consumerOffsetManager;
    private final ConsumerManager consumerManager;
    private final ProducerManager producerManager;
    private TopicConfigManager topicConfigManager;
    private final SubscriptionGroupManager subscriptionGroupManager;

    private final ClientHousekeepingService clientHousekeepingService;
    private final PullMessageProcessor pullMessageProcessor;
    private final PullRequestHoldService pullRequestHoldService;
    private final Broker2Client broker2Client;
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();

    private final ConsumerIdsChangeListener consumerIdsChangeListener;

    private final BrokerOuterAPI brokerOuterAPI;
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("BrokerControllerScheduledThread"));
    private final SlaveSynchronize slaveSynchronize;
    private MessageStore messageStore;
    private RemotingServer remotingServer;

    private ExecutorService sendMessageExecutor;
    private ExecutorService pullMessageExecutor;
    private ExecutorService adminBrokerExecutor;
    private ExecutorService clientManageExecutor;
    private boolean updateMasterHAServerAddrPeriodically = false;

    private BrokerStats brokerStats;
    private final BlockingQueue<Runnable> sendThreadPoolQueue;

    private final BlockingQueue<Runnable> pullThreadPoolQueue;

    private final FilterServerManager filterServerManager;

    private final BrokerStatsManager brokerStatsManager;
    private InetSocketAddress storeHost;


    public BrokerController(//
            final BrokerConfig brokerConfig, //
            final NettyServerConfig nettyServerConfig, //
            final NettyClientConfig nettyClientConfig, //
            final MessageStoreConfig messageStoreConfig //
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.messageStoreConfig = messageStoreConfig;

        this.consumerOffsetManager = new ConsumerOffsetManager(this);
        this.topicConfigManager = new TopicConfigManager(this);
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pullRequestHoldService = new PullRequestHoldService(this);
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
        this.producerManager = new ProducerManager();
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.broker2Client = new Broker2Client(this);
        this.subscriptionGroupManager = new SubscriptionGroupManager(this);
        this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        this.filterServerManager = new FilterServerManager(this);

        if (this.brokerConfig.getNamesrvAddr() != null) {
            this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            log.info("user specfied name server address: {}", this.brokerConfig.getNamesrvAddr());
        }

        this.slaveSynchronize = new SlaveSynchronize(this);

        this.sendThreadPoolQueue =
                new LinkedBlockingQueue<Runnable>(this.brokerConfig.getSendThreadPoolQueueCapacity());

        this.pullThreadPoolQueue =
                new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPullThreadPoolQueueCapacity());

        this.brokerStatsManager = new BrokerStatsManager(this.brokerConfig.getBrokerClusterName());
        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), this
            .getNettyServerConfig().getListenPort()));
    }

    /**
     * 初始化操作
     * @return
     */
    public boolean initialize() {
        boolean result = true;
        //1,加载之前存在的topic配置[C:\Users\Administrator\store\config\topics.json]
        result = result && this.topicConfigManager.load();
        //2，加载之前的消费进度[C:\Users\Administrator\store\config\consumerOffset.json]
        result = result && this.consumerOffsetManager.load();
        //3,加载之前的订阅组情况[C:\Users\Administrator\store\config\subscriptionGroup.json]
        result = result && this.subscriptionGroupManager.load();

        if (result) {
            try {
                //4,创建存储层默认实现对象
                this.messageStore = new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager);
            }
            catch (IOException e) {
                result = false;
                e.printStackTrace();
            }
        }
        //5，加载存储数据
        result = result && this.messageStore.load();

        if (result) {
            //6，开启broker服务
            this.remotingServer =
                    new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
            //7，创建发送消息线程池
            this.sendMessageExecutor = new ThreadPoolExecutor(//
                this.brokerConfig.getSendMessageThreadPoolNums(),//
                this.brokerConfig.getSendMessageThreadPoolNums(),//
                1000 * 60,//
                TimeUnit.MILLISECONDS,//
                this.sendThreadPoolQueue,//
                new ThreadFactoryImpl("SendMessageThread_"));
            //8，创建拉信息线程池他如果提出
            this.pullMessageExecutor = new ThreadPoolExecutor(//
                this.brokerConfig.getPullMessageThreadPoolNums(),//
                this.brokerConfig.getPullMessageThreadPoolNums(),//
                1000 * 60,//
                TimeUnit.MILLISECONDS,//
                this.pullThreadPoolQueue,//
                new ThreadFactoryImpl("PullMessageThread_"));
            //9，broker管理线程池
            this.adminBrokerExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getAdminBrokerThreadPoolNums(),
                        new ThreadFactoryImpl("AdminBrokerThread_"));
            //10，客户端管理线程池
            this.clientManageExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getClientManageThreadPoolNums(),
                        new ThreadFactoryImpl("ClientManageThread_"));
            //11，NettyRemotingServer注册消息处理器【从消费端或者生产端收到请求包如何处理？这步决定】
            this.registerProcessor();

            this.brokerStats = new BrokerStats((DefaultMessageStore) this.messageStore);

            // TODO remove in future
            //距离下一天多长时间，单位毫秒，比如现在是20190623 16:34,则initialDelay = 20190624 00:00 - 20190623 16:34
            final long initialDelay = UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis();
            final long period = 1000 * 60 * 60 * 24;//毫秒为单位的24小时
            //每天00：00统计过去一天内生产消息总数以及消费消息总数
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.getBrokerStats().record();
                    }
                    catch (Exception e) {
                        log.error("schedule record error.", e);
                    }
                }
            }, initialDelay, period, TimeUnit.MILLISECONDS);
            //定时消费进度刷盘
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.persist();
                    }
                    catch (Exception e) {
                        log.error("schedule persist consumerOffset error.", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.scanUnsubscribedTopic();
                    }
                    catch (Exception e) {
                        log.error("schedule scanUnsubscribedTopic error.", e);
                    }
                }
            }, 10, 60, TimeUnit.MINUTES);

            if (this.brokerConfig.getNamesrvAddr() != null) {
                this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            }
            else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                        }
                        catch (Exception e) {
                            log.error("ScheduledTask fetchNameServerAddr exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
            }

            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                if (this.messageStoreConfig.getHaMasterAddress() != null
                        && this.messageStoreConfig.getHaMasterAddress().length() >= 6) {
                    this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                    this.updateMasterHAServerAddrPeriodically = false;
                }
                else {
                    this.updateMasterHAServerAddrPeriodically = true;
                }

                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.slaveSynchronize.syncAll();
                        }
                        catch (Exception e) {
                            log.error("ScheduledTask syncAll slave exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
            else {
                //每隔一分钟打印出master和slave差距
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.printMasterAndSlaveDiff();
                        }
                        catch (Exception e) {
                            log.error("schedule printMasterAndSlaveDiff error.", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
        }

        return result;
    }


    public void registerProcessor() {
        /**
         * SendMessageProcessor
         */
        SendMessageProcessor sendProcessor = new SendMessageProcessor(this);
        sendProcessor.registerSendMessageHook(sendMessageHookList);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor,
            this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor,
            this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor,
            this.sendMessageExecutor);

        /**
         * PullMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor,
            this.pullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        /**
         * QueryMessageProcessor
         */
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor,
            this.pullMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor,
            this.pullMessageExecutor);

        /**
         * ClientManageProcessor
         */
        ClientManageProcessor clientProcessor = new ClientManageProcessor(this);
        clientProcessor.registerConsumeMessageHook(this.consumeMessageHookList);
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor,
            this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor,
            this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, clientProcessor,
            this.clientManageExecutor);

        /**
         * Offset存储更新转移到ClientProcessor处理
         */
        this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, clientProcessor,
            this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, clientProcessor,
            this.clientManageExecutor);

        /**
         * EndTransactionProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this),
            this.sendMessageExecutor);

        /**
         * Default
         */
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
    }


    public Broker2Client getBroker2Client() {
        return broker2Client;
    }


    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }


    public String getConfigDataVersion() {
        return this.configDataVersion.toJson();
    }


    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }


    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }


    public MessageStore getMessageStore() {
        return messageStore;
    }


    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }


    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }


    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }


    public ProducerManager getProducerManager() {
        return producerManager;
    }


    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }


    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }


    public RemotingServer getRemotingServer() {
        return remotingServer;
    }


    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }


    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }


    public void shutdown() {
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.shutdown();
        }

        if (this.remotingServer != null) {
            this.remotingServer.shutdown();
        }

        if (this.messageStore != null) {
            this.messageStore.shutdown();
        }

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
        }

        this.unregisterBrokerAll();

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.shutdown();
        }

        this.consumerOffsetManager.persist();

        if (this.filterServerManager != null) {
            this.filterServerManager.shutdown();
        }
    }


    private void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(//
            this.brokerConfig.getBrokerClusterName(), //
            this.getBrokerAddr(), //
            this.brokerConfig.getBrokerName(), //
            this.brokerConfig.getBrokerId());
    }


    public String getBrokerAddr() {
        String addr = this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
        return addr;
    }


    public void start() throws Exception {
        if (this.messageStore != null) {
            //启动存储服务
            this.messageStore.start();
        }

        if (this.remotingServer != null) {
            //启动netty服务端，对外提供生产消息以及消费消息功能
            this.remotingServer.start();
        }

        if (this.brokerOuterAPI != null) {
            //作为client连接命名服务器
            this.brokerOuterAPI.start();
        }

        if (this.pullRequestHoldService != null) {
            // 拉消息请求管理，如果拉不到消息，则在这里Hold住，等待消息到来
            this.pullRequestHoldService.start();
        }

        if (this.clientHousekeepingService != null) {
            //定期检测客户端连接，清除不活动的连接
            this.clientHousekeepingService.start();
        }

        if (this.filterServerManager != null) {
            this.filterServerManager.start();
        }

        this.registerBrokerAll(true, false);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    BrokerController.this.registerBrokerAll(true, false);
                }
                catch (Exception e) {
                    log.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, 1000 * 30, TimeUnit.MILLISECONDS);

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }

        this.addDeleteTopicTask();
    }


    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway) {
        TopicConfigSerializeWrapper topicConfigWrapper =
                this.getTopicConfigManager().buildTopicConfigSerializeWrapper();

        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable =
                    new ConcurrentHashMap<String, TopicConfig>(topicConfigWrapper.getTopicConfigTable());
            for (TopicConfig topicConfig : topicConfigTable.values()) {
                topicConfig.setPerm(this.getBrokerConfig().getBrokerPermission());
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        RegisterBrokerResult registerBrokerResult = this.brokerOuterAPI.registerBrokerAll(//
            this.brokerConfig.getBrokerClusterName(), //
            this.getBrokerAddr(), //
            this.brokerConfig.getBrokerName(), //
            this.brokerConfig.getBrokerId(), //
            this.getHAServerAddr(), //
            topicConfigWrapper,//
            this.filterServerManager.buildNewFilterServerList(),//
            oneway);

        if (registerBrokerResult != null) {
            if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
                this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
            }

            this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());

            if (checkOrderConfig) {
                this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
            }
        }
    }


    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }


    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }


    public String getHAServerAddr() {
        String addr = this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
        return addr;
    }


    public void updateAllConfig(Properties properties) {
        MixAll.properties2Object(properties, brokerConfig);
        MixAll.properties2Object(properties, nettyServerConfig);
        MixAll.properties2Object(properties, nettyClientConfig);
        MixAll.properties2Object(properties, messageStoreConfig);
        this.configDataVersion.nextVersion();
        this.flushAllConfig();
    }


    private void flushAllConfig() {
        String allConfig = this.encodeAllConfig();
        try {
            MixAll.string2File(allConfig, BrokerPathConfigHelper.getBrokerConfigPath());
            log.info("flush broker config, {} OK", BrokerPathConfigHelper.getBrokerConfigPath());
        }
        catch (IOException e) {
            log.info("flush broker config Exception, " + BrokerPathConfigHelper.getBrokerConfigPath(), e);
        }
    }


    public String encodeAllConfig() {
        StringBuilder sb = new StringBuilder();
        {
            Properties properties = MixAll.object2Properties(this.brokerConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.messageStoreConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.nettyServerConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.nettyClientConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }
        return sb.toString();
    }


    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }


    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }


    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }


    public ExecutorService getPullMessageExecutor() {
        return pullMessageExecutor;
    }


    public void setPullMessageExecutor(ExecutorService pullMessageExecutor) {
        this.pullMessageExecutor = pullMessageExecutor;
    }


    public BrokerStats getBrokerStats() {
        return brokerStats;
    }


    public void setBrokerStats(BrokerStats brokerStats) {
        this.brokerStats = brokerStats;
    }


    public BlockingQueue<Runnable> getSendThreadPoolQueue() {
        return sendThreadPoolQueue;
    }


    public FilterServerManager getFilterServerManager() {
        return filterServerManager;
    }


    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }


    private void printMasterAndSlaveDiff() {
        long diff = this.messageStore.slaveFallBehindMuch();

        // XXX: warn and notify me
        log.info("slave fall behind master, how much, {} bytes", diff);
    }


    public void addDeleteTopicTask() {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                int removedTopicCnt =
                        BrokerController.this.messageStore.cleanUnusedTopic(BrokerController.this
                            .getTopicConfigManager().getTopicConfigTable().keySet());
                log.info("addDeleteTopicTask removed topic count {}", removedTopicCnt);
            }
        }, 5, TimeUnit.MINUTES);
    }

    private final List<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();


    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();


    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register ConsumeMessageHook Hook, {}", hook.hookName());
    }


    public void registerServerRPCHook(RPCHook rpcHook) {
        getRemotingServer().registerRPCHook(rpcHook);
    }


    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
    }


    public InetSocketAddress getStoreHost() {
        return storeHost;
    }


    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }
}
