/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 * <p>
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */


public class FastLeaderElection implements Election {
    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    final static int finalizeWait = 200;


    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    final static int maxNotificationInterval = 60000;

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    QuorumCnxManager manager;


    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */

    static public class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public final static int CURRENTVERSION = 0x1;
        int version;

        /*
         * Proposed leader
         */
        long leader;

        /*
         * zxid of the proposed leader
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * current state of sender
         */
        QuorumPeer.ServerState state;

        /*
         * Address of sender
         */
        long sid;

        /*
         * epoch of the proposed leader
         */
        long peerEpoch;

        @Override
        public String toString() {
            return Long.toHexString(version) + " (message format version), "
                   + leader + " (n.leader), 0x"
                   + Long.toHexString(zxid) + " (n.zxid), 0x"
                   + Long.toHexString(electionEpoch) + " (n.round), " + state
                   + " (n.state), " + sid + " (n.sid), 0x"
                   + Long.toHexString(peerEpoch) + " (n.peerEpoch) ";
        }
    }

    static ByteBuffer buildMsg(int state,
                               long leader,
                               long zxid,
                               long electionEpoch,
                               long epoch) {
        byte requestBytes[] = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);

        return requestBuffer;
    }

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    static public class ToSend {
        static enum mType {crequest, challenge, notification, ack}

        ToSend(mType type,
               long leader,
               long zxid,
               long electionEpoch,
               ServerState state,
               long sid,
               long peerEpoch) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
        }

        /*
         * Proposed leader in the case of notification
         */
        long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * Current state;
         */
        QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */
        long sid;

        /*
         * Leader epoch
         */
        long peerEpoch;
    }

    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */

    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */

        class WorkerReceiver extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if (response == null) continue;

                        /*
                         * If it is from an observer, respond right away.
                         * Note that the following predicate assumes that
                         * if a server is not a follower, then it must be
                         * an observer. If we ever have any other type of
                         * learner in the future, we'll have to change the
                         * way we check for observers.
                         */
                        if (!validVoter(response.sid)) {
                            Vote current = self.getCurrentVote();
                            ToSend notmsg = new ToSend(ToSend.mType.notification,
                                                       current.getId(),
                                                       current.getZxid(),
                                                       logicalclock.get(),
                                                       self.getPeerState(),
                                                       response.sid,
                                                       current.getPeerEpoch());

                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Receive new notification message. My id = "
                                          + self.getId());
                            }

                            /*
                             * We check for 28 bytes for backward compatibility
                             */
                            if (response.buffer.capacity() < 28) {
                                LOG.error("Got a short response: "
                                          + response.buffer.capacity());
                                continue;
                            }
                            boolean backCompatibility = (response.buffer.capacity() == 28);
                            response.buffer.clear();

                            // Instantiate Notification and set its attributes
                            Notification n = new Notification();

                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (response.buffer.getInt()) {
                                case 0:
                                    ackstate = QuorumPeer.ServerState.LOOKING;
                                    break;
                                case 1:
                                    ackstate = QuorumPeer.ServerState.FOLLOWING;
                                    break;
                                case 2:
                                    ackstate = QuorumPeer.ServerState.LEADING;
                                    break;
                                case 3:
                                    ackstate = QuorumPeer.ServerState.OBSERVING;
                                    break;
                                default:
                                    continue;
                            }

                            n.leader = response.buffer.getLong();
                            n.zxid = response.buffer.getLong();
                            n.electionEpoch = response.buffer.getLong();
                            n.state = ackstate;
                            n.sid = response.sid;
                            if (!backCompatibility) {
                                n.peerEpoch = response.buffer.getLong();
                            } else {
                                if (LOG.isInfoEnabled()) {
                                    LOG.info("Backward compatibility mode, server id=" + n.sid);
                                }
                                n.peerEpoch = ZxidUtils.getEpochFromZxid(n.zxid);
                            }

                            /*
                             * Version added in 3.4.6
                             */

                            n.version = (response.buffer.remaining() >= 4) ?
                                        response.buffer.getInt() : 0x0;

                            /*
                             * Print notification info
                             */
                            if (LOG.isInfoEnabled()) {
                                printNotification(n);
                            }

                            /*
                             * If this server is looking, then send proposed leader
                             */

                            if (self.getPeerState() == QuorumPeer.ServerState.LOOKING) {
                                recvqueue.offer(n);

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if ((ackstate == QuorumPeer.ServerState.LOOKING)
                                    && (n.electionEpoch < logicalclock.get())) {
                                    Vote v = getVote();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification,
                                                               v.getId(),
                                                               v.getZxid(),
                                                               logicalclock.get(),
                                                               self.getPeerState(),
                                                               response.sid,
                                                               v.getPeerEpoch());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                Vote current = self.getCurrentVote();
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Sending new notification. My id =  " +
                                                  self.getId() + " recipient=" +
                                                  response.sid + " zxid=0x" +
                                                  Long.toHexString(current.getZxid()) +
                                                  " leader=" + current.getId());
                                    }

                                    ToSend notmsg;
                                    if (n.version > 0x0) {
                                        notmsg = new ToSend(
                                                ToSend.mType.notification,
                                                current.getId(),
                                                current.getZxid(),
                                                current.getElectionEpoch(),
                                                self.getPeerState(),
                                                response.sid,
                                                current.getPeerEpoch());

                                    } else {
                                        Vote bcVote = self.getBCVote();
                                        notmsg = new ToSend(
                                                ToSend.mType.notification,
                                                bcVote.getId(),
                                                bcVote.getZxid(),
                                                bcVote.getElectionEpoch(),
                                                self.getPeerState(),
                                                response.sid,
                                                bcVote.getPeerEpoch());
                                    }
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        System.out.println("Interrupted Exception while waiting for new message" +
                                           e.toString());
                    }
                }
                LOG.info("WorkerReceiver is down");
            }
        }


        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */

        class WorkerSender extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
               // 不断从阻塞队列中poll数据
                while (!stop) {
                    try {
                        //带有超时阻塞的机制去从阻塞队列中获得数据
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if (m == null) continue;
                        // 处理数据，这里是进行发送
                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m message to send
             */
            void process(ToSend m) {
                // 构建消息
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(),
                                                    m.leader,
                                                    m.zxid,
                                                    m.electionEpoch,
                                                    m.peerEpoch);
                // 发送数据
                manager.toSend(m.sid, requestBuffer);
            }
        }


        WorkerSender ws;
        WorkerReceiver wr;

        /**
         * Constructor of class Messenger.
         *
         * @param manager Connection manager
         */
        Messenger(QuorumCnxManager manager) {

            // 发送票据的线程(用于消费sendQueue)
            this.ws = new WorkerSender(manager);

            Thread t = new Thread(this.ws,
                                  "WorkerSender[myid=" + self.getId() + "]");
            // 守护线程
            t.setDaemon(true);
            t.start();
            // 接收票据的线程(用于消费recvqueue)
            this.wr = new WorkerReceiver(manager);

            t = new Thread(this.wr,
                           "WorkerReceiver[myid=" + self.getId() + "]");
            t.setDaemon(true);
            t.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */
    long proposedLeader;
    long proposedZxid;
    long proposedEpoch;


    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock.get();
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        this.manager = manager;
        // 开启发送和接收票据两个线程
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;
        // 创建发送阻塞队列
        sendqueue = new LinkedBlockingQueue<ToSend>();
        // 创建接受阻塞队列
        recvqueue = new LinkedBlockingQueue<Notification>();
        // 实例化messager对象
        this.messenger = new Messenger(manager);
    }

    private void leaveInstance(Vote v) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("About to leave FLE instance: leader="
                      + v.getId() + ", zxid=0x" +
                      Long.toHexString(v.getZxid()) + ", my id=" + self.getId()
                      + ", my state=" + self.getPeerState());
        }
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;

    public void shutdown() {
        stop = true;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }


    /**
     * Send notifications to all peers upon a change in our vote
     */
    private void sendNotifications() {
        for (QuorumServer server : self.getVotingView().values()) {
            long sid = server.id;

            ToSend notmsg = new ToSend(ToSend.mType.notification,
                                       proposedLeader, // myid
                                       proposedZxid, // zxid
                                       logicalclock.get(),// epoch
                                       QuorumPeer.ServerState.LOOKING,// 节点状态 LOOKING
                                       sid, // myid
                                       proposedEpoch); // epoch
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending Notification: " + proposedLeader + " (n.leader), 0x" +
                          Long.toHexString(proposedZxid) + " (n.zxid), 0x" + Long.toHexString(logicalclock.get()) +
                          " (n.round), " + sid + " (recipient), " + self.getId() +
                          " (myid), 0x" + Long.toHexString(proposedEpoch) + " (n.peerEpoch)");
            }
            // 阻塞队列,  线程->生产者消费者模式
            // 这里将数据放入sendqueue中，然后通过WorkerSender线程行出去
            sendqueue.offer(notmsg);
        }
    }


    private void printNotification(Notification n) {
        LOG.info("Notification: " + n.toString()
                 + self.getPeerState() + " (my state)");
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" +
                  Long.toHexString(newZxid) + ", proposed zxid: 0x" + Long.toHexString(curZxid));
        // 默认权重会返回1
        if (self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }

        /**
         * 下列情况返回true
         * new开头表示接收到的消息
         * 1.新的epoch大于当前的epoch
         * 2.在epoch相同的情况下，新的zxid大于当前zxid
         * 3.在epoch相同的情况下，新的zxid==curZxid时，新的myid大于myid
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher 
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */

        return ((newEpoch > curEpoch) ||
                ((newEpoch == curEpoch) &&
                 ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));
    }

    /**
     * Termination predicate. Given a set of votes, determines if
     * have sufficient to declare the end of the election round.
     *
     * @param votes Set of votes
     */
    protected boolean termPredicate(
            HashMap<Long, Vote> votes,
            Vote vote) {

        HashSet<Long> set = new HashSet<Long>();

        /*
         * First make the views consistent. Sometimes peers will have
         * different zxids for a server depending on timing.
         *
         */
        // vote为当前节点的选票
        // votes表示收到的外部选票
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            // 对选票进行归纳，就是把所有选票数据中和当前节点的票据相同的票据进行统计
            // 注意key为sid，也就是服务器的id server.id=ip:port:port，然后进行归纳
            // equals相同，说明是同一票决
            if (vote.equals(entry.getValue())) {
                set.add(entry.getKey());
            }
        }
        // 这里就是验证是否有过半的票数
        return self.getQuorumVerifier().containsQuorum(set);
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes         set of votes
     * @param leader        leader id
     * @param electionEpoch epoch id
     */
    protected boolean checkLeader(
            HashMap<Long, Vote> votes,
            long leader,
            long electionEpoch) {

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */
        // 判断是否为leader 
        if (leader != self.getId()) {
            if (votes.get(leader) == null) {
                predicate = false;
            } else if (votes.get(leader).getState() != ServerState.LEADING) {
                predicate = false;
            }
        } else if (logicalclock.get() != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    /**
     * This predicate checks that a leader has been elected. It doesn't
     * make a lot of sense without context (check lookForLeader) and it
     * has been separated for testing purposes.
     *
     * @param recv map of received votes
     * @param ooe  map containing out of election votes (LEADING or FOLLOWING)
     * @param n    Notification
     * @return
     */
    protected boolean ooePredicate(HashMap<Long, Vote> recv,
                                   HashMap<Long, Vote> ooe,
                                   Notification n) {

        return (termPredicate(recv, new Vote(n.version,
                                             n.leader,
                                             n.zxid,
                                             n.electionEpoch,
                                             n.peerEpoch,
                                             n.state))
                && checkLeader(ooe, n.leader, n.electionEpoch));

    }

    synchronized void updateProposal(long leader, long zxid, long epoch) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating proposal: " + leader + " (newleader), 0x"
                      + Long.toHexString(zxid) + " (newzxid), " + proposedLeader
                      + " (oldleader), 0x" + Long.toHexString(proposedZxid) + " (oldzxid)");
        }
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    synchronized Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        // 默认是FOLLOWING状态
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I'm a participant: " + self.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I'm an observer: " + self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getId();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getLastLoggedZxid();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT)
            try {
                return self.getCurrentEpoch();
            } catch (IOException e) {
                RuntimeException re = new RuntimeException(e.getMessage());
                re.setStackTrace(e.getStackTrace());
                throw re;
            }
        else return Long.MIN_VALUE;
    }

    /**
     * 开始发起投票流程
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    @Override
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(
                    self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }
        if (self.start_fle == 0) {
            self.start_fle = Time.currentElapsedTime();
        }
        try {
            // 接收到的票据的集合
            HashMap<Long, Vote> recvset = new HashMap<Long, Vote>();

            // 存储接受到leader消息的集合
            HashMap<Long, Vote> outofelection = new HashMap<Long, Vote>();

            int notTimeout = finalizeWait;

            synchronized (this) {
                // 更新逻辑时钟，用来判断是否在同一轮选举周期 原子自增
                logicalclock.incrementAndGet();
                //proposal
                // 初始化选票数据，其实就是把myid，zxid和epoch更新到本地的成员属性
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            LOG.info("New election. My id =  " + self.getId() +
                     ", proposed zxid=0x" + Long.toHexString(proposedZxid));
            // 异步发送选举信息 
            sendNotifications();

            /* 不断循环直到选举出leader
             * Loop in which we exchange notifications until we find a leader
             */
            while ((self.getPeerState() == ServerState.LOOKING) &&
                   (!stop)) {
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 */
                // recvqueue是从网络上接收到的其他机器的Notification
                Notification n = recvqueue.poll(notTimeout,
                                                TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                // 如果没有获取到外部的投票，有可能是集群之间的节点没有真正连接上
                if (n == null) {
                    // 判断发送队列是否有数据，如果发送队列为空，再发一次自己的选票
                    if (manager.haveDelivered()) {
                        sendNotifications();
                    } else {
                        // 重新连接集群中的所有节点
                        manager.connectAll();
                    }

                    /*
                     * Exponential backoff
                     */
                    int tmpTimeOut = notTimeout * 2;
                    notTimeout = (tmpTimeOut < maxNotificationInterval ?
                                  tmpTimeOut : maxNotificationInterval);
                    LOG.info("Notification time out: " + notTimeout);
                }
                // 判断是否是一个有效的票据
                // validVoter判断sid是否在配置文件中的server.id中
                // 判断收到的选票中的 sid 和选举的 leader 的 sid 是否存在于我们的集群所配置的myid范围中
                else if (validVoter(n.sid) && validVoter(n.leader)) {
                    /*
                     * Only proceed if the vote comes from a replica in the
                     * voting view for a replica in the voting view.
                     */
                    // 判断接收到的投票者的状态，默认是 LOOKING 状态,说明当前发起投票的服务器也是在找 leader
                    switch (n.state) {
                        // 第一次进入到这个case
                        case LOOKING:
                            // If notification > current, replace and send messages out
                            // 如果收到的投票的逻辑时钟大于当前的节点的逻辑时钟
                            if (n.electionEpoch > logicalclock.get()) {
                                // 更新成新一轮的时钟
                                logicalclock.set(n.electionEpoch);
                                // 清空原来的接收票据集合 因为是新一轮投票了
                                recvset.clear();
                                // 收到票据之后，当前的server要听谁的。
                                // 可能是听server1的、也可能是听server2，也可能是听server3
                                /**
                                 * zab  leader选举算法
                                 * 比较接收的投票信息，比较epoch、zxid、myid,如果返回 true，
                                 * 则更新当前节点的票据（sid,zxid,epoch）,那么下次再发起投票的时候，就不再是选自己了
                                 * 判断是否要更新票据
                                 */
                                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                                        getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                    // 把自己的票据更新成对方的票据，那么下一次，发送的票据就是新的票据
                                    updateProposal(n.leader, n.zxid, n.peerEpoch);
                                } else {
                                    // 收到的票据小于当前的节点的票据，下一次发送票据，仍然发送自己的
                                    updateProposal(getInitId(),
                                                   getInitLastLoggedZxid(),
                                                   getPeerEpoch());
                                }
                                // 继续发送通知
                                sendNotifications();
                            }
                            // 如果收到消息的epoch小于当前的epoch，则直接丢弃这张票据
                            else if (n.electionEpoch < logicalclock.get()) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x"
                                              + Long.toHexString(n.electionEpoch)
                                              + ", logicalclock=0x" + Long.toHexString(logicalclock.get()));
                                }
                                break;
                            }
                            // 如果epoch相等，则使用zxid、myid进行比较
                            else if (totalOrderPredicate(n.leader,
                                                         n.zxid,
                                                         n.peerEpoch,
                                                         proposedLeader,
                                                         proposedZxid,
                                                         proposedEpoch)) {
                                // 返回true，则需要将自己票据更新为收到的票据，然后继续发送通知
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                                // 告诉集群自己要选择n为leader
                                sendNotifications();
                            }

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Adding vote: from=" + n.sid +
                                          ", proposed leader=" + n.leader +
                                          ", proposed zxid=0x" + Long.toHexString(n.zxid) +
                                          ", proposed election epoch=0x" + Long.toHexString(n.electionEpoch));
                            }
                            // 将收到的投票信息放入投票的集合 recvset 中, 用来作为最终的 "过半原则" 判断
                            // 凡是收到的票据都会进入recvset(hashMap中，自己投自己也会出现在集合中，key为服务器id)
                            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));
                            // 决断时刻(当前节点的更新后的vote信息，和recvset集合中的票据进行归纳)
                            if (termPredicate(recvset,
                                              new Vote(proposedLeader, proposedZxid,
                                                       logicalclock.get(), proposedEpoch))) {

                                // Verify if there is any change in the proposed leader
                                /**
                                 * 进入这个判断，说明选票达到了 leader 选举的要求
                                 * 在更新状态之前，服务器会等待 finalizeWait 毫秒时间来接收新的选票，以防止漏下关键选票。
                                 *  如果收到可能改变 Leader 的新选票，则重新进行计票
                                 */
                                while ((n = recvqueue.poll(finalizeWait,
                                                           TimeUnit.MILLISECONDS)) != null) {
                                    if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                                            proposedLeader, proposedZxid, proposedEpoch)) {
                                        recvqueue.put(n);
                                        break;
                                    }
                                }

                                /*
                                 * This predicate is true once we don't read any new
                                 * relevant message from the reception queue
                                 */
                                // 如果n为空，说明leader节点确定好了
                                if (n == null) {
                                    // 设置当前当前节点的状态，判断leader节点是不是我自己
                                    // 如果是，直接更新当前节点的 state 为 LEADING
                                    //否则，根据当前节点的特性进行判断，决定是FOLLOWING还是OBSERVING
                                    self.setPeerState((proposedLeader == self.getId()) ?
                                                      ServerState.LEADING : learningState());

                                    // 组装生成这次 Leader 选举最终的投票的结果
                                    Vote endVote = new Vote(proposedLeader,
                                                            proposedZxid,
                                                            logicalclock.get(),
                                                            proposedEpoch);
                                    // 清空接收到的票据的集合
                                    leaveInstance(endVote);
                                    // 返回最终票据
                                    return endVote;
                                }
                            }
                            break;
                        // OBSERVING不参与leader选举    
                        case OBSERVING:
                            LOG.debug("Notification from observer: " + n.sid);
                            break;
                        case FOLLOWING:
                        case LEADING:
                            /*
                             * Consider all notifications from the same epoch
                             * together.
                             */
                            // 判断epoch是否处于同一时期
                            if (n.electionEpoch == logicalclock.get()) {
                                // 收集选票
                                recvset.put(n.sid, new Vote(n.leader,
                                                            n.zxid,
                                                            n.electionEpoch,
                                                            n.peerEpoch));
                                
                                // 检查leader是否进行了选举
                                if (ooePredicate(recvset, outofelection, n)) {
                                    // 上面返回true，说明leader已经选举成功
                                    // 更新本机节点的状态
                                    self.setPeerState((n.leader == self.getId()) ?
                                                      ServerState.LEADING : learningState());

                                    Vote endVote = new Vote(n.leader,
                                                            n.zxid,
                                                            n.electionEpoch,
                                                            n.peerEpoch);
                                    // 清空接受队列
                                    leaveInstance(endVote);
                                    // 返回最终票据
                                    return endVote;
                                }
                            }

                            /*
                             * Before joining an established ensemble, verify
                             * a majority is following the same leader.
                             */
                            // 将接受到的票据进行存储
                            outofelection.put(n.sid, new Vote(n.version,
                                                              n.leader,
                                                              n.zxid,
                                                              n.electionEpoch,
                                                              n.peerEpoch,
                                                              n.state));

                            // 决断当前节点是否为leader
                            if (ooePredicate(outofelection, outofelection, n)) {
                               // 如果是，则更新epoch，然后更新当前节点的状态
                                synchronized (this) {
                                    logicalclock.set(n.electionEpoch);
                                    self.setPeerState((n.leader == self.getId()) ?
                                                      ServerState.LEADING : learningState());
                                }
                                Vote endVote = new Vote(n.leader,
                                                        n.zxid,
                                                        n.electionEpoch,
                                                        n.peerEpoch);
                                leaveInstance(endVote);
                                return endVote;
                            }
                            break;
                        default:
                            LOG.warn("Notification state unrecognized: {} (n.state), {} (n.sid)",
                                     n.state, n.sid);
                            break;
                    }
                } else {
                    if (!validVoter(n.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if (self.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(
                            self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}",
                      manager.getConnectionThreadCount());
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return self.getVotingView().containsKey(sid);
    }
}
