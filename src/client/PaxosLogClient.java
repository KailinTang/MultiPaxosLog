package client;

import java.io.IOException;

import message.ClientToServerMsg;
import message.Message;
import message.ServerToClientMsg;
import thread.ThreadHandler;
import util.AddressPortPair;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Random;


public class PaxosLogClient {

    private final static long TIME_OUT_RETRANSMIT_PERIOD = 10000;

    private final long clientId;
    private final String clientAddr;
    private final int clientPort;
    private int clientSequence;

    private final int totalNumOfReplicas;
    private final List<AddressPortPair> allReplicasInfo;

    private final double messageLossRate;

    private boolean receivedLastSendMsgResponse;
    private int leaderServerID;

    private final List<Socket> allReceiveSockets;

    // for allClientSendSockets, the key is the replica ID and value is the socket used to send message to other replicas
    private final Map<Integer, Socket> allClientSendSockets;

    private final Queue<ClientToServerMsg.ChatMsg> sendMessageQueue;
    private final Queue<String> receiveMessageQueue;
    private ClientToServerMsg.ChatMsg nextSendMsg;
    private Message nextMsg;

    private final ClientToServerMsg.HelloMsg MsgHello;

    private final Random randomServerId;
    private boolean receivedNack;
    private final Map<Integer, Boolean> ReceivedResponseForHello;
    private int HelloID;

    public PaxosLogClient(
            final String clientAddr,
            final int clientPort,
            final List<AddressPortPair> allReplicasInfo,
            final double messageLossRate
    ) {
        this.clientId = System.currentTimeMillis();
        this.clientAddr = clientAddr;
        this.clientPort = clientPort;
        this.clientSequence = 0;
        this.allReplicasInfo = allReplicasInfo;
        this.messageLossRate = messageLossRate;
        this.totalNumOfReplicas = allReplicasInfo.size();
        this.allReceiveSockets = new Vector<>();
        this.allClientSendSockets = new ConcurrentHashMap<>();
        this.receiveMessageQueue = new ConcurrentLinkedQueue<>();
        this.sendMessageQueue = new ConcurrentLinkedQueue<>();
        // at the beginning, we should never wait for the previous message
        this.receivedLastSendMsgResponse = true;
        this.leaderServerID = 0;
        this.MsgHello = new ClientToServerMsg.HelloMsg(clientId, clientAddr, clientPort);
        this.randomServerId = new Random(totalNumOfReplicas);
        this.receivedNack = false;
        this.ReceivedResponseForHello = new HashMap<>();
        this.HelloID = 0;

        System.out.println("Client with ID: " + clientId + " initialize at address: " + clientAddr + ':' + clientPort);
    }


    /**
     * Entrance of the client
     */
    public void start() {
        new Thread(new IncomingSocketHandler(clientPort)).start();
        new Thread(new ScannerHandler()).start();
        createSendSocketsForClientsIfNecessary();
        runQueuesProcessor();
    }

    /**
     * Create replica sending sockets if we don't have 2f such sockets or some such sockets are died
     */
    private void createSendSocketsForClientsIfNecessary() {
        // If all replicas sending sockets are alive and # of those equal to 2f, we no longer
        if (areAllSendSocketsAlive() && allClientSendSockets.size() == totalNumOfReplicas) {
            return;
        } else {
            createSendSocketsForClients();
        }
    }

    /**
     * Create replica sending sockets if we don't have that connection and save it to allClientSendSockets
     */
    private void createSendSocketsForClients() {
        for (int i = 0; i < allReplicasInfo.size(); i++) {
            try {
                if (!allClientSendSockets.containsKey(i)) {
                    final Socket socket = new Socket(allReplicasInfo.get(i).getIp(), allReplicasInfo.get(i).getPort());
                    if (socket != null && socket.isConnected()) {
                        allClientSendSockets.put(i, socket);
                    }
                }
            } catch (Exception e) {
                System.out.println("Server whose address is " + allReplicasInfo.get(i).getIp()
                        + ':' + allReplicasInfo.get(i).getPort() + " is not accessible now");
            }
        }
    }

    /**
     * @return Whether all sockets in allClientSendSockets are alive
     */
    private boolean areAllSendSocketsAlive() {
        if (allClientSendSockets.size() == 0) {
            return false;
        }
        for (final Integer replicaID : this.allClientSendSockets.keySet()) {
            if (!allClientSendSockets.get(replicaID).isConnected()) {
                allClientSendSockets.remove(replicaID);   // remove the dead sockets if necessary
                return false;
            }
        }
        return true;
    }


    /**
     * A worker for listening the port of server and create receiving sockets for any incoming sockets (client & other replicas)
     * Note that the sockets created in this worker is only responsible for receiving messages
     */
    public class IncomingSocketHandler implements Runnable {

        private ServerSocket serverSocket;

        public IncomingSocketHandler(final int port) {
            try {
                serverSocket = new ServerSocket(port);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Client with ID: " + clientId + "fail to listen to port:" + clientPort + ". Terminating...");
                System.exit(1);
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    final Socket acceptedSocket = serverSocket.accept();
                    allReceiveSockets.add(acceptedSocket);
                    new Thread(new ReceiveMessageHandler(acceptedSocket)).start();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Client with ID: " + clientId + "fail to accept connection");
                }
            }
        }
    }

    /**
     * A worker for receive messages from each socket which is responsible for receiving messages
     * According to the different message received (client, heartbeat or replica message), perform different operations
     */
    public class ReceiveMessageHandler extends ThreadHandler {

        public ReceiveMessageHandler(Socket socket) {
            super(socket);
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line = super.bufferedReader.readLine()) != null) {
                    System.out.println(line);
                    receiveMessageQueue.offer(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * A worker to deal with input message from scanner and cache it in the sendMessageQueue
     */
    public class ScannerHandler implements Runnable {

        private Scanner scanner;

        public ScannerHandler() {
            try {
                this.scanner = new Scanner(System.in);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Fail to build scanner");

            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    String nextLine = scanner.nextLine();
                    sendMessageQueue.offer(new ClientToServerMsg.ChatMsg(clientId, clientSequence, nextLine));
                    System.out.println("client want to send message: " + nextLine);
                    clientSequence += 1;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Fail to add msg to sendMessageQueue");
                }
            }
        }
    }

    /**
     * Handle messages in sendMessageQueue and receiveMessageQueue
     */
    public void runQueuesProcessor() {

        while (true) {
            if (receivedLastSendMsgResponse == true) {
                nextSendMsg = sendMessageQueue.peek();
                if (nextSendMsg != null) {
                    receivedLastSendMsgResponse = false;
                    sendHelloRandom();
                    new ReTransmitSchedulerHello(HelloID, TIME_OUT_RETRANSMIT_PERIOD);
                }
            }

            String nextString = receiveMessageQueue.poll();
            if (nextString != null && Message.getMessageType(nextString) == Message.MESSAGE_TYPE.SERVER_TO_CLIENT) {
                switch (ServerToClientMsg.getServerToClientType(nextString)) {
                    case ACK:
                        ReceivedResponseForHello.put(HelloID, true);
                        nextMsg = ServerToClientMsg.ServerAckMsg.fromString(nextString);
                        try {
                            PrintWriter leaderPrintWriter = new PrintWriter(allClientSendSockets.get(leaderServerID).getOutputStream(), true);
                            leaderPrintWriter.println(nextSendMsg.toString());
                            receivedNack = false;
                            new ReTransmitScheduler(nextSendMsg, TIME_OUT_RETRANSMIT_PERIOD);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.printf("build printWriter failed for index %s", leaderServerID);

                        }
                        break;
                    case NACK:
                        receivedNack = true;
                        ReceivedResponseForHello.put(HelloID, true);
                        nextMsg = ServerToClientMsg.ServerNackMsg.fromString(nextString);
                        leaderServerID = ((ServerToClientMsg.ServerNackMsg) nextMsg).getCurrentLeaderId();
                        sendHello();
                        new ReTransmitSchedulerHello(HelloID, TIME_OUT_RETRANSMIT_PERIOD);
                        break;
                    case RESPONSE:
                        nextMsg = ServerToClientMsg.ServerResponseMsg.fromString(nextString);
                        if (nextSendMsg.getMessageSequenceNumber() == ((ServerToClientMsg.ServerResponseMsg) nextMsg).getMessageSequenceNumber()) {
                            sendMessageQueue.poll();
                            receivedLastSendMsgResponse = true;
                        } else {
                            throw new IllegalStateException("received inconsistent message response");
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Can not detect message type in message queue");
                }
            }

        }
    }


    /* define the function to send next msg to leader*/
    //
    //
    //

    public void sendHelloRandom() {

        leaderServerID = randomServerId.nextInt(totalNumOfReplicas);
        sendHello();
    }


    public void sendHello() {
        try {
            HelloID += 1;
            System.out.println(allClientSendSockets.entrySet().size());
            System.out.println(allClientSendSockets.containsKey(leaderServerID));
            System.out.println(leaderServerID);
            PrintWriter printWriterRandom = new PrintWriter(allClientSendSockets.get(leaderServerID).getOutputStream(), true);
            printWriterRandom.println(MsgHello.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("send Hello failed");
        }
    }


    /**
     * A retransmit task that can be executed periodically if timeout
     */
    public class WaitRepeatSendTask extends TimerTask {

        final ClientToServerMsg.ChatMsg curMsg;

        public WaitRepeatSendTask(ClientToServerMsg.ChatMsg curMsg) {
            this.curMsg = curMsg;
        }

        @Override
        public void run() {

            if (curMsg == sendMessageQueue.peek() && !receivedNack) {
                sendHelloRandom();
            } else {
                super.cancel();
            }
        }
    }


    /**
     * A scheduler executing message retransmit if we do not receive response from server (timeout)
     */
    public class ReTransmitScheduler {

        final Timer timer;

        public ReTransmitScheduler(ClientToServerMsg.ChatMsg curMsg, long millSeconds) {
            timer = new Timer();
            final Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.MILLISECOND, (int) millSeconds);
            final Date time = calendar.getTime();
            timer.schedule(new WaitRepeatSendTask(curMsg), time, millSeconds);
        }
    }


    public class WaitRepeatSendHello extends TimerTask {

        int HelloID;

        public WaitRepeatSendHello(int HelloID) {
            this.HelloID = HelloID;
        }

        @Override
        public void run() {

            if (!ReceivedResponseForHello.get(HelloID)) {
                sendHelloRandom();
            } else {
                super.cancel();
            }
        }
    }


    /**
     * A scheduler executing message retransmit if we do not receive response from server (timeout)
     */
    public class ReTransmitSchedulerHello {

        final Timer timer;

        public ReTransmitSchedulerHello(int HelloID, long millSeconds) {
            timer = new Timer();
            final Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.MILLISECOND, (int) millSeconds);
            final Date time = calendar.getTime();
            timer.schedule(new WaitRepeatSendHello(HelloID), time, millSeconds);
        }
    }

}
