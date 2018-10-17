package service;

import message.Message;
import thread.ThreadHandler;
import util.AddressPortPair;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PaxosLogServer {

    private final static int HEART_BEAT_PERIOD_MILLS = 5000;

    private final int serverId;
    private final String serverAddr;
    private final int serverPort;

    private final int totalNumOfReplicas;
    private final List<AddressPortPair> allReplicasInfo;

    private final int skipSlotSeqNum;
    private final double messageLossRate;

    private final List<Socket> allReceiveSockets;
    private final List<Socket> allReplicaSendSockets;
    private final Queue<Message> messageQueue;

    public PaxosLogServer(final int serverId, final String serverAddr, final int serverPort, final int numOfToleratedFailures,
                          final List<AddressPortPair> allReplicasInfo, final int skipSlotSeqNum, final double messageLossRate) {
        this.serverId = serverId;
        this.serverAddr = serverAddr;
        this.serverPort = serverPort;
        this.totalNumOfReplicas = numOfToleratedFailures * 2 + 1;
        this.allReplicasInfo = allReplicasInfo;
        this.skipSlotSeqNum = skipSlotSeqNum;
        this.messageLossRate = messageLossRate;
        this.allReceiveSockets = new Vector<>();
        this.allReplicaSendSockets = new Vector<>();
        this.messageQueue = new ConcurrentLinkedQueue<>();
        System.out.println("Server with ID: " + serverId + " initialize at address: " + serverAddr + ':' + serverPort);
    }

    public void start() {
        new Thread(new IncomingSocketHandler(serverPort)).start();
        createSocketsForReplicas();
    }

    private void createSocketsForReplicas() {
        for (int i = 0; i < allReplicasInfo.size(); i++) {
            if (serverId == i) {
                continue;
            }
            try {
                final Socket socket = new Socket(allReplicasInfo.get(i).getIp(), allReplicasInfo.get(i).getPort());
                if (socket != null) {
                    allReceiveSockets.add(socket);
                    new Thread(new IncomingMessageHandler(socket)).start();
                }
            } catch (Exception e) {
                System.out.println("Replica whose address is " + allReplicasInfo.get(i).getIp()
                        + ':' + allReplicasInfo.get(i).getPort() + "is not accessible now");
            }
        }
    }


    public class IncomingSocketHandler implements Runnable {

        private ServerSocket serverSocket;

        public IncomingSocketHandler(final int port) {
            try {
                serverSocket = new ServerSocket(port);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Server with ID: " + serverId + "fail to listen to port:" + serverPort + ". Terminating...");
                System.exit(1);
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Socket acceptedSocket = serverSocket.accept();
                    allReceiveSockets.add(acceptedSocket);
                    new Thread(new IncomingMessageHandler(acceptedSocket)).start();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Server with ID: " + serverId + "fail to accept connection");
                }
            }
        }
    }

    public class IncomingMessageHandler extends ThreadHandler {

        public IncomingMessageHandler(Socket socket) {
            super(socket);
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line = super.bufferedReader.readLine()) != null) {
                    messageQueue.offer(new Message(line));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
