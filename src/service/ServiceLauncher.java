package service;

import util.AddressPortPair;

import java.util.ArrayList;
import java.util.List;

public class ServiceLauncher {

    public static void main(String[] args) {
        final List<AddressPortPair> allReplicasInfo = new ArrayList<>();
        allReplicasInfo.add(new AddressPortPair("127.0.0.1", 3057));
        allReplicasInfo.add(new AddressPortPair("127.0.0.1", 3058));
        allReplicasInfo.add(new AddressPortPair("127.0.0.1", 3059));

        final PaxosLogServer logServer = new PaxosLogServer(
                0,
                "127.0.0.1",
                3057,
                true,
                1,
                allReplicasInfo,
                0,
                0.0
        );

        logServer.start();

    }

}
