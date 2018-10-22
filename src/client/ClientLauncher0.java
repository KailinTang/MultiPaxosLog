package client;


import util.AddressPortPair;

import java.util.ArrayList;
import java.util.List;

public class ClientLauncher0 {

    public static void main(String args[]) {

        final List<AddressPortPair> allReplicasInfo = new ArrayList<>();
        allReplicasInfo.add(new AddressPortPair("127.0.0.1", 3057));
        allReplicasInfo.add(new AddressPortPair("127.0.0.1", 3058));
        allReplicasInfo.add(new AddressPortPair("127.0.0.1", 3059));

        final PaxosLogClient logClient = new PaxosLogClient(
                "127.0.0.1",
                3060,
                allReplicasInfo,
                0.0);

        logClient.start();
    }

}
