package util;

import java.io.*;

public class LogWriter {

    private final int serverId;


    public LogWriter(int serverId) {
        this.serverId = serverId;
    }

    public void write(final String outputString) {
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(
                    new OutputStreamWriter(
                            new FileOutputStream("replica" + serverId + ".log", true)));
            bufferedWriter.write(outputString);
            bufferedWriter.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
