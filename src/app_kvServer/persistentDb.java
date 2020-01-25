package app_kvServer;

import java.io.*;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class persistentDb {

    public static PrintWriter writer = null;

    public static FileWriter fw = null;
    public static BufferedWriter bw = null;
    public static PrintWriter pw = null;

    private static Logger logger = Logger.getRootLogger();

    /*
    * Instantiates the database locally by creating the file
    */
    public static void initializeDb() {
        try {
            writer = new PrintWriter("persistentDb.txt", "UTF-8");
            writer.close();
        } catch (FileNotFoundException e) {
            logger.error("Error! Cannot open file persistentDb.txt"); 
        } catch (UnsupportedEncodingException e) {
            logger.error("Error! UTF-8 is unsupported for writing to file"); 
        }
    }

    public static String find(String key) {
        try {
            File file = new File("persistentDb.txt");
            BufferedReader br = new BufferedReader(new FileReader(file)); 

            String st; 
            while ((st = br.readLine()) != null) {
                String[] tokens = st.split(" ");
                if(tokens[0].equals(key)) {
                    String value = "";
                    // skip key and colon
                    for(int i = 2; i < tokens.length; i++) {
                        value += tokens[i];
                    }
                    return value;
                }
            }
            br.close();
        } catch (FileNotFoundException e) {
            logger.error("Error! Cannot open file persistentDb.txt"); 
        } catch (IOException e) {
            logger.error("Error! Cannot read from file persistentDb.txt"); 
        }
        return null;
    }

    public static void add(String key, String value) {
        if (key.isEmpty() || key == null) {
            // invalid key
            logger.error("Error: Empty key not allowed.");
            return;
        }
        if (value.isEmpty() || value.equals("null") || value == null) {
            // delete operation
            deleteLine(key);
            return;
        }
        if (find(key) != null) {
            // modify key operation
            deleteLine(key);
        }

        try {
            FileWriter fw = new FileWriter("persistentDb.txt", true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter pw = new PrintWriter(bw);

            pw.println(key + " : " + value);

            pw.flush();
            pw.close();
            bw.close();
            fw.close();
        } catch (IOException e) {
            logger.error("Error! Cannot open file persistentDb.txt"); 
        }
    }

    private static void deleteLine(String key) {
        try {
            File file = new File("persistentDb.txt");
            BufferedReader br = new BufferedReader(new FileReader(file)); 
            StringBuffer sb = new StringBuffer("");

            String st; 
            while ((st = br.readLine()) != null) {
                String[] tokens = st.split(" ");
                if (!tokens[0].equals(key)) {
                    sb.append(st + "\n");
                }
            }

            br.close();
            FileWriter fw = new FileWriter(new File("persistentDb.txt"));
			//Write entire string buffer into the file
			fw.write(sb.toString());
			fw.close();
        } catch (FileNotFoundException e) {
            logger.error("Error! Cannot open file persistentDb.txt"); 
        } catch (IOException e) {
            logger.error("Error! Cannot read from file persistentDb.txt"); 
        }
    }

    public static void clearDb() {
        File file = new File("persistentDb.txt");
        try {
            file.delete();
        } catch (IOException e) {
            logger.error("Error! Cannot delete file persistentDb.txt"); 
        }
    }
}