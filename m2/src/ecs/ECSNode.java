package ecs;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ECSNode implements IECSNode{
   private String nodeName;
   private String nodeHost;
   private int nodePort;
   private String nodeHash;
   private ArrayList<String> nodeRange;
   
   //constructor
   public ECSNode(String nodeName, String nodeHost, int nodePort){
      this.nodeHost=nodeHost;
      this.nodePort=nodePort;
      this.nodeName=nodeName;
      String stringHash=nodeHost + ":" +Integer.toString(nodePort);
      this.nodeHash=convertToMD5(stringHash);
   }
   public String getNodeName(){
      return this.nodeName;
   }

   public String getNodeHash(){
      return this.nodeHash;
   }
   public String getNodeHost(){
      return this.nodeHost;
   }
   public int getNodePort(){
      return this.nodePort;
   }
   public void setRange(ArrayList<String>range){
      nodeRange=range;
   }
   public String[] getNodeHashRange(){
      return nodeRange.toArray(new String[nodeRange.size()]);
   }
   public String convertToMD5(String md5){
      try{
	 MessageDigest md=MessageDigest.getInstance("MD5");
	 byte[]array=md.digest(md5.getBytes());
	 StringBuffer sb=new StringBuffer();
	 for (int i=0;i<array.length;++i){
	    sb.append(Integer.toHexString((array[i]&0xFF)|0x100).substring(1,3));
	 }
	 return sb.toString();
      }catch (NoSuchAlgorithmException e){
      }
      return null;
   }
}
