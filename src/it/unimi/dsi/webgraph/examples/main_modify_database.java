/*This is a single main file to make modifications in the database such that the variants of a are converted to the base letters.
 *This function reads the Database from a file named "Database.txt" in which the pagetitles are written in successive lines.
 * This data has to be further sorted for which I have used a c++ file named a.cpp. Details to give input and take output from 
 * that file can be found in the text file named "ReadMe.txt"
 * 
 */
package it.unimi.dsi.webgraph.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;

/**
 *
 * @author SCARS Lapi
 */
public class main_modify_database {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        FileInputStream fstream = new FileInputStream("Database.txt");
        DataInputStream in = new DataInputStream(fstream);
        
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File("Database_modified.txt"),true));
        String line;
        int charint;
        while((line=br.readLine())!=null){
        	String parts[]=line.split("\t");//split pageId and title
        	line=parts[0];
        	
            int linelength = line.length();
            if (linelength>1){
                for(int i=0;i<linelength;i++){
                    //int a=0;
                    charint = (int)line.charAt(i);
                    //Handling variants of A and a 
                    if( (charint >=192 & charint<=197) || (charint>=225 & charint <=229) || (charint>=256 & charint<=261) || charint==478 || charint==479 || charint==506 ||charint==507)
                        charint = 97;

                    // Handling variants of B
                    else if(charint==7682 || charint==7683)
                        charint=98;
                    // Handling C variants
                    else if((charint>=262 &charint<=269) || charint==199 || charint==231)
                        charint = 99;
                    // Handling D variants
                    else if((charint>=270 &charint<=273) || charint==7696 || charint==7697 || charint==7690 || charint==7691 || charint==208 || charint==240)
                        charint= 100;

                    // Handling E variants
                    else if((charint>=200 & charint<=203)||(charint>=232 & charint<=235) || (charint>=274 & charint<=283) )
                        charint = 101;
                    // Handling variants of F
                    else if((charint >=64256 & charint<=64261) || charint ==7710 || charint==7711 || charint == 402)
                        charint = 102;
                    //Handling variants of G
                    else if ((charint>=284 & charint<=291) || (charint>=484 & charint <=487) || charint==500 || charint==501)
                        charint = 103;
                    //Handling variants of H
                    else if((charint>=292 & charint<=295))
                        charint = 104;
                    //Handling I variants
                    else if((charint>=204 & charint<=207) || (charint>=236 & charint<=239) || (charint>=296 & charint<=305)){
                    //    System.out.println("The word is "+line+" and the charint is "+charint);
                        charint = 105;
                        //return;
                    }
                    //Handling variants of J
                    else if (charint==308 || charint==309)
                        charint = 106;
                    //Handling K variants
                    else if (charint==310 || charint==311 || charint == 312 || charint ==7728 || charint == 7729|| charint ==488 || charint==489)
                        charint = 107;
                    //Handling L variants
                    else if ((charint>=313 & charint<=322))
                        charint = 108;
                    //M
                    else if (charint== 7744|| charint==7745)
                        charint = 109;
                    // N
                    else if ((charint>=323 & charint<=331) || charint==209 || charint==241)
                        charint = 110;
                    //O
                    else if ((charint>=210 & charint<=216) || (charint >=242 & charint <=248) || (charint>=332 & charint<=337) || charint== 511|| charint==510)
                        charint = 111;

                    //P
                    else if (charint==7766 || charint==7767)
                        charint = 112;
                    //R
                    else if (charint >=340 & charint <=345)
                        charint = 114;
                    //S
                    else if((charint>=346 &charint <=353) || charint==7776 || charint==7777)
                        charint = 115;

                    //T
                    else if ((charint>= 354 & charint<=359) || charint==7786 || charint == 7787)
                        charint=116;
                    //U
                    else if((charint>=217 & charint<=220) || (charint>=249 & charint<=252) || (charint>=360 & charint<=371))
                        charint = 117;
                    //W
                    else if ((charint>=7808 & charint<=7813) || charint==372 || charint==373)
                        charint = 119;
                    //Y
                    else if (charint==7922 || charint==7923 || charint==221 || charint==253 || charint==374 || charint==375 || charint==159 || charint==255)
                        charint=121;
                    //Z
                    else if(charint>=377 & charint<=382)
                        charint = 122;
                    //System.out.println("The word here is "+line);
                    if(charint>=97 & charint<=122 & i==0)
                        charint = charint-32;
                    else if(charint>=65 & charint<=90 & i>0)
                        charint +=32;
                    
                    line = line.substring(0,i)+Character.toString((char)charint)+line.substring(i+1);
                    //System.out.println("The word after correction is "+line);
                    //Handling cases of two letterd characters
                    if(charint==198 || charint ==230 || charint== 508 || charint==509){
                        charint = 97;
                        System.out.println("Got the word"+line);
                        if(charint>=97 & charint<=122 & i==0)
                            charint = charint-32;
                        
                        line = line.substring(0,i)+Character.toString((char)charint)+Character.toString((char)101)+line.substring(i+1);
                        linelength = line.length();
                        //System.out.println("Changed it "+line+" and at next iteration it should find "+line.charAt(i+1));
                    }
                    else if (charint == 223){
                        charint = 115;
                        //System.out.println("Got the word"+line);
                        if(charint>=97 & charint<=122 & i==0)
                            charint = charint-32;
                        
                        line = line.substring(0,i)+Character.toString((char)charint)+Character.toString((char)115)+line.substring(i+1);
                        linelength = line.length();
                        //System.out.println("Changed it "+line+" and at next iteration it should find "+line.charAt(i+1));
                    }
                    else if (charint == 338|| charint ==339){
                        charint = 111;
                        //System.out.println("Got the word"+line);
                        if(charint>=97 & charint<=122 & i==0)
                            charint = charint-32;
                       
                        line = line.substring(0,i)+Character.toString((char)charint)+Character.toString((char)101)+line.substring(i+1);
                        linelength = line.length();
                        //System.out.println("Changed it "+line+" and at next iteration it should find "+line.charAt(i+1));
                    }
                    else if(charint== 497 || charint==498 || charint== 499 || charint==452 || charint==453 || charint==454){
                        charint = 100;
                        System.out.println("Got the word "+line);
                        if(charint>=97 & charint<=122 & i==0)
                            charint = charint-32;
                        line = line.substring(0,i)+Character.toString((char)charint)+Character.toString((char)122)+line.substring(i+1);
                        linelength = line.length();
                        //System.out.println("Changed it to "+line+" and at next iteration it should find "+line.charAt(i+1));
                    }

                }

                
            }
            //System.out.print("The word is "+line);
            bw.write(line+"\t"+parts[1]);
            bw.newLine();
            bw.flush();
        }
        bw.close();
    }
}
