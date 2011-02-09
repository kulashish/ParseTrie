/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unimi.dsi.webgraph.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;

/**
 *
 * @author SCARS Lapi
 */
public class divide_db {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        // TODO code application logic here
        //for (int i=75;i<91;i++){
            FileInputStream fstream = new FileInputStream("TUV_db_2.txt");
            DataInputStream in = new DataInputStream(fstream);

            BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File("VWXY_db_2.txt"), true));
            String line = br.readLine();
            while(true){
                if (line.length()>1){
                    if(line.charAt(1)>95 & line.charAt(0)==86)
                        break;
                }
                //bw.write(line);
                //bw.newLine();
                line = br.readLine();
            }
            //bw.newLine();
            bw.write(line);
            while((line = br.readLine())!=null){
                bw.newLine();
                bw.write(line);
            }
            /*while(line.charAt(0)<=76){
                line = br.readLine();
            }
            bw.newLine();
            bw.write(line);
            while((line=br.readLine())!=null){
                bw.newLine();
                bw.write(line);
            }*/
            bw.close();
            System.out.println("Job Done");
        //}
    }
}
