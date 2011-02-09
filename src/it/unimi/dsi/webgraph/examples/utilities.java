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
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 *
 * @author SCARS Lapi
 */
public class utilities {
    
    
    public static void find_same_sds(String filename) throws FileNotFoundException, IOException{
        FileInputStream fstream = new FileInputStream(filename+".txt");
        DataInputStream in = new DataInputStream(fstream);
        
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        String source="";
        int count = 0;
        int outlinks=0;
        int maxout=0;
        while((line=br.readLine())!=null){
            String p[] = line.split("\t");
            if(p.length==5){
                //System.out.println("It came here");
                source = p[0];
                outlinks=0;
                if(source.equals(p[2]))
                    System.out.println("source same as destination at "+count);
                outlinks++;
            }
            if(p.length==3){
                //if(source.equals(p[0]))
                //    System.out.println("source same as destination at "+count);
                outlinks++;
                if(outlinks>5000){
                    System.out.println("7000 nodes exceeded, source is "+source);
                }
                if(outlinks>maxout)
                    maxout = outlinks;
            }
            count++;
            if(count%5000000==0)
                System.out.println("Crossed "+count);
        }
    
    
    }
    
    
    public static void merge_ids() throws FileNotFoundException, IOException{
        FileInputStream fstream = new FileInputStream("PageLinks.txt");
        DataInputStream in = new DataInputStream(fstream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        
        FileInputStream f2stream = new FileInputStream("PageLinks2.txt");
        DataInputStream in2 = new DataInputStream(f2stream);
        BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));
        
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File("PageLinks_combined.txt"), false));
        
        String line;
        int max_id=-1;
        int id, count=0;
        while((line=br.readLine())!=null){
            String p[] = line.split("\t");
            if(p.length==5){
                id = Integer.parseInt(p[1]);
                max_id=id;
            }
            bw.write(line);
            bw.newLine();
            bw.flush();
            count++;
            if(count%5000000==0)
                System.out.println("Crossed "+count);
        }
        System.out.println("Shifted over to second file");
 
        while((line=br2.readLine())!=null){
            String p[] = line.split("\t");
            if(p.length==5){
                id = Integer.parseInt(p[1]);
                if (max_id<id)
                    break;
            }
            else
                continue;
        }
        System.out.println("Crossed the repitition zone");
        bw.write(line);
        bw.newLine();
        while((line=br2.readLine())!=null){
            bw.write(line);
            bw.newLine();
            bw.flush();
            count++;
            if(count%5000000==0)
                System.out.println("Crossed "+count);
        }
        bw.close();
    }
    
    
    
    public static void main(String args[]) throws FileNotFoundException, IOException{
        //merge_ids();
        String filename = "PageLinks_combined";
        find_same_sds(filename);
        //File f = new File("a_names.txt");
        
        //f.renameTo(new File("a_name2.txt"));
    
    }
}
