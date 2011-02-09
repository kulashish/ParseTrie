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
import java.util.ArrayList;
//import java.lang.Object;
/**
 *
 * @author SCARS Lapi
 */
public class big_graphgen {
    
public static int[] make_triplets(String filename, int firstchar, int secondchar) throws Exception{
        
        FileInputStream fstream = new FileInputStream(filename+".txt");
        DataInputStream in = new DataInputStream(fstream);
        
        BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
        
        int nodeoffset = 0;
        int lastnode = 0;
        int prevcharint=0;
        int prevchar2 = 0;
        int lastregchar2 = 0;
        int lastregcharint=0;
        String line;
        String lastword="a",previousword = "a";
        int[] endchars = new int[3];
        endchars[2] = 0;
        
        String basename="";
        int numnodes = 0;
        int MAX_NODES = 2200000;
        ArrayList<ArrayList<Integer>> Successors = new ArrayList<ArrayList<Integer>>();
        
        ArrayList<ArrayList<Integer>> flags = new ArrayList<ArrayList<Integer>>();
        Successors.add(new ArrayList<Integer>());
        flags.add(new ArrayList<Integer>());
        //System.out.println("The outputs are "+Successors.get(0).get(1));
        
        int charint;
        int linelength;
        
        line = br.readLine();
        
        while((int)line.charAt(0)<firstchar){
            line = br.readLine();
        }
        if((int)line.charAt(0)==firstchar){
            if (line.length()>1)
                while((int)line.charAt(1)<=secondchar)
                    line = br.readLine();
            else if (secondchar>0){
                line = br.readLine();
                while((int)line.charAt(1)<=secondchar)
                    line = br.readLine();
            }
        }
        System.out.println("Now starting to code from "+line+" onwards");
        outerloop:
        while(true){
            line = line.trim();
            //line = line.substring(1, line.length());
            int firstmatch = 0;
            
            if ( (int)line.charAt(0)!=prevcharint){
                lastregcharint = prevcharint;
                lastnode = numnodes;
                lastword = previousword;
                if ((int)line.charAt(0)>64 & (int)line.charAt(0)<91)
                    basename = basename + Character.toString(line.charAt(0));
                if(line.length()>1)
                    prevchar2 = (int)line.charAt(1);
                else
                    prevchar2 = 0;
            }
            else{
                if (line.length()>1)
                    firstmatch = (int)line.charAt(1);
                if (firstmatch!=prevchar2){
                    lastregcharint = prevcharint;
                    lastregchar2 = prevchar2;
                    lastnode = numnodes;
                    lastword = previousword;
                }
            }
            prevcharint = (int)line.charAt(0);
            if(line.length()>1)
                prevchar2 = (int)line.charAt(1);
            else
                prevchar2 = 0;
            
            
            if (numnodes>MAX_NODES){//add things to be done at this point here
                System.out.println("The numnodes is "+numnodes+" at word "+line);
                System.out.println(+MAX_NODES+" nodes exceeded, so falling back to "+lastregcharint+"and"+lastregchar2+" the lastword is "+lastword+". Right now the lastnode is "+lastnode);
                    break;
                
            }
            
            linelength = line.length();
            int curr=0;
            for(int i=0 ; i<linelength ; i++){
                charint = line.charAt(i);
                int pos,pos1,pos2;
                
                
                
                if (charint>64 & charint<91){
                    charint = charint+32;
                    pos1 = flags.get(curr).indexOf(charint-32);
                    pos2 = flags.get(curr).indexOf(charint);
                    if (pos1!=-1) pos = pos1;
                    else pos=pos2;
                }
                else if (charint >96 & charint < 123){
                    pos1 = flags.get(curr).indexOf(charint-32);
                    pos2 = flags.get(curr).indexOf(charint);
                    if (pos1!=-1) 
                        pos = pos1;
                    else 
                        pos = pos2;
                }
                else
                    pos = flags.get(curr).indexOf(charint);
                if (charint>nodeoffset)
                    nodeoffset = charint;
                //int pos = flags.get(curr).indexOf(charint);
                if (pos ==-1){
                    try{
                        Successors.add(new ArrayList<Integer>());
                        flags.add(new ArrayList<Integer>());
                        
                        numnodes++;
                        
                        Successors.get(curr).add(numnodes);
                        if (charint>96 & charint<123 & i==linelength-1)
                            flags.get(curr).add(charint-32);
                        else 
                            flags.get(curr).add(charint);
                        flags.get(curr).trimToSize();
                        Successors.get(curr).trimToSize();
                        curr = numnodes;
                    }
                    catch(OutOfMemoryError E){// Put things similar to Max_NODES here
                        System.out.println("caught "+E.getMessage() );
                        System.out.println(" memory exceeded, so falling back to "+lastregcharint+"and the lastword is "+lastword);
                        
                        //if (lastregcharint==0) return 0;
                         break outerloop;
                                           
                    }
                }
                
                else{
                    if (charint>96 & charint<123 & i==linelength-1)
                            flags.get(curr).set ( pos,charint-32 );
                    //ArrayList<Integer> temp = Successors.get(curr);
                    curr = Successors.get(curr).get(pos);
                    //System.out.println("The value of curr is "+curr);
                    //System.out.println("We are looking for "+i+"th character of "+line+". the size of temp is "+temp.size()+"whereas that of flags is "+flags.get(curr).size());
                    
                }
                
            }
            previousword = line;
            if ((line= br.readLine())== null)
                break;
        }
        if((line=br.readLine())==null){
            lastnode = numnodes;
            endchars[2] = 1;
        }
        
        
        
        
        String S = "Lục_bát";
        int curr = 0;
        int pos,pos1,pos2;
        for(int i=0;i<S.length();i++){
            charint = (int)S.charAt(i);
            if (charint>64 & charint<91){
                    charint = charint+32;
                    pos1 = flags.get(curr).indexOf(charint-32);
                    pos2 = flags.get(curr).indexOf(charint);
                    if (pos1!=-1) pos = pos1;
                    else pos=pos2;
                }
            else if (charint >96 & charint < 123){
                    pos1 = flags.get(curr).indexOf(charint-32);
                    pos2 = flags.get(curr).indexOf(charint);
                    if (pos1!=-1) 
                        pos = pos1;
                    else 
                        pos = pos2;
            }
            else
                pos = flags.get(curr).indexOf(charint);
            if(pos==-1){
                System.out.println("More debugging !!!!!");
                break;
            }
            curr = Successors.get(curr).get(pos);
            System.out.println("The value of next "+charint+" is "+curr);
        }
        
        
        
        //if (lastregcharint==0)
        //    return 0;
        //System.out.println("The number of nodes is "+numnodes);
        String newfile = "db_"+basename+"_"+String.valueOf(lastregchar2)+"_"+String.valueOf(nodeoffset)+".txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(newfile), false));
        BufferedWriter logwriter = new BufferedWriter(new FileWriter(new File("log.txt"), false));
        logwriter.write(newfile+"\t"+lastregcharint+"\t"+lastregchar2);
        logwriter.newLine();
        //logwriter.write("The first character of the last word coded is "+lastregcharint+" and the 2nd last character is "+lastregchar2);
        //logwriter.newLine();
        logwriter.close();
        System.out.println("The number of nodes used is "+lastnode);
        for(int i=0;i<lastnode;i++){
            //System.out.println("In "+i+"th iteration, the size is "+Successors.get(i).size());
            for(int j=0 ; j<Successors.get(i).size() ; j++){
                //System.out.println("It came here");
                bw.write(String.valueOf(i+nodeoffset+1)+"\t" +String.valueOf(Successors.get(i).get(j)+nodeoffset+1) +"\t"+String.valueOf(flags.get(i).get(j)));
                bw.newLine();
                bw.write(String.valueOf(Successors.get(i).get(j)+nodeoffset+1) +"\t"+String.valueOf(i+nodeoffset+1)+"\t" +String.valueOf(flags.get(i).get(j)));
                bw.newLine();
                bw.write(String.valueOf(Successors.get(i).get(j)+nodeoffset+1) +"\t"+String.valueOf(flags.get(i).get(j))+"\t"+String.valueOf(flags.get(i).get(j)));
                bw.newLine();
                bw.flush();
            }
            Successors.get(i).clear();
        
        }
        
        endchars[0] = lastregcharint;
        endchars[1] = lastregchar2;
        return endchars;
        
        
    }
}