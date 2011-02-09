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
public class graphgen {
    
public static int make_triplets(String filename) throws Exception{
        
        FileInputStream fstream = new FileInputStream(filename+".txt");
        DataInputStream in = new DataInputStream(fstream);
        
        BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
        
        int nodeoffset = 0;
        int lastnode = 0;
        int prevcharint=0;
        int lastregcharint=0;
        String line;
        String lastword="a",previousword = "a";
        int numnodes = 0;
        int MAX_NODES = 900000;
        ArrayList<ArrayList<Integer>> Successors = new ArrayList<ArrayList<Integer>>();
        
        ArrayList<ArrayList<Integer>> flags = new ArrayList<ArrayList<Integer>>();
        Successors.add(new ArrayList<Integer>());
        flags.add(new ArrayList<Integer>());
        //System.out.println("The outputs are "+Successors.get(0).get(1));
        
        int charint;
        int linelength;
        
        outerloop:
        while((line= br.readLine())!= null){
            // The below code is only for A, as A is being coded in 2 files.. Comment it out in other cases
            line = line.trim();
            //line = line.substring(1, line.length());
            if ( (int)line.charAt(0)!=prevcharint){
                lastregcharint = prevcharint;
                lastnode = numnodes;
                lastword = previousword;
            }
            prevcharint = (int)line.charAt(0);
            
            if (numnodes>MAX_NODES){//add things to be done at this point here
                System.out.println("See the numnodes is "+numnodes+" at word "+line);
                System.out.println(+MAX_NODES+" nodes exceeded, so falling back to "+lastregcharint+"and the lastword is "+lastword);
                if (lastregcharint==0) return 0;
                else
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
                    if (pos1!=-1) pos = pos1;
                    else pos=pos2;
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
                        curr = numnodes;
                    }
                    catch(OutOfMemoryError E){// Put things similar to Max_NODES here
                        System.out.println("caught "+E.getMessage() );
                        System.out.println(" memory exceeded, so falling back to "+lastregcharint);
                        
                        if (lastregcharint==0) return 0;
                        else break outerloop;
                                           
                    }
                }
                else{
                    if (charint>96 & charint<123 & i==linelength-1)
                            flags.get(curr).set ( pos,charint-32 );
                    ArrayList<Integer> temp = Successors.get(curr);
                    curr = temp.get(pos);
                }
            }
            previousword = line;
        }
        
        
        
        if (lastregcharint==0)
            return 0;
        //System.out.println("The number of nodes is "+numnodes);
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File("A_"+String.valueOf(lastregcharint)+"_"+String.valueOf(nodeoffset)+".txt"), true));
        
        
        
        
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
        return lastregcharint;
        
        
        
    }
}