/* This class takes in a sorted Database assumed to be in a .txt file, the name of which is passed as the parameter filename,
 * and writes down the arcs as tab-separated triplets in the form of text files the names of which begin with db_ and are 
 * followed by the distinct first letters encoded in one file follwed by an "_" and then the ascii value of the second 
 * character of the encoded word. The reason is that I have made sure that all words having the same first 2 letters same are found
 * in the same file. This file may contain words starting with different characters, or having same first character but a different
 * second character, but in no case are 2 words having the same first 2 characters found in 2 different files. For example a file named
 * db_ABC_99.txt will have some words starting with A or in some cases even the words starting with a character of ascii value less than 65
 * , the whole of B, and all the words that start with C and have the ascii value of their second character less than or equal to 99(c).  
 * 
 * A log.txt file is also written and it carries the names and information about the ending characters of the encoding for a
 * particular file. This information is used by the searching program to find the appropriate graph files to search a paricular file
 * 
 * These triplets respresent in order, the source, the destination and the ascii value of the label attached with the arc. The encoding
 * has been done in a way that at any place the appearance of a capital letter signifies the end of a title in the database. Similary, a leaf 
 * node should also be considered as the end of a title in the database.
 * 
 * These arcs are used for creating the prefix tree. The structure of the prefic tree finally created is that out of it's successors, the 
 * first one is always the parent of the node, and then it's children as the remaining successors.
 * 
 * 
 * First of all, this creates two arraylists, 1. Successors: for storing the successors of a particular node
 * and 2. Flags: for storing the ascii value of the labels attached with the the corresponding successor. These two lists will always have the same 
 * dimension at any given time.
 * 
 * 
 * 
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
public class labelled_graphgen {
    
/*
     * filename is the basename of the file (without the .txt extension) in which the sorted database with each word 
     * in a different line has been written.
     * firstchar is the ascii value of the first character of the last word that has already been encoded, similarly 
     * secondchar is the ascii value of the second character of the last word that has already been encoded
     * We only need to skip these many and start with the next word that has at least one of the two things different.
     *
     * MAX_NODES is a tunable variable. I have tuned it to 2200000 such that this does not give out of memory error on my system.
     * This number should be changed experimentally for different systems and the variation should be based on the RAM size
     */
    
public static int[] make_triplets(String filename, int firstchar, int secondchar) throws Exception{
        
        FileInputStream fstream = new FileInputStream(filename+".txt");
        DataInputStream in = new DataInputStream(fstream);
        
        BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
        int lastnode = 0;
        int prevcharint=0;
        int prevchar2 = 0;
        int lastregchar2 = 0;
        int lastregcharint=0;
        String pInfo[]=new String[2];
        String lastword="a",previousword = "a";
        int[] endchars = new int[3];
        endchars[2] = 0;
        
        String basename="";
        int numnodes = 0;
        int MAX_NODES = 2200000;// Please tune this parameter to suit your RAM requirements.
        // You can possibly try different values and see if where it actually runs out of memory
        
        ArrayList<ArrayList<Integer>> Successors = new ArrayList<ArrayList<Integer>>();
        
        ArrayList<ArrayList<Integer>> flags = new ArrayList<ArrayList<Integer>>();
        Successors.add(new ArrayList<Integer>());
        flags.add(new ArrayList<Integer>());
                
        int charint;
        int linelength;
        
        String inLine = br.readLine();
        pInfo=inLine.split("\t");
        
        //Skipping the titles already encoded
        while((int)pInfo[0].charAt(0)<firstchar){
        	inLine= br.readLine();
        	pInfo=inLine.split("\t");
        }
        int currSecondChar=secondchar;
        if((int)pInfo[0].charAt(0)==firstchar){
            if (pInfo[0].length()>1){
                while((int)pInfo[0].charAt(1)<=secondchar)
                {
                	inLine= br.readLine();
                	pInfo=inLine.split("\t");
                }
            }
            else if (secondchar>0){
            	inLine= br.readLine();
            	pInfo=inLine.split("\t");
            	if(pInfo[0].length()>1)currSecondChar=(int)pInfo[0].charAt(1);
                while(currSecondChar<=secondchar)
                {
                	inLine= br.readLine();
                	pInfo=inLine.split("\t");
                	if(pInfo[0].length()>1)currSecondChar=(int)pInfo[0].charAt(1);
                }
            }
        }
        
        
        System.out.println("Now starting to code from "+pInfo[0]+" onwards");
        outerloop:
        while(true){
            pInfo[0] = pInfo[0].trim();// to remove unnecssary white spaces from the beginneing and the end
            //line = line.substring(1, line.length());
            int firstmatch = 0;
            
            if ( (int)pInfo[0].charAt(0)!=prevcharint){
                lastregcharint = prevcharint;//the first character of the word to be encoded last tentatively
                lastnode = numnodes;
                lastword = previousword;
                if ((int)pInfo[0].charAt(0)>64 & (int)pInfo[0].charAt(0)<91)
                    basename = basename + Character.toString(pInfo[0].charAt(0));
                if(pInfo[0].length()>1)
                    prevchar2 = (int)pInfo[0].charAt(1);
                else
                    prevchar2 = 0;
            }
            else{
                if (pInfo[0].length()>1)
                    firstmatch = (int)pInfo[0].charAt(1);
                if (firstmatch!=prevchar2){
                    lastregcharint = prevcharint;
                    lastregchar2 = prevchar2;
                    lastnode = numnodes;
                    lastword = previousword;
                }
            }
            prevcharint = (int)pInfo[0].charAt(0);
            if(pInfo[0].length()>1)
                prevchar2 = (int)pInfo[0].charAt(1);
            else
                prevchar2 = 0;
            
            
            if (numnodes>MAX_NODES){//add things to be done at this point here
                System.out.println("The numnodes is "+numnodes+" at word "+pInfo[0]);
                System.out.println(+MAX_NODES+" nodes exceeded, so falling back to "+lastregcharint+"and"+lastregchar2+" the lastword is "+lastword+". Right now the lastnode is "+lastnode);
                    break;
                
            }
            
            linelength = pInfo[0].length();
            int curr=0;
            for(int i=0 ; i<linelength ; i++){
                charint = pInfo[0].charAt(i);
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
          //      if (charint>nodeoffset)
          //          nodeoffset = charint;
                //int pos = flags.get(curr).indexOf(charint);
                if (pos ==-1){
                    try{
                        Successors.add(new ArrayList<Integer>());
                        flags.add(new ArrayList<Integer>());
                        
                        numnodes++;
                        
                        Successors.get(curr).add(numnodes);
                        if (i==linelength-1)
                        {
                            flags.get(curr).add(charint-32);
                            Successors.add(new ArrayList<Integer>());
                            flags.add(new ArrayList<Integer>());
                            numnodes++;
                    		Successors.get(numnodes-1).add(numnodes);
                    		flags.get(numnodes-1).add(Integer.parseInt(pInfo[1]));
                        }
                        else 
                            flags.get(curr).add(charint);
                        flags.get(curr).trimToSize();
                        Successors.get(curr).trimToSize();
                        if(i==linelength-1)
                        	curr=numnodes-1;
                        else
                        	curr=numnodes;
                    }
                    catch(OutOfMemoryError E){// Put things similar to Max_NODES here
                        System.out.println("caught "+E.getMessage() );
                        System.out.println(" memory exceeded, so falling back to "+lastregcharint+"and the lastword is "+lastword);
                        
                        //if (lastregcharint==0) return 0;
                         break outerloop;
                                           
                    }
                }
                
                else{
                	if(i==linelength-1)
                	{
                		numnodes++;
                		Successors.add(new ArrayList<Integer>());
                        flags.add(new ArrayList<Integer>());
                		Successors.get(pos).add(numnodes);
                		flags.get(pos).add(Integer.parseInt(pInfo[1]));
                	}
                    curr = Successors.get(curr).get(pos);
                    
                }
                
            }
            previousword = pInfo[0];
            if ((pInfo[0]= br.readLine())== null)
                break;
            else{
            	pInfo=pInfo[0].split("\t");
            }
        }
        if((pInfo[0]=br.readLine())==null){
            lastnode = numnodes;
            endchars[2] = 1;
        }
        
        /*
         This commented out portion below shows how you can actually look for a word in the above 2 lists. Hoever, it is 
         * unnecessary for the final task. It is only for an understanding of how the above 2 lists store the information 
         * and was used by me primarily for the purpose of debugging
         
         */
        
        
        /*String S = "Lá»¥c_bÃ¡t";
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
        }*/
        
        
        
        
        String newfile = "db_"+basename+"_"+String.valueOf(lastregchar2)+".txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(newfile), true));
        BufferedWriter logwriter = new BufferedWriter(new FileWriter(new File("log.txt"), true));
        logwriter.write(newfile+"\t"+lastregcharint+"\t"+lastregchar2);
        logwriter.newLine();
        //logwriter.write("The first character of the last word coded is "+lastregcharint+" and the 2nd last character is "+lastregchar2);
        //logwriter.newLine();
        logwriter.close();
        System.out.println("The number of nodes used is "+lastnode);
        for(int i=0;i<lastnode;i++){
            for(int j=0 ; j<Successors.get(i).size() ; j++){
                bw.write(String.valueOf(i)+"\t" +String.valueOf(Successors.get(i).get(j)) +"\t"+String.valueOf(flags.get(i).get(j)));
                bw.newLine();
                bw.write(String.valueOf(Successors.get(i).get(j)) +"\t"+String.valueOf(i)+"\t" +String.valueOf(flags.get(i).get(j)));
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