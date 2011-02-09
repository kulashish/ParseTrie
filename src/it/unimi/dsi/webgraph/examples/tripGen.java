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
public class tripGen {
    
public static void make_triplets(String filename) throws Exception{
        
        FileInputStream fstream = new FileInputStream(filename+".txt");
        DataInputStream in = new DataInputStream(fstream);
        
        BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
        //BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filename+"_db.txt"), true));
        
        int nodeoffset = 10000;
        int lastend=10000;
        String line;
        while((line= br.readLine())!= null){
            int numnodes = 0;
            int MAX_NODES = 900000;
            int Triplets[][] = new int[MAX_NODES][26];
            //int Triplets[][] = new int[MAX_NODES][10000];
            int flag[] = new int[MAX_NODES];



            int a,b,c,linelength;

            while( ( line = br.readLine() ) != null ){
                if (numnodes > MAX_NODES){
                    System.out.println("Changing over to next set at numnode "+(numnodes+nodeoffset));
                    nodeoffset += numnodes;
                    break;
                }
                b = 0;
                line = line.trim();
                linelength = line.length();
                
                
                for (int i = 0; i< linelength;i++){
                    char ch = line.charAt(i);
                    a = (int)ch;
                    if ( a>96 & a<123)
                        a = a-97;
                    else if(a>64 & a<91)
                        a = a+32;
                    //else 
                    //    a = 0;
                    else if(a>47 & a < 58)
                        ;
                    else if (a==8242 || a==181 || a== 176 || a==240 ||a==230 ||a==618 || a==643 || a==189 || a==61 || a== 1082 || a==1091 || a== 1089 || a==39 || a==36 || a==38 || a==37 || a==948 || a== 8734 || a== 1073 || a==1075 || a==1088 || a==1092);
                    else if ( a==8722 || a==8212 || a== 178 || a==33 || a==95 || a==40 || a ==41 || a==45 || a==46 || a==34 || a==44 || a==8211 || a==58 || a==63 || a== 43 || a==42 || a==47)
                        a = 32;
                    else if (a == 238 || a==299)
                        a=8;
                    else if(a ==225 || a==228 || a==229 || a==224 || a==257 || a==260)
                        a=0;
                    else if (a==946 || a==223)
                        a=1;
                    else if (a==232 || a==277 || a==233 || a==949 || a==601 || a==281)
                        a=4;
                    else if (a==249 || a==363 || a==252)
                        a=20;
                    else if (a==333 || a==246 || a==243)
                        a=14;
                    else if (a==269)
                        a=2;
                    else if (a==241)
                        a=13;
                    else if (a==351 || a==347 || a==353)
                        a=18;
                    else if (a==322)
                        a=11;
                    else{
                        System.out.println("The character is "+ch+"and teh ASCII value is "+a+" in line no. "+nodeoffset+" where the word is " +line);
                        return;
                    }
                     
                
                    // flag = 0 represnts unvisited node
                    // flag = 1 represents visted node but not terminal for any word
                    // flag = 2 represenats visited terminal node
                    /*if (flag[Triplets[b][a]]==0){
                        numnodes++ ;
                        Triplets[b][a] = numnodes;
                        //System.out.println("i is "+i+" and linelength is "+linelength);
                        if(i ==linelength-1){
                            //System.out.println("reach 2");
                            flag[Triplets[b][a]] = 2;
                        }
                        else{
                            //System.out.print("line);
                            flag[Triplets[b][a]] = 1;

                        }
                        b = Triplets[b][a];
                    }
                    else if (flag[Triplets[b][a]] > 0){
                        if(i == linelength-1)
                            flag[Triplets[b][a]] = 2;
                        b = Triplets[b][a];
                    }
                    else
                        System.out.println("There is some problem, debug !!!!");
                }
            }
            //System.out.println("It points to the node "+Triplets[8][2]);
            //System.out.println("The flag value is "+flag[Triplets[8][2]]);
            a =0;b=0;c=0;

            for (int i=0 ; i<= numnodes ;i++){
                for (int j=0; j<10000; j++){
                    b = Triplets[i][j];
                    if (b==0)
                        continue;
                    a = flag[b];
                    if (a==2 & j>96 & j<123){
                        c = j-32;
                        //System.out.print("I am planting a flag at "+b+"\n");
                    }
                    else if(a ==1)
                        c = j;
                    /*else if(a == -1)
                        c = 0;*/
                    /*else
                        System.out.println("Some problem possibly with Flags");
                    bw.write(String.valueOf(i+nodeoffset)+"\t"+String.valueOf(b+nodeoffset)+"\t"+String.valueOf(c));
                    bw.newLine();
                    bw.write(String.valueOf(b+nodeoffset)+"\t"+String.valueOf(c)+"\t"+String.valueOf(c));
                    bw.newLine();
                    //bw.write(String.valueOf(b+52)+"\t"+String.valueOf(i+52)+"\t"+String.valueOf(c));
                    //bw.newLine();
                */}
            }
        }
        //bw.close();
    }
    
    //public static void printhello(){
    //    System.out.println("Hello there");
    //}
}

