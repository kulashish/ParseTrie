/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unimi.dsi.webgraph.examples;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 *
 * @author SCARS Lapi
 */
public class biGraphGen {
    public static void graphgen(String filename ) throws Exception{
        int numnodes = 150;
        int MAX_NODES = 500000;
        int Triplets[][] = new int[MAX_NODES][26];
        int flag[] = new int[MAX_NODES];
        
        FileInputStream fstream = new FileInputStream(filename+".txt");
        DataInputStream in = new DataInputStream(fstream);
        
        BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
        
        String line;
        int a,b,c;
        
        while( ( line = br.readLine() ) != null & numnodes < MAX_NODES - 26){
            b = 150;
            for (int i = 0; i< line.length();i++){
                a = (int)line.charAt(i);
                if ( a>96 & a<123)
                    a = a-97;
                else if(a>64 & a<91)
                    a = a-65;
                else
                    break;
                // flag = 0 represnts unvisited node
                // flag = 1 represents visted node but not terminal for any word
                // flag = 2 represenats visited terminal node
                if (flag[Triplets[b][a]]==0){
                    numnodes++ ;
                    Triplets[b][a] = numnodes;
                    if(i ==line.length()-1){
                        //System.out.println("reach 2");
                        flag[Triplets[b][a]] = 2;}
                    else{
                        //System.out.print("reach 1");
                        flag[Triplets[b][a]] = 1;}
                    b = Triplets[b][a];
                    /*for(int j = 1;j<=26;j++){
                        flag[numnodes+j] = -1;
                        Triplets[b][j-1] = numnodes+j;
                    }*/
                    
                }
                else if (flag[Triplets[b][a]] > 0){
                    if(i == line.length()-1)
                        flag[Triplets[b][a]] = 2;
                    b = Triplets[b][a];
                }
                else
                    System.out.println("There is some problem, debug !!!!");
            }
        }
        //System.out.println("It points to the node "+Triplets[8][2]);
        //System.out.println("The flag value is "+flag[Triplets[8][2]]);
        /*a =0;b=0;c=0;
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filename+"_db.txt"), true));
        for (int i=0 ; i<= numnodes ;i++){
            for (int j=0; j<26; j++){
                b = Triplets[i][j];
                if (b==0)
                    continue;
                a = flag[Triplets[i][j]];
                if (a==2)
                    c = j+65;
                else if(a ==1)
                    c = j+97;
                else if(a == -1)
                    c = 0;
                else
                    System.out.println("Some problem possibly with Flags");
                bw.write(String.valueOf(i)+"\t"+String.valueOf(b)+"\t"+String.valueOf(c));
                bw.newLine();
            }
        }
        bw.close();
    }
    
    public static void printhello(){
        System.out.println("Hello there");*/
    } 
}
