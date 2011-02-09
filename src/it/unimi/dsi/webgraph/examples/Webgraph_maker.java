package it.unimi.dsi.webgraph.examples;

/*		 
 * Copyright (C) 2007-2011 Paolo Boldi and Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.webgraph.AbstractLazyIntIterator;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableSequentialGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.GammaCodedIntLabel;
import it.unimi.dsi.webgraph.labelling.Label;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;

import java.io.BufferedReader;
//import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.NoSuchElementException;

import com.martiansoftware.jsap.JSAP;
//import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;
import it.unimi.dsi.webgraph.ImmutableGraph;
//import it.unimi.dsi.webgraph.labelling.GammaCodedIntLabel;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/** A class exposing a list of triples as an {@link ArcLabelledImmutableGraph}. The triples are
 * interpreted as labelled arcs: the first element is the source, the second element is the target,
 * and the third element must be a nonnegative integer that will be saved using a {@link GammaCodedIntLabel}.
 * 
 * <p>This class is mainly a useful example of how to expose of your data <i>via</i> an {@link ArcLabelledImmutableGraph}, and
 * it is also used to build test cases, but it is not efficient or particularly refined.
 * 
 * <p>A main method reads from a text file,  a list of TAB-separated triples and writes the corresponding graph
 * using {@link BVGraph} and {@link BitStreamArcLabelledImmutableGraph}.
 */

public class Webgraph_maker extends ArcLabelledImmutableSequentialGraph {
	/** The list of triples. */
	final private int[][] triple;
	/** The prototype of the labels used by this class. */
	final private GammaCodedIntLabel prototype;
	/** The number of nodes, computed at construction time by triple inspection. */
	final private int n;
	
	/** Creates a new arc-labelled immutable graph using a specified list of triples.
	 * 
	 * <p>Note that it is impossible to specify isolated nodes with indices larger than
	 * the largest node with positive indegree or outdegree, as the number of nodes is computed
	 * by maximising over all indices in <code>triple</code>. 
	 * 
	 * @param triple a list of triples specifying labelled arcs (see the {@linkplain IntegerTriplesArcLabelledImmutableGraph class documentation});
	 * order is not relevant, but multiple arcs are not allowed.
	 */
	public Webgraph_maker( int[][] triple ) {
		this.triple = triple;
		prototype = new GammaCodedIntLabel( "A", 16 );
		int m = 0;
		for( int i = 0; i < triple.length; i++ ) m = Math.max( m, Math.max( triple[ i ][ 0 ], triple[ i ][ 1 ] ) );
		Arrays.sort( triple, new Comparator<int[]>() {
			public int compare( int[] p, int[] q ) {
				final int t =  p[ 0 ] - q[ 0 ]; // Compare by source
				if ( t != 0 ) return t;
				return p[ 1 ] - q[ 1 ]; // Compare by destination
			}
		} );
		
		n = m + 1;
	}
	
	@Override
	public Label prototype() {
		return prototype;
	}

	@Override
	public int numNodes() {
		return n;
	}

	@Override
	public ArcLabelledNodeIterator nodeIterator( int from ) {
		if ( from == 0 ) return nodeIterator();
		throw new UnsupportedOperationException();
	}

	private final class ArcIterator extends AbstractLazyIntIterator implements LabelledArcIterator  {
		private final int d;
		private int k = 0; // Index of the last returned triple is pos+k
		private final int pos;
		private final GammaCodedIntLabel label;

		private ArcIterator( int d, int pos, GammaCodedIntLabel label ) {
			this.d = d;
			this.pos = pos;
			this.label = label;
		}

		public Label label() {
			if ( k == 0 ) throw new IllegalStateException();
			label.value = triple[ pos + k ][ 2 ];
			return label;
		}

		public int nextInt() {
			if ( k >= d ) return -1;
			return triple[ pos + ++k ][ 1 ];
		}
	}

	@Override
	public ArcLabelledNodeIterator nodeIterator() {
		return new ArcLabelledNodeIterator() {
			/** Last node returned by this iterator. */
			private int last = -1;
			/** Last triple examined by this iterator. */
			private int pos = -1;
			/** A local copy of the prototye. */
			private GammaCodedIntLabel label = (GammaCodedIntLabel) prototype.copy();

			@Override
			public LabelledArcIterator successors() {
				if ( last < 0 ) throw new IllegalStateException();
				final int d = outdegree(); // Triples to be returned are pos+1,pos+2,...,pos+d 
				return new ArcIterator( d, pos, label );
			}

			@Override
			public int outdegree() {
				if ( last < 0 ) throw new IllegalStateException();
				int p;
				for ( p = pos + 1; p < triple.length && triple[ p ][ 0 ] == last; p++ );
				return p - pos - 1;
			}

			public boolean hasNext() {
				return last < n - 1;
			}
			
			@Override
			public int nextInt() {
				if ( !hasNext() ) throw new NoSuchElementException();
				if ( last >= 0 ) pos += outdegree();
				return ++last;
			}
			
		};
	}
        
        /* This main file assumes that the arcs for the graph files to be created have been written in a tab seperated triplets 
         * format as generated by labelled_graphgen and whose filename begins with the word db.
         */
        
        
	public static void main( String arg[] ) throws Exception {
		File directory = new File (".");
		String[] listoffiles = directory.list();

		String basename;

		for(int i=0;i<listoffiles.length;i++){
			//System.out.println(listoffiles[i]+"and this is the result of the test"+list[i].endsWith(".txt"));
			if(!listoffiles[i].endsWith(".txt"))
				continue;
			else if(!listoffiles[i].startsWith("db"))
				continue;

			basename = listoffiles[i];
			int j = basename.indexOf(".");
			basename = basename.substring(0, j);
			FileInputStream fstream = new FileInputStream(basename+".txt");
			//System.out.println("reach 1");
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
			System.out.println("It has started writing");
			ObjectArrayList<int[]> list = new ObjectArrayList<int[]>();
			//ObjectArrayList<int[]> list_rel = new ObjectArrayList<int[]>();
			String line;//, arg2;
			//int charint;
			int log=0;
			while( ( line = br.readLine() ) != null ) {
				//System.out.println(line);
				log++;
				if  (log%400000==0)
					System.out.println("I am at line no. "+log);
				if(log>6000000)
					break;
				final String p[] = line.split( "\t" );
				list.add( new int[] { Integer.parseInt(  p[ 0 ] ),Integer.parseInt( p[ 1 ] ), Integer.parseInt(  p[2] ) } );

			}
			System.out.println("It has started building the graph");

			final ArcLabelledImmutableGraph g = new Webgraph_maker( list.toArray( new int[0][] ) );

			BVGraph.store( g, basename + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX );
			BitStreamArcLabelledImmutableGraph.store( g, basename, basename + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX );
		}  
	}
}
