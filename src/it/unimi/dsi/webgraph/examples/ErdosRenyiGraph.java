package it.unimi.dsi.webgraph.examples;

/*		 
 * Copyright (C) 2010-2011 Paolo Boldi and Sebastiano Vigna 
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


import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XorShiftStarRandom;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.webgraph.NodeIterator;

import java.io.IOException;

import org.apache.log4j.Logger;

import cern.jet.random.Binomial;
import cern.jet.random.engine.RandomEngine;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** Models an Erdos-Renyi type of graph, where the number of nodes
 *  is fixed, and there is a fixed probability that an arc is put
 *  between any two nodes (independently for every pair). 
 *  
 *  <p>Note that the graph returned by {@link #generate()} is not {@linkplain ImmutableGraph#randomAccess() random-access}:
 *  you can, however, {@linkplain ArrayListMutableGraph#ArrayListMutableGraph(ImmutableGraph) make a mutable copy of the returned graph}
 *  and then {@linkplain ArrayListMutableGraph#immutableView() take its immutable view}. 
 *  */
public class ErdosRenyiGraph  {
	
	private final static Logger LOGGER = Util.getLogger( ErdosRenyiGraph.class );
	
	/** Number of nodes. */
	private final int n;
	/** Probability to put an arc between each pair of nodes. */
	private final double p;
	/** Whether loops should also be generated. */
	private final boolean loops;

	/** Create a graph with given parameters.
	 * 
	 * @param n the number of nodes.
	 * @param p the probability of generating each arc.
	 * @param loops whether loops are allowed or not.
	 */
	public ErdosRenyiGraph( final int n, final double p, final boolean loops ) {
		this.n = n;
		this.p = p;
		this.loops = loops;
	}

	/** Create a graph with given parameters.
	 * 
	 * @param n the number of nodes.
	 * @param m the expected number of arcs.
	 * @param loops whether loops are allowed or not.
	 */
	public ErdosRenyiGraph( final int n, final long m, final boolean loops ) {
		this.n = n;
		this.p = (double)m / ( loops? (long)n * n : (long)n * ( n - 1 ) );
		this.loops = loops;
	}

	/** Generates an Erdos-Renyi graph with the specified parameters, using the given random object.
	 * 
	 * @param seed the seed for random generation.
	 * @return the generated graph.
	 */
	public ImmutableSequentialGraph generate( final long seed ) {
		LOGGER.debug( "Generating with probability " + p );

		return new ImmutableSequentialGraph() {
			@Override
			public int numNodes() {
				return n;
			}

			public NodeIterator nodeIterator() {
				return new NodeIterator() {
					private XorShiftStarRandom random = new XorShiftStarRandom( seed );
					
					private Binomial bg = new Binomial( n - ( loops ? 0 : 1 ), p, new RandomEngine() {
						private static final long serialVersionUID = 1L;
						@Override
						public int nextInt() {
							return random.nextInt();
						}
					});

					private int outdegree;
					private int curr = -1;
					private IntOpenHashSet successors = new IntOpenHashSet();
					private int[] successorArray = new int[ 1024 ]; 

					public boolean hasNext() {
						return curr < n - 1;
					}
					
					@Override
					public int nextInt() {
						curr++;
						outdegree = bg.nextInt();
						successors.clear();
						if ( ! loops ) successors.add( curr );
						for( int i = 0; i < outdegree; i++ ) while( ! successors.add( random.nextInt( n ) ) );
						if ( ! loops ) successors.remove( curr );
						successorArray = IntArrays.grow( successorArray, outdegree );
						successors.toIntArray( successorArray );
						IntArrays.quickSort( successorArray, 0, outdegree );
						return curr;
					}
					
					@Override
					public int outdegree() {
						return outdegree;
					}
					
					@Override
					public int[] successorArray() {
						return successorArray;
					}
				};
			}
		};	
	}

	/** Generates an Erdos-Renyi graph with the specified parameters.
	 * 
	 * @return the generated graph.
	 */
	public ImmutableGraph generate() {
		return generate( Util.randomSeed() );
	}
	
	
	public static void main( String arg[] ) throws IOException, JSAPException {		
		SimpleJSAP jsap = new SimpleJSAP( ErdosRenyiGraph.class.getName(), "Generates an Erdos-Renyi random graph.",
				new Parameter[] {
			new Switch( "loops", 'l', "loops", "Tells if the graph should include self-loops." ), 
			new FlaggedOption( "p", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "The edge presence probability." ),
			new FlaggedOption( "m", JSAP.LONGSIZE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'm', "The expected number of arcs." ),
			new UnflaggedOption( "baseName", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the output graph file." ),
			new UnflaggedOption( "n", JSAP.INTEGER_PARSER, JSAP.REQUIRED, "The number of nodes." ),
		});
		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) System.exit( 1 );

		final String baseName = jsapResult.getString( "baseName" );
		final int n = jsapResult.getInt( "n" );
		final boolean loops = jsapResult.getBoolean( "loops" );
		
		if ( jsapResult.userSpecified( "p" ) && jsapResult.userSpecified( "m" ) ) {
			System.err.println( "Options p and m cannot be specified together" );
			System.exit( 1 );
		}
		if ( ! jsapResult.userSpecified( "p" ) && ! jsapResult.userSpecified( "m" ) ) {
			System.err.println( "Exactly one of the options p and m must be specified" );
			System.exit( 1 );
		}
		
		BVGraph.store( ( jsapResult.userSpecified( "p" ) ? new ErdosRenyiGraph( n, jsapResult.getDouble( "p" ), loops ) : new ErdosRenyiGraph( n, jsapResult.getLong( "m" ), loops ) ).generate(), baseName, new ProgressLogger() );
	}
}
