package it.unimi.dsi.webgraph.algo;

/*		 
 * Copyright (C) 2007-2011 Sebastiano Vigna 
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
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntStack;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.Transform.LabelledArcFilter;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;

import java.io.IOException;
import java.util.BitSet;

import org.apache.log4j.Logger;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** Computes the strongly connected components (and optionally the buckets) of an immutable graph.
 * 
 * <p>The {@link #compute(ImmutableGraph, boolean, ProgressLogger)} method of this class will return
 * an instance that contains the data computed by running a variant of Tarjan's algorithm on an immutable graph.
 * Besides the usually strongly connected components, it is possible to compute the <em>buckets</em> of the
 * graph, that is, nodes belonging to components that are terminal, but not dangling, in the component DAG.
 * 
 * <p>After getting an resulting instance, it is possible to run the {@link #computeSizes()} and {@link #sortBySize(int[])}
 * methods to obtain further information. This scheme has been devised to exploit the available memory as much
 * as possible&mdash;after the components have been computed, the returned instance keeps no track of
 * the graph, and the related memory can be freed by the garbage collector.
 * 
 * <h2>Stack size</h2>
 * 
 * <p>The method {@link #compute(ImmutableGraph, boolean, ProgressLogger)} might require a large stack size,
 * that should be set using suitable JVM options. Note, however,
 * that the stack size must be enlarged also on the operating-system side&mdash;for instance, using <samp>ulimit -s unlimited</samp>.
 */


public class StronglyConnectedComponents {
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	private static final Logger LOGGER = Util.getLogger( StronglyConnectedComponents.class );
	
	/** The number of strongly connected components. */
	final public int numberOfComponents;
	/** The component of each node. */
	final public int component[];
	/** The bit set for buckets, or <code>null</code>, in which case buckets have not been computed. */
	final public BitSet buckets;

	protected StronglyConnectedComponents( final int numberOfComponents, final int[] component, final BitSet buckets ) {
		this.numberOfComponents = numberOfComponents;
		this.component = component;
		this.buckets = buckets;
	}  
	
	private final static class Visit {
		/** The graph. */
		private final ImmutableGraph graph;
		/** The number of nodes in {@link #graph}. */
		private final int n;
		/** A progress logger. */
		private final ProgressLogger pl;
		/** Whether we should compute buckets. */
		private final boolean computeBuckets;
		/** For non visited nodes, 0. For visited non emitted nodes the visit time. For emitted node -c-1, where c is the component number. */
		private final int status[];
		/** The buckets. */
		private final BitSet buckets;
		/** The component stack. */
		private final IntStack stack;

		/** The first-visit clock (incremented at each visited node). */
		private int clock;
		/** The number of components already output. */
		private int numberOfComponents;

		private Visit( final ImmutableGraph graph, final int[] status, final BitSet buckets, ProgressLogger pl ) {
			this.graph = graph;
			this.buckets = buckets;
			this.status = status;
			this.pl = pl;
			this.computeBuckets = buckets != null;
			this.n = graph.numNodes();
			stack = new IntArrayList( n );
		}
		
		/** Visits a node.
		 * 
		 * @param x the node to visit.
		 * @return true if <code>x</code> is a bucket.
		 */
		private boolean visit( final int x ) {
			final int[] status = this.status;
			if ( pl != null ) pl.lightUpdate();
			status[ x ] = ++clock;
			stack.push( x );

			int d = graph.outdegree( x );
			boolean noOlderNodeFound = true, isBucket = d != 0; // If we're dangling we're certainly not a bucket.
			
			if ( d != 0 ) {
				final LazyIntIterator successors = graph.successors( x );
				while( d-- != 0 ) {
					final int s = successors.nextInt();
					// If we can reach a non-bucket or another component we are not a bucket.
					if ( status[ s ] == 0 && ! visit( s ) || status[ s ] < 0 ) isBucket = false;
					if ( status[ s ] > 0 && status[ s ] < status[ x ] ) {
						status[ x ] = status[ s ];
						noOlderNodeFound = false;
					}
				}
			}

			if ( noOlderNodeFound ) {
				numberOfComponents++;
				int z;
				do {
					z = stack.popInt();
					// Component markers are -c-1, where c is the component number.
					status[ z ] = -numberOfComponents;
					if ( isBucket && computeBuckets ) buckets.set( z );
				} while( z != x );
			}
			
			return isBucket;
		}
		
		
		public void run() {
			if ( pl != null ) {
				pl.itemsName = "nodes";
				pl.expectedUpdates = n;
				pl.displayFreeMemory = true;
				pl.start( "Computing strongly connected components..." );
			}
			for ( int x = 0; x < n; x++ ) if ( status[ x ] == 0 ) visit( x );
			if ( pl != null ) pl.done();

			// Turn component markers into component numbers.
			for ( int x = n; x-- != 0; ) status[ x ] = -status[ x ] - 1;
			
			stack.push( numberOfComponents ); // Horrible kluge to return the number of components.
		}
	}
	
	/** Computes the strongly connected components of a given graph.
	 * 
	 * @param graph the graph whose strongly connected components are to be computed.
	 * @param computeBuckets if true, buckets will be computed.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an instance of this class containing the computed components.
	 */
	public static StronglyConnectedComponents compute( final ImmutableGraph graph, final boolean computeBuckets, final ProgressLogger pl ) {
		final int n = graph.numNodes();
		final Visit visit = new Visit( graph, new int[ n ], computeBuckets ? new BitSet( n ) : null, pl );
		visit.run();
		return new StronglyConnectedComponents( visit.numberOfComponents, visit.status, visit.buckets );
	}


	private final static class FilteredVisit {
		/** The graph. */
		private final ArcLabelledImmutableGraph graph;
		/** The number of nodes in {@link #graph}. */
		private final int n;
		/** A progress logger. */
		private final ProgressLogger pl;
		/** A filter on arc labels. */
		private final LabelledArcFilter filter;
		/** Whether we should compute buckets. */
		private final boolean computeBuckets;
		/** For non visited nodes, 0. For visited non emitted nodes the visit time. For emitted node -c-1, where c is the component number. */
		private final int status[];
		/** The buckets. */
		private final BitSet buckets;
		/** The component stack. */
		private final IntStack stack;

		
		/** The first-visit clock (incremented at each visited node). */
		private int clock;
		/** The number of components already output. */
		private int numberOfComponents;
		
		private FilteredVisit( final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter, final int[] status, final BitSet buckets, ProgressLogger pl ) {
			this.graph = graph;
			this.filter = filter;
			this.buckets = buckets;
			this.status = status;
			this.pl = pl;
			this.computeBuckets = buckets != null;
			this.n = graph.numNodes();
			stack = new IntArrayList( n );
		}
		
		/** Visits a node.
		 * 
		 * @param x the node to visit.
		 * @return true if <code>x</code> is a bucket.
		 */
		private boolean visit( final int x ) {
			final int[] status = this.status;
			if ( pl != null ) pl.lightUpdate();
			status[ x ] = ++clock;
			stack.push( x );

			int d = graph.outdegree( x ), filteredDegree = 0;
			boolean noOlderNodeFound = true, isBucket = true;
			
			if ( d != 0 ) {
				final LabelledArcIterator successors = graph.successors( x );
				while( d-- != 0 ) {
					final int s = successors.nextInt();
					if ( ! filter.accept( x, s, successors.label() ) ) continue;
					filteredDegree++;
					// If we can reach a non-bucket or another component we are not a bucket.
					if ( status[ s ] == 0 && ! visit( s ) || status[ s ] < 0 ) isBucket = false;
					if ( status[ s ] > 0 && status[ s ] < status[ x ] ) {
						status[ x ] = status[ s ];
						noOlderNodeFound = false;
					}
				}
			}

			if ( filteredDegree == 0 ) isBucket = false;
			
			if ( noOlderNodeFound ) {
				numberOfComponents++;
				int z;
				do {
					z = stack.popInt();
					// Component markers are -c-1, where c is the component number.
					status[ z ] = -numberOfComponents;
					if ( isBucket && computeBuckets ) buckets.set( z );
				} while( z != x );
			}
			
			return isBucket;
		}
		
		
		public void run() {
			if ( pl != null ) {
				pl.itemsName = "nodes";
				pl.expectedUpdates = n;
				pl.displayFreeMemory = true;
				pl.start( "Computing strongly connected components..." );
			}
			for ( int x = 0; x < n; x++ ) if ( status[ x ] == 0 ) visit( x );
			if ( pl != null ) pl.done();

			// Turn component markers into component numbers.
			for ( int x = n; x-- != 0; ) status[ x ] = -status[ x ] - 1;
			
			stack.push( numberOfComponents ); // Horrible kluge to return the number of components.
		}
	}

	/** Computes the strongly connected components of a given arc-labelled graph, filtering its arcs.
	 * 
	 * @param graph the arc-labelled graph whose strongly connected components are to be computed.
	 * @param filter a filter selecting the arcs that must be taken into consideration.
	 * @param computeBuckets if true, buckets will be computed.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an instance of this class containing the computed components.
	 */
	public static StronglyConnectedComponents compute( final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter, final boolean computeBuckets, final ProgressLogger pl ) {
		final int n = graph.numNodes();
		FilteredVisit filteredVisit = new FilteredVisit( graph, filter, new int[ n ], computeBuckets ? new BitSet( n ) : null, pl );
		filteredVisit.run();
		return new StronglyConnectedComponents( filteredVisit.numberOfComponents, filteredVisit.status, filteredVisit.buckets );
	}


	/** Returns the size array for this set of strongly connected components.
	 * 
	 * @return the size array for this set of strongly connected components.
	 */
	public int[] computeSizes() {
		final int[] size = new int[ numberOfComponents ];
		for( int i = component.length; i-- != 0; ) size[ component[ i ] ]++;
		return size;
	}
	
	/** Renumbers by decreasing size the components of this set.
	 *
	 * <p>After a call to this method, both the internal status of this class and the argument
	 * array are permuted so that the sizes of strongly connected components are decreasing
	 * in the component index.
	 *
	 *  @param size the components sizes, as returned by {@link #computeSizes()}.
	 */
	public void sortBySize( final int[] size ) {
		final int[] perm = Util.identity( size.length );
		IntArrays.quickSort( perm, 0, perm.length, new AbstractIntComparator() {
			public int compare( final int x, final int y ) {
				return size[ y ] - size[ x ]; 
			}
		});
		final int[] copy = size.clone();
		for ( int i = size.length; i-- != 0; ) size[ i ] = copy[ perm[ i ] ];
		Util.invertPermutationInPlace( perm );
		for( int i = component.length; i-- != 0; ) component[ i ] = perm[ component[ i ] ];
	}
	

	public static void main( String arg[] ) throws IOException, JSAPException {
		SimpleJSAP jsap = new SimpleJSAP( StronglyConnectedComponents.class.getName(), 
				"Computes the strongly connected components (and optionally the buckets) of a graph of given basename. The resulting data is saved " +
				"in files stemmed from the given basename with extension .scc (a list of binary integers specifying the " +
				"component of each node), .sccsizes (a list of binary integer specifying the size of each component) and .buckets " +
				" (a serialised BitSet specifying buckets). Please use suitable JVM options to set a large stack size.",
				new Parameter[] {
			new Switch( "sizes", 's', "sizes", "Compute component sizes." ),
			new Switch( "renumber", 'r', "renumber", "Renumber components in decreasing-size order." ),
			new Switch( "buckets", 'b', "buckets", "Compute buckets (nodes belonging to a bucket component, i.e., a terminal nondangling component)." ),
			new FlaggedOption( "filter", new ObjectParser( LabelledArcFilter.class, GraphClassParser.PACKAGE ), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "filter", "A filter for labelled arcs; requires the provided graph to be arc labelled." ),
			new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
			new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph." ),
			new UnflaggedOption( "resultsBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The basename of the resulting files." ),
		}		
		);

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) System.exit( 1 );

		final String basename = jsapResult.getString( "basename" );
		final String resultsBasename = jsapResult.getString( "resultsBasename", basename );
		final LabelledArcFilter filter = (LabelledArcFilter)jsapResult.getObject( "filter" );
		ProgressLogger pl = new ProgressLogger( LOGGER, jsapResult.getLong( "logInterval" ) );

		final StronglyConnectedComponents components = 
			filter != null ? StronglyConnectedComponents.compute( ArcLabelledImmutableGraph.load( basename ), filter, jsapResult.getBoolean( "buckets" ), pl )
					: StronglyConnectedComponents.compute( ImmutableGraph.load( basename ), jsapResult.getBoolean( "buckets" ), pl );

		if ( jsapResult.getBoolean( "sizes" ) || jsapResult.getBoolean( "renumber" ) ) {
			final int size[] = components.computeSizes();
			if ( jsapResult.getBoolean( "renumber" ) ) components.sortBySize( size );
			if ( jsapResult.getBoolean( "sizes" ) ) BinIO.storeInts( size, resultsBasename + ".sccsizes" );
		}
		BinIO.storeInts(  components.component, resultsBasename + ".scc" );
		if ( components.buckets != null ) BinIO.storeObject( components.buckets, resultsBasename + ".buckets" );
	}
}
