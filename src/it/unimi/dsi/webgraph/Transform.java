package it.unimi.dsi.webgraph;

/*		 
 * Copyright (C) 2003-2011 Paolo Boldi, Massimo Santini and Sebastiano Vigna 
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
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntHeapSemiIndirectPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XorShiftStarRandom;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableSequentialGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.Label;
import it.unimi.dsi.webgraph.labelling.LabelMergeStrategy;
import it.unimi.dsi.webgraph.labelling.LabelSemiring;
import it.unimi.dsi.webgraph.labelling.Labels;
import it.unimi.dsi.webgraph.labelling.UnionArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** Static methods that manipulate immutable graphs. 
 * 
 *  <P>Most methods take an {@link
 *  it.unimi.dsi.webgraph.ImmutableGraph} (along with some other data, that
 *  depend on the kind of transformation), and return another {@link
 *  it.unimi.dsi.webgraph.ImmutableGraph} that represents the transformed
 *  version.
 */

public class Transform {

	private static final Logger LOGGER = Util.getLogger( Transform.class );
	
	private static final boolean DEBUG = false;
	private static final boolean ASSERTS = false;

	private Transform() {}



	/** Provides a method to accept or reject an arc.
	 *
	 * <P>Note that arc filters are usually stateless. Thus, their declaration
	 * should comprise a static singleton (e.g., {@link Transform#NO_LOOPS}).
	 */
	public interface ArcFilter {
	
		/**
		 * Tells if the arc <code>(i,j)</code> has to be accepted or not.
		 * 
		 * @param i the source of the arc. 
		 * @param j the destination of the arc.
		 * @return if the arc has to be accepted.
		 */
		public boolean accept( int i, int j );
	}

	/** Provides a method to accept or reject a labelled arc.
	 *
	 * <P>Note that arc filters are usually stateless. Thus, their declaration
	 * should comprise a static singleton (e.g., {@link Transform#NO_LOOPS}).
	 */
	public interface LabelledArcFilter {
	
		/**
		 * Tells if the arc <code>(i,j)</code> with label <code>label</code> has to be accepted or not.
		 * 
		 * @param i the source of the arc. 
		 * @param j the destination of the arc.
		 * @param label the label of the arc.
		 * @return if the arc has to be accepted.
		 */
		public boolean accept( int i, int j, Label label );
	}

	/** An arc filter that rejects loops. */
	final static private class NoLoops implements ArcFilter, LabelledArcFilter {
		private NoLoops() {}
		/** Returns true if the two arguments differ.
		 * 
		 * @return <code>i != j</code>.
		 */
		public boolean accept( final int i, final int j ) {
			return i != j;
		}
		public boolean accept( int i, int j, Label label ) {
			return i != j;
		}
	}

	/** An arc filter that rejects arcs whose well-known attribute has a value smaller than a given threshold. */
	final static public class LowerBound implements LabelledArcFilter {
		private final int lowerBound;
		
		public LowerBound( final int lowerBound ) {
			this.lowerBound = lowerBound;
		}
		
		public LowerBound( String lowerBound ) {
			this( Integer.parseInt( lowerBound ) );
		}
		/** Returns true if the integer value associated to the well-known attribute of the label is larger than the threshold.
		 * 
		 * @return true if <code>label.{@link Label#getInt()}</code> is larger than the threshold.
		 */
		public boolean accept( int i, int j, Label label ) {
			return label.getInt() >= lowerBound;
		}
	}

	
	/** A singleton providing an arc filter that rejects loops. */
	final static public NoLoops NO_LOOPS = new NoLoops();

	/** A class that exposes an immutable graph viewed through a filter. */
	private static final class FilteredImmutableGraph extends ImmutableGraph {
		private final ArcFilter filter;
		private final ImmutableGraph graph;
		private int succ[];
		private int cachedNode = -1;

		private FilteredImmutableGraph( ArcFilter filter, ImmutableGraph graph ) {
			this.filter = filter;
			this.graph = graph;
		}

		public int numNodes() { 
			return graph.numNodes(); 
		}
		
		public FilteredImmutableGraph copy() {
			return new FilteredImmutableGraph( filter, graph.copy() );
		}

		@Override
		public boolean randomAccess() {
			return graph.randomAccess();
		}

		public LazyIntIterator successors( final int x ) {
			return new AbstractLazyIntIterator() {
		
				@SuppressWarnings("hiding")
				private final LazyIntIterator succ = graph.successors( x );

				public int nextInt() {
					int t;
					while ( ( t = succ.nextInt() ) != -1 ) if ( filter.accept( x, t ) ) return t;
					return -1;
				}
			};
		}
		
		private void fillCache( final int x ) {
			if ( x == cachedNode ) return;
			succ = LazyIntIterators.unwrap( successors( x ) );
			cachedNode = x;
		}

		public int[] successorArray( int x ) {
			fillCache( x );
			return succ;
		}

		public int outdegree( int x ) {
			fillCache( x );
			return succ.length;
		}

		public NodeIterator nodeIterator() {
			return new NodeIterator() {
				final NodeIterator nodeIterator = graph.nodeIterator();
				@SuppressWarnings("hiding") int[] succ = IntArrays.EMPTY_ARRAY;
				int outdegree = -1;
		
				public int outdegree() {
					if ( outdegree == -1 ) throw new IllegalStateException();
					return outdegree;
				}
		
				public int nextInt() {
					final int currNode = nodeIterator.nextInt();
					final int oldOutdegree = nodeIterator.outdegree();
					int[] oldSucc = nodeIterator.successorArray();
					succ = IntArrays.ensureCapacity( succ, oldOutdegree, 0 );
					outdegree = 0;
					for( int i = 0; i < oldOutdegree; i++ ) if ( filter.accept( currNode, oldSucc[ i ] ) ) succ[ outdegree++ ] = oldSucc[ i ];
					return currNode;
				}
				
				public int[] successorArray() {
					if ( outdegree == -1 ) throw new IllegalStateException();
					return succ;
				}
		
				public boolean hasNext() {
					return nodeIterator.hasNext();
				}
			};
		}
	}

	/** A class that exposes an arc-labelled immutable graph viewed through a filter. */
	private static final class FilteredArcLabelledImmutableGraph extends ArcLabelledImmutableGraph {
		private final LabelledArcFilter filter;
		private final ArcLabelledImmutableGraph graph;
		private int succ[];
		private Label label[];
		private int cachedNode = -1;
		
		private final class FilteredLabelledArcIterator extends AbstractLazyIntIterator implements LabelledArcIterator {
			private final int x;

			private final LabelledArcIterator successors;

			private FilteredLabelledArcIterator( final int x, final LabelledArcIterator successors ) {
				this.x = x;
				this.successors = successors;
			}

			public int nextInt() {
				int t;
				while ( ( t = successors.nextInt() ) != -1 ) if ( filter.accept( x, t, successors.label() ) ) return t;
				return -1;
			}

			public Label label() {
				return successors.label();
			}
		}

		private FilteredArcLabelledImmutableGraph( LabelledArcFilter filter, ArcLabelledImmutableGraph graph ) {
			this.filter = filter;
			this.graph = graph;
		}

		public int numNodes() { 
			return graph.numNodes(); 
		}
		
		public ArcLabelledImmutableGraph copy() {
			return new FilteredArcLabelledImmutableGraph( filter, graph.copy() );
		}

		@Override
		public boolean randomAccess() {
			return graph.randomAccess();
		}

		@Override
		public Label prototype() {
			return graph.prototype();
		}

		private void fillCache( final int x ) {
			if ( x == cachedNode ) return;
			succ = LazyIntIterators.unwrap( successors( x ) );
			label = super.labelArray( x );
			cachedNode = x;
		}

		public LabelledArcIterator successors( final int x ) {
			return new FilteredLabelledArcIterator( x, graph.successors( x ) );
		}

		public int[] successorArray( final int x ) {
			fillCache( x );
			return succ;
		}

		public Label[] labelArray( final int x ) {
			fillCache( x );
			return label;
		}
		
		public int outdegree( int x ) {
			fillCache( x );
			return succ.length;
		}

		public ArcLabelledNodeIterator nodeIterator() {
			return new ArcLabelledNodeIterator() {
				final ArcLabelledNodeIterator nodeIterator = graph.nodeIterator();
				private int currNode = -1;
				private int outdegree = -1;

				public int outdegree() {
					if ( currNode == -1 ) throw new IllegalStateException();
					if ( outdegree == -1 ) {
						int d = 0;
						LabelledArcIterator successors = successors();
						while( successors.nextInt() != -1 ) d++;
						outdegree = d;
					}
					return outdegree;
				}
		
				public int nextInt() {
					outdegree = -1;
					return currNode = nodeIterator.nextInt();
				}
		
				public boolean hasNext() {
					return nodeIterator.hasNext();
				}

				@Override
				public LabelledArcIterator successors() {
					return new FilteredLabelledArcIterator( currNode, nodeIterator.successors() );
				}
			};
		}

	}	
	
	/** Returns a graph with some arcs eventually stripped, according to the given filter.
	 * 
	 * @param graph a graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @return the filtered graph.
	 */
	public static ImmutableGraph filterArcs( final ImmutableGraph graph, final ArcFilter filter ) {
		return filterArcs( graph, filter, null );
	}

	/** Returns a graph with some arcs eventually stripped, according to the given filter.
	 * 
	 * @param graph a graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @param ignored a progress logger.
	 * @return the filtered graph.
	 */
	public static ImmutableGraph filterArcs( final ImmutableGraph graph, final ArcFilter filter, final ProgressLogger ignored ) {
		return new FilteredImmutableGraph( filter, graph );
	}

	/** Returns a labelled graph with some arcs eventually stripped, according to the given filter.
	 * 
	 * @param graph a labelled graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @return the filtered graph.
	 */
	public static ArcLabelledImmutableGraph filterArcs( final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter ) {
		return filterArcs( graph, filter, null );
	}
	
	/** Returns a labelled graph with some arcs eventually stripped, according to the given filter.
	 * 
	 * @param graph a labelled graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @param ignored a progress logger.
	 * @return the filtered graph.
	 */
	public static ArcLabelledImmutableGraph filterArcs( final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter, final ProgressLogger ignored ) {
		return new FilteredArcLabelledImmutableGraph( filter, graph );
	}
	
	private static final class RemappedImmutableGraph extends ImmutableGraph {
		private final int[] map;
		private final ImmutableGraph g;
		private final boolean isInjective;
		private final boolean isPermutation;
		private final int remappedNodes;
		private final int destNumNodes;
		private final int[] pseudoInverse;
		private int[] succ;
		private int outdegree;
		private int currentNode = -1;

		private RemappedImmutableGraph( int[] map, ImmutableGraph g, boolean isInjective, boolean isPermutation, int remappedNodes, int destNumNodes, int[] pseudoInverse ) {
			this.map = map;
			this.g = g;
			this.isInjective = isInjective;
			this.isPermutation = isPermutation;
			this.remappedNodes = remappedNodes;
			this.destNumNodes = destNumNodes;
			this.pseudoInverse = pseudoInverse;
		}

		public RemappedImmutableGraph copy() {
			return new RemappedImmutableGraph( map, g.copy(), isInjective, isPermutation, remappedNodes, destNumNodes, pseudoInverse );
		}
		
		public int numNodes() {
			return destNumNodes;
		}

		@Override
		public boolean randomAccess() {
			return true;
		}

		public int[] successorArray( int x ) {
			if ( currentNode != x ) {
				IntSet succSet = new IntOpenHashSet();
				succSet.clear();
					
				if ( isPermutation ) {
					LazyIntIterator i = g.successors( pseudoInverse[ x ] );
					for( int d = g.outdegree( pseudoInverse[ x ] ); d-- != 0; ) succSet.add( map[ i.nextInt() ] );
				} 
				else {
					int low = 0, high = remappedNodes - 1, mid = 0;
					while ( low <= high ) {
						mid = ( low + high ) >>> 1;
						final int midVal = map[ pseudoInverse[ mid ] ];
						if ( midVal < x )low = mid + 1;
						else if ( midVal > x ) high = mid - 1;
						else break;
					}
					int t, p;
					if ( isInjective ) {
						if ( map[ p = pseudoInverse[ mid ] ] == x ) {
							LazyIntIterator i = g.successors( p );
							for( int d = g.outdegree( p ); d-- != 0; ) if ( ( t = map[ i.nextInt() ] ) != -1 ) succSet.add( t );
						}
					}
					else {
						while ( mid > 0 && map[ pseudoInverse[ mid - 1 ] ] == x ) mid--;
						while ( mid < remappedNodes && map[ p = pseudoInverse[ mid ] ] == x ) {
							LazyIntIterator i = g.successors( p );
							for( int d = g.outdegree( p ); d-- != 0; ) if ( ( t = map[ i.nextInt() ] ) != -1 ) succSet.add( t );
							mid++;
						}
					}
				}
				outdegree = succSet.size();
				currentNode = x;
				succ = succSet.toIntArray();
				if ( outdegree > 0 ) Arrays.sort( succ, 0, outdegree );
			}
			return succ;
		}

		public int outdegree( int x ) {
			if ( currentNode != x ) successorArray( x );
			return outdegree;
		}
	}

	/** Remaps the the graph nodes through a partial function specified via
	 *  an array. More specifically, <code>map.length=g.numNodes()</code>,
	 *  and <code>map[i]</code> is the new name of node <code>i</code>, or -1 if the node
	 *  should not be mapped. If some
	 *  index appearing in <code>map</code> is larger than or equal to the
	 *  number of nodes of <code>g</code>, the resulting graph is enlarged correspondingly.
	 *  
	 *  <P>Arcs are mapped in the obvious way; in other words, there is
	 *  an arc from <code>map[i]</code> to <code>map[j]</code> (both nonnegative) 
	 *  in the transformed 
	 *  graph iff there was an arc from <code>i</code> to <code>j</code>
	 *  in the original graph.  
	 * 
	 *  <P>Note that if <code>map</code> is bijective, the returned graph
	 *  is simply a permutation of the original graph. 
	 *  Otherwise, the returned graph is obtained by deleting nodes mapped
	 *  to -1, quotienting nodes w.r.t. the equivalence relation induced by the fibres of <code>map</code>
	 *  and renumbering the result, always according to <code>map</code>.
	 * 
	 * <P>This method <strong>requires</strong> {@linkplain ImmutableGraph#randomAccess()} random access.
	 * 
	 * @param g the graph to be transformed.
	 * @param map the transformation map.
	 * @param pl a progress logger to be used during the precomputation, or <code>null</code>.
	 * @return the transformed graph (provides {@linkplain ImmutableGraph#randomAccess() random access}.
	 */
	public static ImmutableGraph map( final ImmutableGraph g, final int map[], final ProgressLogger pl ) {	
		int i, j;
		if ( ! g.randomAccess() ) throw new IllegalArgumentException( "Graph mapping requires random access" );

		final int sourceNumNodes = g.numNodes();
		if ( map.length != sourceNumNodes ) throw new IllegalArgumentException( "The graph to be mapped has " + sourceNumNodes + " whereas the map contains " + map.length + " entries" );

		int max = -1;
		if ( pl != null ) {
			pl.itemsName = "nodes";
			pl.start( "Storing identity..." );
		}

		// Compute the number of actually remapped nodes (those with f[] != -1)
		for ( i = j = 0; i < sourceNumNodes; i++ ) if ( map[ i ] >= 0 ) j++; 
		final int remappedNodes = j;
		final boolean everywhereDefined = remappedNodes == sourceNumNodes;
		
		/* The pseudoinverse array: for each node of the transformed graph that is image of a node
		 * of the source graph, it contains the index of that node. */
		final int pseudoInverse[] = new int[ remappedNodes ];

		for ( i = j = 0; i < sourceNumNodes; i++ ) {
			if ( max < map[ i ] ) max = map[ i ];
			//if ( f[ i ] < 0 ) throw new IllegalArgumentException( "The supplied map contains a negative value (" + f[ i ] +") at index " + i );
			if ( map[ i ] >= 0 ) pseudoInverse[ j++ ] = i;
		}
		
		final int destNumNodes = max + 1;
		final boolean notEnlarged = destNumNodes <= sourceNumNodes;
		
		if ( pl != null ) {
			pl.count = remappedNodes;
			pl.done();
		}

		// sort sf[]			
		if ( pl != null ) pl.start( "Sorting to obtain pseudoinverse..." );
		IntArrays.radixSortIndirect( pseudoInverse, map, 0, remappedNodes, false );
		if ( pl != null ) {
			pl.count = sourceNumNodes;
			pl.done();
		}
		
		// check if f is injective
		if ( pl != null ) pl.start( "Checking whether it is injective..." );
		int k = remappedNodes - 1;
		// Note that we need the first check for the empty graph.
		if ( k >= 0 ) while( k-- != 0 ) if ( map[ pseudoInverse[ k ] ] == map[ pseudoInverse[ k + 1 ] ] ) break;
		final boolean isInjective = k == -1;
		if ( pl != null ) {
			pl.count = sourceNumNodes;
			pl.stop( "(It is" + ( isInjective ? "" : " not") + " injective.)" );
			pl.done();
		}

		final boolean isPermutation = isInjective && everywhereDefined && notEnlarged;
		
		return new RemappedImmutableGraph( map, g, isInjective, isPermutation, remappedNodes, destNumNodes, pseudoInverse );
	}

	/** Remaps the the graph nodes through a function specified via
	 *  an array.
	 * 
	 * @param g the graph to be transformed.
	 * @param f the transformation map.
	 * @return the transformed graph.
	 * @see #map(ImmutableGraph, int[], ProgressLogger)
	 */
	public static ImmutableGraph map( final ImmutableGraph g, final int f[] ) {
		return map( g, f, null );
	}

	/** Returns a symmetrized graph using an offline transposition.
	 * 
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @return the symmetrized graph.
	 * @see #symmetrizeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */
	public static ImmutableGraph symmetrizeOffline( final ImmutableGraph g, final int batchSize ) throws IOException {
		return symmetrizeOffline( g, batchSize, null, null );
	}

	/** Returns a symmetrized graph using an offline transposition.
	 * 
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return the symmetrized graph.
	 * @see #symmetrizeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */
	public static ImmutableGraph symmetrizeOffline( final ImmutableGraph g, final int batchSize, final File tempDir ) throws IOException {
		return symmetrizeOffline( g, batchSize, tempDir, null );
	}

	/** Returns a symmetrized graph using an offline transposition.
	 * 
	 * <P>The symmetrized graph is the union of a graph and of its transpose. This method will
	 * compute the transpose on-the-fly using {@link #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)}.  
	 * 
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the symmetrized graph.
	 */
	public static ImmutableGraph symmetrizeOffline( final ImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl ) throws IOException {
		return union( g, transposeOffline( g, batchSize, tempDir, pl ) );
	}


	/** Returns a symmetrized graph.
	 * 
	 * <P>The symmetrized graph is the union of a graph and of its transpose. This method will
	 * use the provided transposed graph, if any, instead of computing it on-the-fly.  
	 * 
	 * @param g the source graph.
	 * @param t the graph <code>g</code> transposed; if <code>null</code>, the transposed graph will be computed on the fly.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the symmetrized graph.
	 */
	public static ImmutableGraph symmetrize( final ImmutableGraph g, final ImmutableGraph t, final ProgressLogger pl ) {
		return t == null ?
			union( g, transpose( g, pl ) ) :
			union( g, t );
	}

	/** Returns a symmetrized graph.
	 * 
	 * <P>The symmetrized graph is the union of a graph and of its transpose. This method will
	 * use the provided transposed graph, if any, instead of computing it on-the-fly.  
	 * 
	 * @param g the source graph.
	 * @param t the graph <code>g</code> transposed; if <code>null</code>, the transposed graph will be computed on the fly.
	 * @return the symmetrized graph.
	 */
	public static ImmutableGraph symmetrize( final ImmutableGraph g, final ImmutableGraph t ) {
		return symmetrize( g, t, null );
	}

	/** Returns a symmetrized graph.
	 * 
	 * @param g the source graph.
	 * @param pl a progress logger.
	 * @return the symmetryzed graph.
	 * @see #symmetrize(ImmutableGraph, ImmutableGraph, ProgressLogger)
	 */
	public static ImmutableGraph symmetrize( final ImmutableGraph g, final ProgressLogger pl ) {
		return symmetrize( g, null, pl );
	}

	/** Returns a symmetrized graph.
	 * 
	 * @param g the source graph.
	 * @return the symmetryzed graph.
	 * @see #symmetrize(ImmutableGraph, ImmutableGraph, ProgressLogger)
	 */
	public static ImmutableGraph symmetrize( final ImmutableGraph g ) {
		return symmetrize( g, null, null );
	}

	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>.
	 * 
	 * <P>This method can process {@linkplain ImmutableGraph#loadOffline(CharSequence) offline graphs}).
	 * 
	 * @param g an immutable graph.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an immutable graph obtained by transposing <code>g</code>.
	 */	
	
	public static ImmutableGraph transpose( final ImmutableGraph g, final ProgressLogger pl ) {
	
		int i, j, d, a[];
	
		final int n = g.numNodes();
		final int numPred[] = new int[ n ];
	
		if ( pl != null ) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start( "Counting predecessors..." ); 
		}
		
		NodeIterator nodeIterator = g.nodeIterator();

		long m = 0; // Number of arcs, computed on the fly.
		
		for( i = n; i-- != 0; ) {
			nodeIterator.nextInt();
			d = nodeIterator.outdegree();
			a = nodeIterator.successorArray();
			m += d;
			while( d-- != 0 ) numPred[ a[ d ] ]++;
			if ( pl != null ) pl.lightUpdate();
		}
		
		if ( pl != null ) pl.done();
		
		final int pred[][] = new int[ n ][];
	
		if ( pl != null ) {
			pl.expectedUpdates = n;
			pl.start( "Allocating memory for predecessors..." );
		}

		for( i = n; i-- != 0; ) {
			if ( numPred[ i ] != 0 ) pred[ i ] = new int[ numPred[ i ] ];
			if ( pl != null ) pl.lightUpdate();
		}

		if ( pl != null ) pl.done();
	
		IntArrays.fill( numPred, 0 );
	
		if ( pl != null ) {
			pl.expectedUpdates = n;
			pl.start( "Computing predecessors..." );
		}

		nodeIterator = g.nodeIterator();

		for( i = n; i-- != 0; ) {
			j = nodeIterator.nextInt();
			d = nodeIterator.outdegree();
			a = nodeIterator.successorArray();
			while( d-- != 0 ) pred[ a[ d ] ][ numPred[ a[ d ] ]++ ] = j;
			if ( pl != null ) pl.update();
		}
		
		if ( pl != null ) pl.done();
	
		if ( pl != null ) {
			pl.expectedUpdates = n;
			pl.start( "Sorting predecessors..." );
		}

		for( i = n; i-- != 0; ) {
			if ( pred[ i ] != null ) Arrays.sort( pred[ i ] );
			if ( pl != null ) pl.lightUpdate();
		}
		
		if ( pl != null ) pl.done();
	
		final long numArcs = m;
		return new ImmutableGraph() {
			public int numNodes() { return n; }
			public long numArcs() { return numArcs; }
			public ImmutableGraph copy() { return this; }
			@Override
			public boolean randomAccess() { return true; }
			public int[] successorArray( final int x ) { return pred[ x ] != null ? pred[ x ] : IntArrays.EMPTY_ARRAY; }
			public int outdegree( final int x ) { return successorArray( x ).length; }
		};
	}	


	
	/* Provides a sequential immutable graph by merging batches on the fly. */
	public final static class BatchGraph extends ImmutableSequentialGraph {
		private final ObjectArrayList<File> batches;
		private final int n;
		private final long numArcs;
		
		public BatchGraph( final int n, final long m, final ObjectArrayList<File> batches ) {
			this.batches = batches;
			this.n = n;
			this.numArcs = m;
		}
		
		public int numNodes() { return n; }
		public long numArcs() { return numArcs; }
		
		public NodeIterator nodeIterator() {
			final int[] refArray = new int[ batches.size() ];
			final InputBitStream[] batchIbs = new InputBitStream[ refArray.length ]; 
			final int[] inputStreamLength = new int[ refArray.length ];
			final int[] prevTarget = new int[ refArray.length ];
			IntArrays.fill( prevTarget, -1 );
			// The indirect queue used to merge the batches.
			final IntHeapSemiIndirectPriorityQueue queue = new IntHeapSemiIndirectPriorityQueue( refArray );
			
			try {
				// We open all files and load the first element into the reference array.
				for( int i = 0; i < refArray.length; i++ ) {
					batchIbs[ i ] = new InputBitStream( batches.get( i ) ); 
					try {
						inputStreamLength[ i ] = batchIbs[ i ].readDelta();
						refArray[ i ] = batchIbs[ i ].readDelta();
					}
					catch ( IOException e ) {
						throw new RuntimeException( e );
					}
					
					queue.enqueue( i );
				}

				return new NodeIterator() {
					/** The last returned node. */
					private int last = -1;
					/** The outdegree of the current node (valid if {@link #last} is not -1). */
					private int outdegree;
					/** The successors of the current node (valid if {@link #last} is not -1); 
					 * only the first {@link #outdegree} entries are meaningful. */
					private int[] successor = IntArrays.EMPTY_ARRAY;
					
					@Override
					public int outdegree() {
						if ( last == -1 ) throw new IllegalStateException();
						return outdegree;
					}
					
					public boolean hasNext() {
						return last < n - 1;
					}
					
					@Override
					public int nextInt() {
						last++;
						int d = 0;
						int i;
						
						try {
							/* We extract elements from the queue as long as their target is equal
							 * to last. If during the process we exhaust a batch, we close it. */
							
							while( ! queue.isEmpty() && refArray[ i = queue.first() ] == last ) {
								successor = IntArrays.grow( successor, d + 1 );
								successor[ d ] = ( prevTarget[ i ] += batchIbs[ i ].readDelta() + 1 );
								if ( --inputStreamLength[ i ] == 0 ) {
									queue.dequeue();
									batchIbs[ i ].close();
									batchIbs[ i ] = null;
								}
								else {
									// We read a new source and update the queue.
									final int sourceDelta = batchIbs[ i ].readDelta();
									if ( sourceDelta != 0 ) {
										refArray[ i ] += sourceDelta;
										prevTarget[ i ] = -1;
										queue.changed();
									}
								}
								d++;
							}
							// Neither quicksort nor heaps are stable, so we reestablish order here.
							Arrays.sort( successor, 0, d );
							if ( d != 0 ) {
								int p = 0;
								for( int j = 1; j < d; j++ ) if ( successor[ p ] != successor[ j ] ) successor[ ++p ] = successor[ j ];
								d = p + 1;
							}
						}
						catch( IOException e ) {
							throw new RuntimeException( e );
						}
						
						outdegree = d;
						return last;
					}
					
					@Override
					public int[] successorArray() { 
						if ( last == -1 ) throw new IllegalStateException();
						return successor;
					}
					
					protected void finalize() throws Throwable {
						try {
							for( InputBitStream ibs: batchIbs ) if ( ibs != null ) ibs.close();
						}
						finally {
							super.finalize();
						}
					}

					
				};
			}
			catch( IOException e ) {
				throw new RuntimeException( e );
			}
		}
		
		protected void finalize() throws Throwable {
			try {
				for( File f : batches ) f.delete();
			}
			finally {
				super.finalize();
			}
		}

	};
	

	/** Sorts the given source and target arrays w.r.t. the target and stores them in a temporary file.  
	 * 
	 * @param n the index of the last element to be sorted (exclusive).
	 * @param source the source array.
	 * @param target the target array.
	 * @param tempDir a temporary directory where to store the sorted arrays, or <code>null</code>
	 * @param batches a list of files to which the batch file will be added.
	 * @return the number of pairs in the batch (might be less than <code>n</code> because duplicates are eliminated).
	 */
	
	public static int processBatch( final int n, final int[] source, final int[] target, final File tempDir, final List<File> batches ) throws IOException {

		IntArrays.radixSort( source, target, 0, n );
		
		final File batchFile = File.createTempFile( "batch", ".bitstream", tempDir );
		batchFile.deleteOnExit();
		batches.add( batchFile );
		final OutputBitStream batch = new OutputBitStream( batchFile );
		int u = 0;
		if ( n != 0 ) {
			// Compute unique pairs
			u = 1;
			for( int i = n - 1; i-- != 0; ) if ( source[ i ] != source[ i + 1 ] || target[ i ] != target[ i + 1 ] ) u++;
			batch.writeDelta( u );
			int prevSource = source[ 0 ];
			batch.writeDelta( prevSource );
			batch.writeDelta( target[ 0 ] );

			for( int i = 1; i < n; i++ ) {
				if ( source[ i ] != prevSource ) {
					batch.writeDelta( source[ i ] - prevSource );
					batch.writeDelta( target[ i ] );
					prevSource = source[ i ];
				}
				else if ( target[ i ] != target[ i - 1 ] ) {
					// We don't write duplicate pairs
					batch.writeDelta( 0 );
					if ( ASSERTS ) assert target[ i ] > target[ i - 1 ] : target[ i ] + "<=" + target[ i - 1 ]; 
					batch.writeDelta( target[ i ] - target[ i - 1 ] - 1 );
				}
			}
		}
		else batch.writeDelta( 0 );
		
		batch.close();
		return u;
	}
	
	/** Sorts the given source and target arrays w.r.t. the target and stores them in two temporary files. 
	 *  An additional positionable input bit stream is provided that contains labels, starting at given positions.
	 *  Labels are also written onto the appropriate file. 
	 * 
	 * @param n the index of the last element to be sorted (exclusive).
	 * @param source the source array.
	 * @param target the target array.
	 * @param start the array containing the bit position (within the given input stream) where the label of the arc starts.
	 * @param labelBitStream the positionable bit stream containing the labels.
	 * @param tempDir a temporary directory where to store the sorted arrays.
	 * @param batches a list of files to which the batch file will be added.
	 * @param labelBatches a list of files to which the label batch file will be added.
	 */
	
	private static void processTransposeBatch( final int n, final int[] source, final int[] target, final long[] start,
			final InputBitStream labelBitStream, final File tempDir, final List<File> batches, final List<File> labelBatches,
			final Label prototype ) throws IOException {
		it.unimi.dsi.fastutil.Arrays.quickSort( 0, n, new AbstractIntComparator() {
			public int compare( int x, int y ) { 
				final int t = source[ x ] - source[ y ];
				if ( t != 0 ) return t;
				return target[ x ] - target[ y ];
			}
		}, new Swapper() {
			public void swap( int x, int y ) {
				int t = source[ x ];
				source[ x ] = source[ y ];
				source[ y ] = t;
				t = target[ x ];
				target[ x ] = target[ y ];
				target[ y ] = t;
				long u = start[ x ];
				start[ x ] = start[ y ];
				start[ y ] = u;
			}
		} );

		final File batchFile = File.createTempFile( "batch", ".bitstream", tempDir );
		batchFile.deleteOnExit();
		batches.add( batchFile );
		final OutputBitStream batch = new OutputBitStream( batchFile );

		if ( n != 0 ) {
			// Compute unique pairs
			batch.writeDelta( n );
			int prevSource = source[ 0 ];
			batch.writeDelta( prevSource );
			batch.writeDelta( target[ 0 ] );

			for( int i = 1; i < n; i++ ) {
				if ( source[ i ] != prevSource ) {
					batch.writeDelta( source[ i ] - prevSource );
					batch.writeDelta( target[ i ] );
					prevSource = source[ i ];
				}
				else if ( target[ i ] != target[ i - 1 ] ) {
					// We don't write duplicate pairs
					batch.writeDelta( 0 );
					batch.writeDelta( target[ i ] - target[ i - 1 ] - 1 );
				}
			}
		}
		else batch.writeDelta( 0 );
		
		batch.close();

		final File labelFile = File.createTempFile( "label-", ".bits", tempDir );
		labelFile.deleteOnExit();
		labelBatches.add( labelFile );
		final OutputBitStream labelObs = new OutputBitStream( labelFile );
		for ( int i = 0; i < n; i++ ) {
			labelBitStream.position( start[ i ] );
			prototype.fromBitStream( labelBitStream, source[ i ] );
			prototype.toBitStream( labelObs, target[ i ] );
		}
		labelObs.close();
	}	
	
	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 * 
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */	
	
	public static ImmutableSequentialGraph transposeOffline( final ImmutableGraph g, final int batchSize ) throws IOException {
		return transposeOffline( g, batchSize, null );
	}
	
	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 * 
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */	
	
	public static ImmutableSequentialGraph transposeOffline( final ImmutableGraph g, final int batchSize, final File tempDir ) throws IOException {
		return transposeOffline( g, batchSize, tempDir, null );
	}
	
	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 * 
	 * <p>This method should be used to transpose very large graph in case {@link #transpose(ImmutableGraph)}
	 * requires too much memory. It creates a number of sorted batches on disk containing arcs 
	 * represented by a pair of integers in {@link java.io.DataInput} format ordered by target 
	 * and returns an {@link ImmutableGraph}
	 * that can be accessed only using a {@link ImmutableGraph#nodeIterator() node iterator}. The node iterator
	 * merges on the fly the batches, providing a transposed graph. The files are marked with
	 * {@link File#deleteOnExit()}, so they should disappear when the JVM exits. An additional safety-net
	 * finaliser tries to delete the batches, too.
	 * 
	 * <p>Note that each {@link NodeIterator} returned by the transpose requires opening all batches at the same time.
	 * The batches are closed when they are exhausted, so a complete scan of the graph closes them all. In any case,
	 * another safety-net finaliser closes all files when the iterator is collected.
	 * 
	 * <P>This method can process {@linkplain ImmutableGraph#loadOffline(CharSequence) offline graphs}. 
	 * 
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 */	
	
	public static ImmutableSequentialGraph transposeOffline( final ImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl ) throws IOException {
	
		int j, currNode;
		final int[] source = new int[ batchSize ] , target = new int[ batchSize ];
		final ObjectArrayList<File> batches = new ObjectArrayList<File>();
		
		final int n = g.numNodes();
		
		if ( pl != null ) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start( "Creating sorted batches..." );
		}
		
		final NodeIterator nodeIterator = g.nodeIterator();

		// Phase one: we scan the graph, accumulating pairs <source,target> and dumping them on disk.
		int succ[];
		long m = 0; // Number of arcs, computed on the fly.
		j = 0;
		for( long i = n; i-- != 0; ) {
			currNode = nodeIterator.nextInt();
			final int d = nodeIterator.outdegree();
			succ = nodeIterator.successorArray();
			m += d;
			
			for( int k = 0; k < d; k++  ) {
				target[ j ] = currNode;
				source[ j++ ] = succ[ k ];

				if ( j == batchSize ) {
					processBatch( batchSize, source, target, tempDir, batches );
					j = 0;
				}
			}
			
			
			if ( pl != null ) pl.lightUpdate();
		}

		if ( j != 0 ) processBatch( j, source, target, tempDir, batches );
		
		if ( pl != null ) {
			pl.done();
			logBatches( batches, m, pl );
		}

		return new BatchGraph( n, m, batches );
	}

	protected static void logBatches( final ObjectArrayList<File> batches, final long pairs, final ProgressLogger pl ) {
		long length = 0;
		for( File f : batches ) length += f.length();
		pl.logger.info( "Created " + batches.size() + " batches using " + Util.format( (double)Byte.SIZE * length / pairs ) + " bits/arc." );
	}

	/** Returns an immutable graph obtained by remapping offline the graph nodes through a partial function specified via an array.
	 * 
	 * @param g an immutable graph.
	 * @param map the transformation map.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @return an immutable, sequentially accessible graph obtained by transforming <code>g</code>.
	 * @see #mapOffline(ImmutableGraph, int[], int, File, ProgressLogger)
	 */	
	public static ImmutableSequentialGraph mapOffline( final ImmutableGraph g, final int map[], final int batchSize ) throws IOException {
		return mapOffline( g, map, batchSize, null );
	}
	
	/** Returns an immutable graph obtained by remapping offline the graph nodes through a partial function specified via an array.
	 * 
	 * @param g an immutable graph.
	 * @param map the transformation map.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible graph obtained by transforming <code>g</code>.
	 * @see #mapOffline(ImmutableGraph, int[], int, File, ProgressLogger)
	 */	
	public static ImmutableSequentialGraph mapOffline( final ImmutableGraph g, final int map[], final int batchSize, final File tempDir ) throws IOException {
		return mapOffline( g, map, batchSize, tempDir, null );
	}

	/** Returns an immutable graph obtained by remapping offline the graph nodes through a partial function specified via an array.
	 * 
	 * See {@link #map(ImmutableGraph, int[], ProgressLogger)} for the semantics of this method and {@link #transpose(ImmutableGraph, ProgressLogger)} for
	 * implementation and performance-related details.
	 * 
	 * @param g an immutable graph.
	 * @param map the transformation map.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an immutable, sequentially accessible graph obtained by transforming <code>g</code>.
	 */	
	public static ImmutableSequentialGraph mapOffline( final ImmutableGraph g, final int map[], final int batchSize, final File tempDir, final ProgressLogger pl ) throws IOException {
	
		int j, currNode;
		final int[] source = new int[ batchSize ] , target = new int[ batchSize ];
		final ObjectArrayList<File> batches = new ObjectArrayList<File>();
		
		final int n = g.numNodes();
		
		if ( pl != null ) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start( "Creating sorted batches..." );
		}
		
		final NodeIterator nodeIterator = g.nodeIterator();

		// Phase one: we scan the graph, accumulating pairs <map[source],map[target]> (if we have to) and dumping them on disk.
		int succ[];
		j = 0;
		long pairs = 0; // Number of pairs
		for( long i = n; i-- != 0; ) {
			currNode = nodeIterator.nextInt();
			if ( map[ currNode ] != -1 ) {
				final int d = nodeIterator.outdegree();
				succ = nodeIterator.successorArray();

				for( int k = 0; k < d; k++  ) {
					if ( map[ succ[ k ] ] != -1 ) {
						source[ j ] = map[ currNode ];
						target[ j++ ] = map[ succ[ k ] ];

						if ( j == batchSize ) {
							pairs += processBatch( batchSize, source, target, tempDir, batches );
							j = 0;
						}
					}
				}
			}
			
			if ( pl != null ) pl.lightUpdate();
		}

		if ( j != 0 ) pairs += processBatch( j, source, target, tempDir, batches );
		
		if ( pl != null ) {
			pl.done();
			logBatches( batches, pairs, pl );
		}

		return new BatchGraph( n, -1, batches );
	}
	
	/** Returns an arc-labelled immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 * 
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method,
	 * plus an additional {@link FastByteArrayOutputStream} needed to store all the labels for a batch.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)
	 */	
	public static ArcLabelledImmutableGraph transposeOffline( final ArcLabelledImmutableGraph g, final int batchSize ) throws IOException {
		return transposeOffline( g, batchSize, null );
	}

	/** Returns an arc-labelled immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 * 
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method,
	 * plus an additional {@link FastByteArrayOutputStream} needed to store all the labels for a batch.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)
	 */	
	public static ArcLabelledImmutableGraph transposeOffline( final ArcLabelledImmutableGraph g, final int batchSize, final File tempDir ) throws IOException {
		return transposeOffline( g, batchSize, tempDir, null );
	}

	
	/** Returns an arc-labelled immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 * 
	 * <p>This method should be used to transpose very large graph in case {@link #transpose(ImmutableGraph)}
	 * requires too much memory. It creates a number of sorted batches on disk containing arcs 
	 * represented by a pair of integers in {@link java.io.DataInput} format ordered by target 
	 * and returns an {@link ImmutableGraph}
	 * that can be accessed only using a {@link ImmutableGraph#nodeIterator() node iterator}. The node iterator
	 * merges on the fly the batches, providing a transposed graph. The files are marked with
	 * {@link File#deleteOnExit()}, so they should disappear when the JVM exits. An additional safety-net
	 * finaliser tries to delete the batches, too. As far as labels are concerned, they are temporarily stored in
	 * an in-memory bit stream, that is permuted when it is stored on the disk
	 * 
	 * <p>Note that each {@link NodeIterator} returned by the transpose requires opening all batches at the same time.
	 * The batches are closed when they are exhausted, so a complete scan of the graph closes them all. In any case,
	 * another safety-net finaliser closes all files when the iterator is collected.
	 * 
	 * <P>This method can process {@linkplain ArcLabelledImmutableGraph#loadOffline(CharSequence) offline graphs}. Note that
	 * no method to transpose on-line arc-labelled graph is provided currently. 
	 * 
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method,
	 * plus an additional {@link FastByteArrayOutputStream} needed to store all the labels for a batch.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 */	
	
	public static ArcLabelledImmutableGraph transposeOffline( final ArcLabelledImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl ) throws IOException {
	
		int i, j, d, currNode;
		final int[] source = new int[ batchSize ] , target = new int[ batchSize ];
		final long[] start = new long[ batchSize ];
		FastByteArrayOutputStream fbos = new FastByteArrayOutputStream();
		OutputBitStream obs = new OutputBitStream( fbos );
		final ObjectArrayList<File> batches = new ObjectArrayList<File>(), labelBatches = new ObjectArrayList<File>();
		final Label prototype = g.prototype().copy();
		
		final int n = g.numNodes();

		if ( pl != null ) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start( "Creating sorted batches..." );
		}
		
		final ArcLabelledNodeIterator nodeIterator = g.nodeIterator();

		// Phase one: we scan the graph, accumulating pairs <source,target> and dumping them on disk.
		int succ[];
		Label label[] = null;
		long m = 0; // Number of arcs, computed on the fly.
		j = 0;
		for( i = n; i-- != 0; ) {
			currNode = nodeIterator.nextInt();
			d = nodeIterator.outdegree();
			succ = nodeIterator.successorArray();
			label = nodeIterator.labelArray();
			m += d;
			
			for( int k = 0; k < d; k++  ) {
				source[ j ] = succ[ k ];
				target[ j ] = currNode;
				start[ j ] = obs.writtenBits();
				label[ k ].toBitStream( obs, currNode );
				j++;
				
				if ( j == batchSize ) {
					obs.flush();
					processTransposeBatch( batchSize, source, target, start, new InputBitStream( fbos.array ), tempDir, batches, labelBatches, prototype );
					fbos = new FastByteArrayOutputStream();
					obs = new OutputBitStream( fbos ); //ALERT here we should re-use 
					j = 0;
				}
			}
			
			
			if ( pl != null ) pl.lightUpdate();
		}
		
		if ( j != 0 ) {
			obs.flush();
			processTransposeBatch( j, source, target, start, new InputBitStream( fbos.array ), tempDir, batches, labelBatches, prototype );
		}
		
		if ( pl != null ) {
			pl.done();
			logBatches( batches, m, pl );
		}

		final long numArcs = m;

		// Now we return an immutable graph whose nodeIterator() merges the batches on the fly. 
		return new ArcLabelledImmutableSequentialGraph() {
			public int numNodes() { return n; }
			public long numArcs() { return numArcs; }
			
			public ArcLabelledNodeIterator nodeIterator() {
				final int[] refArray = new int[ batches.size() ];
				final InputBitStream[] batchIbs = new InputBitStream[ refArray.length ]; 
				final InputBitStream[] labelInputBitStream = new InputBitStream[ refArray.length ];
				final int[] inputStreamLength = new int[ refArray.length ];
				final int[] prevTarget = new int[ refArray.length ];
				IntArrays.fill( prevTarget, -1 );
				// The indirect queue used to merge the batches.
				final IntHeapSemiIndirectPriorityQueue queue = new IntHeapSemiIndirectPriorityQueue( refArray );
				
				try {
					// We open all files and load the first element into the reference array.
					for( int i = 0; i < refArray.length; i++ ) {
						inputStreamLength[ i ] = (int)( batches.get( i ).length() / ( Integer.SIZE / 8 ) );
						batchIbs[ i ] = new InputBitStream( batches.get( i ) ); 
						labelInputBitStream[ i ] = new InputBitStream( labelBatches.get( i ) );
						try {
							inputStreamLength[ i ] = batchIbs[ i ].readDelta();
							refArray[ i ] = batchIbs[ i ].readDelta();
						}
						catch ( IOException e ) {
							throw new RuntimeException( e );
						}
						
						queue.enqueue( i );
					}
					
					return new ArcLabelledNodeIterator() {
						/** The last returned node. */
						private int last = -1;
						/** The outdegree of the current node (valid if {@link #last} is not -1). */
						private int outdegree;
						/** The successors of the current node (valid if {@link #last} is not -1); 
						 * only the first {@link #outdegree} entries are meaningful. */
						private int[] successor = IntArrays.EMPTY_ARRAY;
						/** The labels of the arcs going out of the current node (valid if {@link #last} is not -1); 
						 * only the first {@link #outdegree} entries are meaningful. */
						@SuppressWarnings("hiding")
						private Label[] label = new Label[ 0 ];
						
						@Override
						public int outdegree() {
							if ( last == -1 ) throw new IllegalStateException();
							return outdegree;
						}
						
						public boolean hasNext() {
							return last < n - 1;
						}
						
						@Override
						public int nextInt() {
							last++;
							int d = 0;
							int i;
							
							try {
								/* We extract elements from the queue as long as their target is equal
								 * to last. If during the process we exhaust a batch, we close it. */
								
								while( ! queue.isEmpty() && refArray[ i = queue.first() ] == last ) {
									successor = IntArrays.grow( successor, d + 1 );
									successor[ d ] = ( prevTarget[ i ] += batchIbs[ i ].readDelta() + 1 );
									label = ObjectArrays.grow( label, d + 1 );
									label[ d ] = prototype.copy();
									label[ d ].fromBitStream( labelInputBitStream[ i ], last );
									
									if ( --inputStreamLength[ i ] == 0 ) {
										queue.dequeue();
										batchIbs[ i ].close();
										labelInputBitStream[ i ].close();
										batchIbs[ i ] = null;
										labelInputBitStream[ i ] = null;
									}
									else {
										// We read a new source and update the queue.
										final int sourceDelta = batchIbs[ i ].readDelta();
										if ( sourceDelta != 0 ) {
											refArray[ i ] += sourceDelta;
											prevTarget[ i ] = -1;
											queue.changed();
										}
									}
									d++;
								}
								// Neither quicksort nor heaps are stable, so we reestablish order here.
								it.unimi.dsi.fastutil.Arrays.quickSort( 0, d, new AbstractIntComparator() {
									public int compare( int x, int y ) {
										return successor[ x ] - successor[ y ];
									}
								}, new Swapper() {
									public void swap( int x, int y ) {
										final int t = successor[ x ];
										successor[ x ] = successor[ y ];
										successor[ y ] = t;
										final Label l = label[ x ];
										label[ x ] = label[ y ];
										label[ y ] = l;
									}
									
								});
							}
							catch( IOException e ) {
								throw new RuntimeException( e );
							}
							
							outdegree = d;
							return last;
						}
						
						@Override
						public int[] successorArray() { 
							if ( last == -1 ) throw new IllegalStateException();
							return successor;
						}
						
						protected void finalize() throws Throwable {
							try {
								for( InputBitStream ibs: batchIbs ) if ( ibs != null ) ibs.close();
								for( InputBitStream ibs: labelInputBitStream ) if ( ibs != null ) ibs.close();
							}
							finally {
								super.finalize();
							}
						}

						@Override
						public LabelledArcIterator successors() {
							if ( last == -1 ) throw new IllegalStateException();
							return new LabelledArcIterator() {
								@SuppressWarnings("hiding")
								int last = -1;

								public Label label() {
									return label[ last ];
								}

								public int nextInt() {
									if ( last + 1 == outdegree ) return -1;
									return successor[ ++last ];
								}

								public int skip( int k ) {
									int toSkip = Math.min( k, outdegree - last - 1 );
									last += toSkip;
									return toSkip;
								}
							};
						}
					};
				}
				catch( IOException e ) {
					throw new RuntimeException( e );
				}
			}
			
			protected void finalize() throws Throwable {
				try {
					for( File f : batches ) f.delete();
					for( File f : labelBatches ) f.delete();
				}
				finally {
					super.finalize();
				}
			}
			@Override
			public Label prototype() {
				return prototype;
			}

		};
	}
	
	
	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>.
	 * 
	 * <P>This method can process {@linkplain ImmutableGraph#loadOffline(CharSequence) offline graphs}.
	 * 
	 * @param g an immutable graph.
	 * @return an immutable graph obtained by transposing <code>g</code>.
	 * @see #transpose(ImmutableGraph, ProgressLogger)
	 */	
	
	public static ImmutableGraph transpose( ImmutableGraph g ) {
		return transpose( g, null );
	}

	/** Returns the union of two arc-labelled immutable graphs.
	 * 
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 * 
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @param labelMergeStrategy the strategy used to merge labels when the same arc
	 *  is present in both graphs; if <code>null</code>, {@link Labels#KEEP_FIRST_MERGE_STRATEGY}
	 *  is used.
	 * @return the union of the two graphs.
	 */
	public static ArcLabelledImmutableGraph union( final ArcLabelledImmutableGraph g0, final ArcLabelledImmutableGraph g1, final LabelMergeStrategy labelMergeStrategy ) {
		return new UnionArcLabelledImmutableGraph( g0, g1, labelMergeStrategy == null? Labels.KEEP_FIRST_MERGE_STRATEGY : labelMergeStrategy );
	}

	/** Returns the union of two immutable graphs.
	 * 
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 * 
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @return the union of the two graphs.
	 */
	public static ImmutableGraph union( final ImmutableGraph g0, final ImmutableGraph g1 ) {
		return g0 instanceof ArcLabelledImmutableGraph && g1 instanceof ArcLabelledImmutableGraph 
			? union( (ArcLabelledImmutableGraph)g0, (ArcLabelledImmutableGraph)g1, (LabelMergeStrategy)null )
					: new UnionImmutableGraph( g0, g1 );
	}


	/** Returns the composition (a.k.a. matrix product) of two immutable graphs.
	 * 
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 * 
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @return the composition of the two graphs.
	 */
	public static ImmutableGraph compose( final ImmutableGraph g0, final ImmutableGraph g1 ) {
		return new ImmutableSequentialGraph() {
			
			@Override
			public int numNodes() {
				return Math.max( g0.numNodes(), g1.numNodes() );
			}
			
			@Override
			public NodeIterator nodeIterator() {
				
				return new NodeIterator() {
					private final NodeIterator it0 = g0.nodeIterator();
					private int[] succ = IntArrays.EMPTY_ARRAY;
					private IntOpenHashSet successors = new IntOpenHashSet( Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR );
					private int outdegree = -1; // -1 means that the cache is empty
					
					@Override
					public int nextInt() {
						outdegree = -1;
						return it0.nextInt();
					}

					public boolean hasNext() {
						return it0.hasNext();
					}

					
					@Override
					public int outdegree() {
						if ( outdegree < 0 ) successorArray();
						return outdegree;
					}

					@Override
					public int[] successorArray() {
						if ( outdegree < 0 ) {
							final int d = it0.outdegree();
							final int[] s = it0.successorArray();
							successors.clear();
							for ( int i = 0; i < d; i++ ) { 
								LazyIntIterator s1 = g1.successors( s[ i ] );
								int x;
								while ( ( x = s1.nextInt() ) >= 0 ) successors.add( x );
							}
							outdegree = successors.size();
							succ = IntArrays.ensureCapacity( succ, outdegree, 0 );
							successors.toArray( succ );
							Arrays.sort( succ, 0, outdegree );
						}
						return succ;
					}
				};
			}
			
		};
	}
	
	
	/** Returns the composition (a.k.a. matrix product) of two arc-labelled immutable graphs.
	 * 
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 * 
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @param strategy a label semiring.
	 * @return the composition of the two graphs.
	 */
	public static ArcLabelledImmutableGraph compose( final ArcLabelledImmutableGraph g0, final ArcLabelledImmutableGraph g1, final LabelSemiring strategy ) {
		if ( g0.prototype().getClass() != g1.prototype().getClass() ) throw new IllegalArgumentException( "The two graphs have different label classes (" + g0.prototype().getClass().getSimpleName() + ", " +g1.prototype().getClass().getSimpleName() + ")" );

		return new ArcLabelledImmutableSequentialGraph() {
			
			@Override
			public Label prototype() {
				return g0.prototype();
			}
			
			@Override
			public int numNodes() {
				return Math.max( g0.numNodes(), g1.numNodes() );
			}
			
			@Override
			public ArcLabelledNodeIterator nodeIterator() {
				
				return new ArcLabelledNodeIterator() {
					private final ArcLabelledNodeIterator it0 = g0.nodeIterator();
					private int[] succ = IntArrays.EMPTY_ARRAY;
					private Label[] label = new Label[ 0 ];
					private int maxOutDegree;
					private int smallCount;
					private Int2ObjectOpenHashMap<Label> successors = new Int2ObjectOpenHashMap<Label>( Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR );
					{
						successors.defaultReturnValue( strategy.zero() );
					}
					private int outdegree = -1; // -1 means that the cache is empty
					
					@Override
					public int nextInt() {
						outdegree = -1;
						return it0.nextInt();
					}

					public boolean hasNext() {
						return it0.hasNext();
					}

					
					@Override
					public int outdegree() {
						if ( outdegree < 0 ) successorArray();
						return outdegree;
					}

					private void ensureCache() {
						if ( outdegree < 0 ) {
							final int d = it0.outdegree();
							final LabelledArcIterator s = it0.successors();
							if ( successors.size() < maxOutDegree / 2 && smallCount++ > 100 ) {
								smallCount = 0;
								maxOutDegree = 0;
								successors = new Int2ObjectOpenHashMap<Label>( Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR );
								successors.defaultReturnValue( strategy.zero() );
							}
							else successors.clear();
							
							for ( int i = 0; i < d; i++ ) { 
								LabelledArcIterator s1 = g1.successors( s.nextInt() );
								int x;
								while ( ( x = s1.nextInt() ) >= 0 ) successors.put( x, strategy.add( strategy.multiply( s.label(), s1.label() ), successors.get( x ) ) );
							}
							outdegree = successors.size();
							succ = IntArrays.ensureCapacity( succ, outdegree, 0 );
							label = ObjectArrays.ensureCapacity( label, outdegree, 0 );
							successors.keySet().toIntArray( succ );
							Arrays.sort( succ, 0, outdegree );
							for( int i = outdegree; i-- != 0; ) label[ i ] = successors.get( succ[ i ] );
							if ( outdegree > maxOutDegree ) maxOutDegree = outdegree;
						}
					}
					
					@Override
					public int[] successorArray() {
						ensureCache();
						return succ;
					}
					
					@Override
					public Label[] labelArray() {
						ensureCache();
						return label;
					}
					
					@Override
					public LabelledArcIterator successors() {
						ensureCache();
						return new LabelledArcIterator() {
							int i = -1;
							public Label label() {
								return label[ i ];
							}

							public int nextInt() {
								return i < outdegree - 1 ? succ[ ++i ] : -1;
							}

							public int skip( final int n ) {
								final int incr = Math.min( n, outdegree - i - 1 );
								i += incr;
								return incr;
							}
						};
					}
				};
			}
		};
	}
	
	/** Computes the line graph of a given symmetric graph. The line graph of <var>g</var> is a graph, whose nodes are
	 *  identified with pairs of the form &lt;<var>x</var>,&nbsp;<var>y</var>> where <var>x</var> and <var>y</var> are nodes of <var>g</var>
	 *  and &lt;<var>x</var>,&nbsp;<var>y</var>> is an arc of <var>g</var>. Moreover, there is an arc from &lt;<var>x</var>,&nbsp;<var>y</var>> to 
	 *  &lt;<var>y</var>,&nbsp;<var>z</var>>.
	 * 
	 * <P>Two additional files are created, with names stemmed from <code>mapBasename</code>; the <var>i</var>-th entries of the two files
	 * identify the source and target node (in the original graph) corresponding the node <var>i</var> in the line graph. 
	 * 
	 * @param g the graph (it must be symmetric and loopless).
	 * @param mapBasename the basename of two files that will, at the end, contain as many integers as the number of nodes in the line graph: the <var>i</var>-th
	 *   integer in the file <code><var>mapBasename</var>.source</code> will contain the source of the arc corresponding to the <var>i</var>-th
	 *   node in the line graph, and similarly <code><var>mapBasename</var>.target</code> will give the target.
	 * @param tempDir the temporary directory to be used.
	 * @param batchSize the size used for batches.
	 * @param pl the progress logger to be used.
	 * @return the line graph of <code>g</code>.
	 * @throws IOException 
	 */
	public static ImmutableSequentialGraph line( final ImmutableGraph g, final String mapBasename, final File tempDir, final int batchSize, final ProgressLogger pl ) throws IOException {
		final int n = g.numNodes();
		final int[] source = new int[ batchSize ] , target = new int[ batchSize ];
		int currBatch = 0, pairs = 0;
		final ObjectArrayList<File> batches = new ObjectArrayList<File>();
		final long[] edge = new long[ (int)g.numArcs() ];
		int edgesSoFar = 0;
		NodeIterator nodeIterator = g.nodeIterator();
		if ( pl != null ) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start( "Producing batches for line graph" );
		}
		long expNumberOfArcs = 0;
		while ( nodeIterator.hasNext() ) {
			int x = nodeIterator.nextInt();
			int d = nodeIterator.outdegree();
			expNumberOfArcs += d * d;
			int[] succ = nodeIterator.successorArray();
			// New edges
			for ( int i = 0; i < d; i++ ) {
				if ( succ[ i ] == x ) throw new IllegalArgumentException( "The graph contains a loop on node " + x );
				edge[ edgesSoFar++ ] = ( (long)x << 32 ) | succ[ i ];
			}
		}
		LOGGER.info( "Expected number of arcs: " + expNumberOfArcs );
		Arrays.sort( edge );
		nodeIterator = g.nodeIterator();

		while ( nodeIterator.hasNext() ) {
			int x = nodeIterator.nextInt();
			int d = nodeIterator.outdegree();
			int[] succ = nodeIterator.successorArray().clone();
			for ( int i = 0; i < d; i++ ) {
				int from0 = x; //Math.min( x, succ[ i ] );
				int to0 = succ[ i ]; //Math.max( x, succ[ i ] );
				int edge0 = LongArrays.binarySearch( edge, 0, edgesSoFar, ( (long)from0 << 32 ) | to0 );
				if ( ASSERTS ) assert edge0 >= 0;
				int dNext = g.outdegree( to0 );
				int[] succNext = g.successorArray( to0 );
				for ( int j = 0; j < dNext; j++ ) {
					int from1 = to0;  //Math.min( x, succ[ j ] );
					int to1 = succNext[ j ]; //Math.max( x, succ[ j ] );
					int edge1 = LongArrays.binarySearch( edge, 0, edgesSoFar, ( (long)from1 << 32 ) | to1 );
					if ( ASSERTS ) assert edge1 >= 0;
					if ( currBatch == batchSize ) {
						pairs += processBatch( batchSize, source, target, tempDir, batches );
						currBatch = 0;												
					}
					source[ currBatch ] = edge0;
					target[ currBatch++ ] = edge1;
				}
			}
			if ( pl != null ) pl.lightUpdate();
		}
		if ( currBatch > 0 )  {
			pairs += processBatch( currBatch, source, target, tempDir, batches );
			currBatch = 0;						
		}
		if ( edgesSoFar != edge.length ) throw new IllegalArgumentException( "Something went wrong (probably the graph was not symmetric)" );
		DataOutputStream dosSource = new DataOutputStream( new BufferedOutputStream( new FileOutputStream( mapBasename + ".source" ) ) );
		DataOutputStream dosTarget = new DataOutputStream( new BufferedOutputStream( new FileOutputStream( mapBasename + ".target" ) ) );
		for ( long e: edge ) {
			dosSource.writeInt( (int)( e >> 32 ) );
			dosTarget.writeInt( (int)( e & 0xFFFFFFFF ) );
		}
		dosSource.close();
		dosTarget.close();
		if ( DEBUG )
			for ( int i = 0; i < edgesSoFar; i++ ) {
				System.out.println( i + " <- (" + ( edge[ i ] >> 32 ) + "," + ( edge[ i ] & 0xFFFFFFFF ) +")" );
			}
		if ( pl != null ) {
			pl.done();
			logBatches( batches, pairs, pl );
		}
		return new BatchGraph( edgesSoFar, -1, batches );
	}
	
	/** Returns a permutation that would make the given graph adjacency lists in Gray-code order.
	 * 
	 * <P>Gray codes list all sequences of <var>n</var> zeros and ones in such a way that
	 * adjacent lists differ by exactly one bit. If we assign to each row of the adjacency matrix of 
	 * a graph its index as a Gray code, we obtain a permutation that will make similar lines
	 * nearer.
	 * 
	 * <P>Note that since a graph permutation permutes <em>both</em> rows and columns, this transformation is
	 * not idempotent: the Gray-code permutation produced from a matrix that has been Gray-code sorted will
	 * <em>not</em> be, in general, the identity.
	 * 
	 * <P>The important feature of Gray-code ordering is that it is completely endogenous (e.g., determined
	 * by the graph itself), contrarily to, say, lexicographic URL ordering (which relies on the knowledge
	 * of the URL associated to each node).
	 * 
	 * @param g an immutable graph.
	 * @return the permutation that would order the graph adjacency lists by Gray order
	 * (you can just pass it to {@link #map(ImmutableGraph, int[], ProgressLogger)}).
	 */ 
	public static int[] grayCodePermutation( final ImmutableGraph g ) {
		final int n = g.numNodes();
		final int[] perm = new int[ n ];
		int i = n;
		while( i-- != 0 ) perm[ i ] = i;
		
		final IntComparator grayComparator =  new AbstractIntComparator() {
			/* Remember that given a Gray code G (expressed as a 0-based sequence
			 * of n bits G[i]), the corresponding binary code B if defined as
			 * follows: B[n-1]=G[n-1], and B[i] = B[i+1] ^ G[i].
			 *
			 * Translating the formula above to our case (where we just have the increasing
			 * list of indices j such that G[i]=1), we see that the binary code
			 * corresponding to the Gray code of an adjacency list is 
			 * made of alternating blocks of zeroes and ones; the alternation
			 * happens at each successor. 
			 * 
			 * Said that, the code below requires some reckoning to be fully 
			 * understood (but it works!).
			 */

			public int compare( final int x, final int y ) {
				final LazyIntIterator i = g.successors( x ), j = g.successors( y );
				int a, b;

				/* This code duplicates eagerly of the behaviour of the lazy comparator
				   below. It is here for documentation and debugging purposes.
				
				byte[] g1 = new byte[ g.numNodes() ], g2 = new byte[ g.numNodes() ];
				while( i.hasNext() ) g1[ g.numNodes() - 1 - i.nextInt() ] = 1;
				while( j.hasNext() ) g2[ g.numNodes() - 1 - j.nextInt() ] = 1;
				for( int k = g.numNodes() - 2; k >= 0; k-- ) {
					g1[ k ] ^= g1[ k + 1 ];
					g2[ k ] ^= g2[ k + 1 ];
				}
				for( int k = g.numNodes() - 1; k >= 0; k-- ) if ( g1[ k ] != g2[ k ] ) return g1[ k ] - g2[ k ];
				return 0;
				*/
				
				boolean parity = false; // Keeps track of the parity of number of arcs before the current ones.
				for( ;; ) {
					a = i.nextInt();
					b = j.nextInt();
					if ( a == -1 && b == -1 ) return 0;
					if ( a == -1 ) return parity ? 1 : -1;
					if ( b == -1 ) return parity ? -1 : 1;
					if ( a != b ) return parity ^ ( a < b ) ? 1 : -1;
					parity = ! parity;
				}				
			}
		};
		
		IntArrays.quickSort( perm, 0, n, grayComparator );
		
		if ( ASSERTS ) for( int k = 0; k < n - 1; k++ ) assert grayComparator.compare( perm[ k ], perm[ k + 1 ] ) <= 0;  
		
		final int[] invPerm = new int[ n ];
		i = n;
		while( i-- != 0 ) invPerm[ perm[ i ] ] = i;
		
		return invPerm;
	}

	/** Returns a random permutation for a given graph.
	 * 
	 * @param g an immutable graph.
	 * @param seed for {@link Random}.
	 * @return a random permutation for the given graph
	 */ 
	public static int[] randomPermutation( final ImmutableGraph g, final long seed ) {
		return IntArrays.shuffle( Util.identity( g.numNodes() ), new XorShiftStarRandom( seed ) );
	}

	/** Returns a permutation that would make the given graph adjacency lists in host-by-host Gray-code order.
	 * 
	 * <p>This permutation differs from {@link #grayCodePermutation(ImmutableGraph)} in that Gray codes
	 * are computed host by host. There are two variants, <em>strict</em> and <em>loose</em>. In the first case,
	 * we restrict the adjacency matrix to the submatrix corresponding to a host and compute the ordering. In
	 * the second case, we just restrict to the rows corresponding to a host, but then entire rows are used
	 * to compute the ordering.
	 * 
	 * @param g an immutable graph.
	 * @param hostMap an array mapping each URL to its host (it is sufficient that each host is assigned a distinct number).
	 * @param strict if true, host-by-host Gray code computation will be strict, that is, the order is computed only
	 * between columns of the same host of the rows.
	 * @return the permutation that would order the graph adjacency lists by host-by-host Gray order
	 * (you can just pass it to {@link #map(ImmutableGraph, int[], ProgressLogger)}).
	 */ 
	public static int[] hostByHostGrayCodePermutation( final ImmutableGraph g, final int[] hostMap, final boolean strict ) {
		final int n = g.numNodes();
		final int[] perm = new int[ n ];
		int i = n;
		while( i-- != 0 ) perm[ i ] = i;
		
		final IntComparator hostByHostGrayComparator =  new AbstractIntComparator() {

			public int compare( final int x, final int y ) {
				final int t = hostMap[ x ] - hostMap[ y ];
				if ( t != 0 ) return t;
				final LazyIntIterator i = g.successors( x ), j = g.successors( y );
				int a, b;
				
				boolean parity = false; // Keeps track of the parity of number of arcs before the current ones.
				for( ;; ) {
					if ( strict ) {
						final int h = hostMap[ x ];
						do a = i.nextInt(); while( a != -1 && hostMap[ a ] != h );
						do b = j.nextInt(); while( b != -1 && hostMap[ b ] != h );
					}
					else {
						a = i.nextInt();
						b = j.nextInt();
					}
					if ( a == -1 && b == -1 ) return 0;
					if ( a == -1 ) return parity ? 1 : -1;
					if ( b == -1 ) return parity ? -1 : 1;
					if ( a != b ) return parity ^ ( a < b ) ? 1 : -1;
					parity = ! parity;
				}				
			}
		};
		
		IntArrays.quickSort( perm, 0, n, hostByHostGrayComparator );
		
		if ( ASSERTS ) for( int k = 0; k < n - 1; k++ ) assert hostByHostGrayComparator.compare( perm[ k ], perm[ k + 1 ] ) <= 0;  
		
		final int[] invPerm = new int[ n ];
		i = n;
		while( i-- != 0 ) invPerm[ perm[ i ] ] = i;
		
		return invPerm;
	}


	
	/** Returns a permutation that would make the given graph adjacency lists in lexicographical order.
	 * 
	 * <P>Note that since a graph permutation permutes <em>both</em> rows and columns, this transformation is
	 * not idempotent: the lexicographical permutation produced from a matrix that has been
	 * lexicographically sorted will
	 * <em>not</em> be, in general, the identity.
	 * 
	 * <P>The important feature of lexicographical ordering is that it is completely endogenous (e.g., determined
	 * by the graph itself), contrarily to, say, lexicographic URL ordering (which relies on the knowledge
	 * of the URL associated to each node).
	 * 
	 * <p><strong>Warning</strong>: rows are numbered from zero <em>from the left</em>. This means,
	 * for instance, that nodes with an arc towards node zero are lexicographically smaller
	 * than nodes without it.
	 * 
	 * @param g an immutable graph.
	 * @return the permutation that would order the graph adjacency lists by lexicographical order
	 * (you can just pass it to {@link #map(ImmutableGraph, int[], ProgressLogger)}).
	 */ 
	public static int[] lexicographicalPermutation( final ImmutableGraph g ) {
		final int n = g.numNodes();
		final int[] perm = new int[ n ];
		int i = n;
		while( i-- != 0 ) perm[ i ] = i;
		
		final IntComparator lexicographicalComparator =  new AbstractIntComparator() {
			public int compare( final int x, final int y ) {
				final LazyIntIterator i = g.successors( x ), j = g.successors( y );
				int a, b;
				for( ;; ) {
					a = i.nextInt();
					b = j.nextInt();
					if ( a == -1 && b == -1 ) return 0;
					if ( a == -1 ) return -1;
					if ( b == -1 ) return 1;
					if ( a != b ) return b - a;
				}				
			}
		};
		
		IntArrays.quickSort( perm, 0, n, lexicographicalComparator );
		
		if ( ASSERTS ) for( int k = 0; k < n - 1; k++ ) assert lexicographicalComparator.compare( perm[ k ], perm[ k + 1 ] ) <= 0;  
		
		final int[] invPerm = new int[ n ];
		i = n;
		while( i-- != 0 ) invPerm[ perm[ i ] ] = i;
		
		return invPerm;
	}


	
	/** Ensures that the arguments are exactly <code>n</code>, if <code>n</code> is nonnegative, or
	 * at least -<code>n</code>, otherwise.
	 */

	private static boolean ensureNumArgs( String param[], int n ) {
		if ( n >= 0 && param.length != n || n < 0 && param.length < -n ) {
			System.err.println( "Wrong number (" + param.length + ") of arguments." );
			return false;
		}
		return true;
	}

	/** Loads a graph with given data and returns it.
	 *  
	 * @param graphClass the class of the graph to be loaded.
	 * @param baseName the graph basename. 
	 * @param sequential whether the graph is to be loaded in a sequential fashion.
	 * @param offline whether the graph is to be loaded in an offline fashion.
	 * @param pl a progress logger.
	 * @return the loaded graph.
	 */
	private static ImmutableGraph load( Class<?> graphClass, String baseName, boolean sequential, boolean offline, ProgressLogger pl ) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException {	
		ImmutableGraph graph = null;

		if ( graphClass != null ) {
			if ( offline ) graph = (ImmutableGraph)graphClass.getMethod( "loadOffline", CharSequence.class ).invoke( null, baseName );
			else if ( sequential ) graph = (ImmutableGraph)graphClass.getMethod( "loadSequential", CharSequence.class, ProgressLogger.class ).invoke( null, baseName, pl );
			else graph = (ImmutableGraph)graphClass.getMethod( "load", CharSequence.class, ProgressLogger.class ).invoke( null, baseName, pl );
		}
		else graph = offline ? 
			ImmutableGraph.loadOffline( baseName ) : 
			sequential ? ImmutableGraph.loadSequential( baseName, pl ) :
			ImmutableGraph.load( baseName, pl );
		
		return graph;
	}


	public static void main( String args[] ) throws IOException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, JSAPException {
		Class<?> sourceGraphClass = null, destGraphClass = BVGraph.class;
		boolean offline = false, sequential = false, ascii = false;

		final Field[] field = Transform.class.getDeclaredFields();
		final List<String> filterList = new ArrayList<String>();
		final List<String> labelledFilterList = new ArrayList<String>();
		
		for( Field f: field ) {
			if ( ArcFilter.class.isAssignableFrom( f.getType() ) ) filterList.add( f.getName() );
			if ( LabelledArcFilter.class.isAssignableFrom( f.getType() ) ) labelledFilterList.add( f.getName() );
		}

		SimpleJSAP jsap = new SimpleJSAP( Transform.class.getName(), 
				"Transforms one or more graphs. All transformations require, after the name,\n" +
				"some parameters specified below:\n" +
				"\n" +
				"identity                  sourceBasename destBasename\n" +
				"map                       sourceBasename destBasename map [cutoff]\n" +
				"mapOffline                sourceBasename destBasename map [batchSize] [tempDir]\n" +
				"transpose                 sourceBasename destBasename\n" +
				"transposeOffline          sourceBasename destBasename [batchSize] [tempDir]\n" +
				"symmetrize                sourceBasename [transposeBasename] destBasename\n" +
				"symmetrizeOffline         sourceBasename destBasename [batchSize] [tempDir]\n" +
				"union                     source1Basename source2Basename destBasename [strategy]\n" +
				"compose                   source1Basename source2Basename destBasename [semiring]\n" +
				"gray                      sourceBasename destBasename\n" +
				"grayPerm                  sourceBasename dest\n" +
				"strictHostByHostGray      sourceBasename destBasename hostMap\n" +
				"strictHostByHostGrayPerm  sourceBasename dest hostMap\n" +
				"looseHostByHostGray       sourceBasename destBasename hostMap\n" +
				"looseHostByHostGrayPerm   sourceBasename dest hostMap\n" +
				"lex                       sourceBasename destBasename\n" +
				"lexPerm                   sourceBasename dest\n" +
				"line                      sourceBasename destBasename mapName [batchSize]\n" +
				"random                    sourceBasename destBasename\n" +
				"arcfilter                 sourceBasename destBasename arcFilter (available filters: " + filterList + ")\n" +
				"larcfilter                sourceBasename destBasename arcFilter (available filters: " + labelledFilterList + ")\n" +
				"\n" +
				"Please consult the Javadoc documentation for more information on each transform.",
				new Parameter[] {
						new FlaggedOption( "sourceGraphClass", GraphClassParser.getParser(), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "source-graph-class", "Forces a Java class to load the source graph." ),
						new FlaggedOption( "destGraphClass", GraphClassParser.getParser(), BVGraph.class.getName(), JSAP.NOT_REQUIRED, 'd', "dest-graph-class", "Forces a Java class to store the destination graph." ),
						new FlaggedOption( "destArcLabelledGraphClass", GraphClassParser.getParser(), BitStreamArcLabelledImmutableGraph.class.getName(), JSAP.NOT_REQUIRED, 'L', "dest-arc-labelled-graph-class", "Forces a Java class to store the labels of the destination graph." ),
						new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
						new Switch( "offline", 'o', "offline", "Use the offline load method to reduce memory consumption." ),
						new Switch( "sequential", 'S', "sequential", "Use the sequential load method to reduce memory consumption." ),
						new Switch( "ascii", 'a', "ascii", "Maps are in ASCII form (one integer per line)." ),
						new UnflaggedOption( "transform", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The transformation to be applied." ),
						new UnflaggedOption( "param", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The remaining parameters." ),
					}	
				);
		
		JSAPResult jsapResult = jsap.parse( args );
		if ( jsap.messagePrinted() ) System.exit( 1 );

		sourceGraphClass = jsapResult.getClass( "sourceGraphClass");
		destGraphClass = jsapResult.getClass( "destGraphClass");
		offline = jsapResult.getBoolean( "offline" );
		sequential = jsapResult.getBoolean( "sequential" );
		ascii = jsapResult.getBoolean( "ascii" );
		String transform = jsapResult.getString( "transform" );
		String[] param = jsapResult.getStringArray( "param" );
		
		String source[] = null, dest = null, map = null;
		ArcFilter arcFilter = null;
		LabelledArcFilter labelledArcFilter = null;
		LabelSemiring labelSemiring = null;
		LabelMergeStrategy labelMergeStrategy = null;
		int batchSize = 1000000, cutoff = -1;
		long seed = 0;
		File tempDir = null;
		
		if ( ! ensureNumArgs( param, -2 ) ) return;
		
		if ( transform.equals( "identity" ) || transform.equals( "transpose" ) || transform.equals( "removeDangling" ) || transform.equals( "gray" ) || transform.equals( "grayPerm" ) || transform.equals( "lex" ) || transform.equals( "lexPerm" ) ) {
			source = new String[] { param[ 0 ] };
			dest = param[ 1 ];
			if ( ! ensureNumArgs( param, 2 ) ) return;
		} 
		else if ( transform.equals( "map" ) || transform.equals( "strictHostByHostGray" ) || transform.equals( "strictHostByHostGrayPerm" ) || transform.equals( "looseHostByHostGray" ) || transform.equals( "looseHostByHostGrayPerm" ) ) {
			if ( ! ensureNumArgs( param, -3 ) ) return;
			source = new String[] { param[ 0 ] };
			dest = param[ 1 ];
			map = param[ 2 ];
			if ( param.length == 4 ) cutoff = Integer.parseInt( param[ 3 ] );
		}
		else if ( transform.equals( "mapOffline" ) ) {
			if ( ! ensureNumArgs( param, -3 ) ) return;
			source = new String[] { param[ 0 ] };
			dest = param[ 1 ];
			map = param[ 2 ];
			if ( param.length >= 4 ) {
				batchSize = ((Integer)JSAP.INTSIZE_PARSER.parse( param[ 3 ] )).intValue();
				if ( param.length == 5 ) tempDir = new File( param[ 4 ] );
				else if ( ! ensureNumArgs( param, 4 ) )	return;
			}
			else if ( ! ensureNumArgs( param, 3 ) )	return;
		}
		else if ( transform.equals( "symmetrize" ) ) {
			if ( param.length == 2 ) {
				source = new String[] { param[ 0 ], null };
				dest = param[ 1 ];
			} 
			else if ( ensureNumArgs( param, 3 ) ) {
				source = new String[] { param[ 0 ], param[ 1 ] };
				dest = param[ 2 ];
			}
			else return;
		} 
		else if ( transform.equals( "random" ) ) {
			if ( param.length == 2 ) {
				source = new String[] { param[ 0 ], null };
				dest = param[ 1 ];
			} 
			else if ( ensureNumArgs( param, 3 ) ) {
				source = new String[] { param[ 0 ] };
				dest = param[ 1 ];
				seed = Long.parseLong( param[ 2 ] );
			}
			else return;
		} 
		else if ( transform.equals( "arcfilter" ) ) {
			if ( ensureNumArgs( param, 3 ) ) {
				try {
					// First try: a public field
					arcFilter = (ArcFilter) Transform.class.getField( param[ 2 ] ).get( null );
				}
				catch( NoSuchFieldException e ) {
					// No chance: let's try with a class
					arcFilter = ObjectParser.fromSpec( param[ 2 ], ArcFilter.class, GraphClassParser.PACKAGE );
				}
				source = new String[] { param[ 0 ], null };
				dest = param[ 1 ];
			} 
			else return;
		} 
		else if ( transform.equals( "larcfilter" ) ) {
			if ( ensureNumArgs( param, 3 ) ) {
				try {
					// First try: a public field
					labelledArcFilter = (LabelledArcFilter) Transform.class.getField( param[ 2 ] ).get( null );
				}
				catch( NoSuchFieldException e ) {
					// No chance: let's try with a class
					labelledArcFilter = ObjectParser.fromSpec( param[ 2 ], LabelledArcFilter.class, GraphClassParser.PACKAGE );
				}
				source = new String[] { param[ 0 ], null };
				dest = param[ 1 ];
			} 
			else return;
		} 
		else if ( transform.equals( "union" ) ) {
			if ( ! ensureNumArgs( param, -3 ) ) return;
			source = new String[] { param[ 0 ], param[ 1 ] };
			dest = param[ 2 ];
			if ( param.length == 4 ) labelMergeStrategy = ObjectParser.fromSpec( param[ 3 ], LabelMergeStrategy.class, GraphClassParser.PACKAGE );
			else if ( ! ensureNumArgs( param, 3 ) ) return;
		}
		else if ( transform.equals( "compose" ) ) {
			if ( ! ensureNumArgs( param, -3 ) ) return;
			source = new String[] { param[ 0 ], param[ 1 ] };
			dest = param[ 2 ];
			if ( param.length == 4 ) labelSemiring = ObjectParser.fromSpec( param[ 3 ], LabelSemiring.class, GraphClassParser.PACKAGE );
			else if ( ! ensureNumArgs( param, 3 ) ) return;
		}
		else if ( transform.equals( "transposeOffline" ) || transform.equals( "symmetrizeOffline" ) ) {
			if ( ! ensureNumArgs( param, -2 ) ) return;
			source = new String[] { param[ 0 ] };
			dest = param[ 1 ];
			if ( param.length >= 3 ) {
				batchSize = ((Integer)JSAP.INTSIZE_PARSER.parse( param[ 2 ] )).intValue();
				if ( param.length == 4 ) tempDir = new File( param[ 3 ] );
				else if ( ! ensureNumArgs( param, 3 ) )	return;
			}
			else if ( ! ensureNumArgs( param, 2 ) )	return;
		}
		else if ( transform.equals( "line" ) ) {
			if ( ! ensureNumArgs( param, -3 ) ) return;
			source = new String[] { param[ 0 ] };
			dest = param[ 1 ];
			map = param[ 2 ];
			if ( param.length == 4 ) batchSize = Integer.parseInt( param[ 3 ] );
		}
		else {
			System.err.println( "Unknown transform: " + transform );
			return;
		}

		final ProgressLogger pl = new ProgressLogger( LOGGER, jsapResult.getLong( "logInterval" ) );
		final ImmutableGraph[] graph = new ImmutableGraph[ source.length ];
		final ImmutableGraph result;
		final Class<?> destLabelledGraphClass = jsapResult.getClass( "destArcLabelledGraphClass" );
		if ( ! ArcLabelledImmutableGraph.class.isAssignableFrom( destLabelledGraphClass ) ) throw new IllegalArgumentException( "The arc-labelled destination class " + destLabelledGraphClass.getName() + " is not an instance of ArcLabelledImmutableGraph" );
		
		for ( int i = 0; i < source.length; i++ ) 
			// Note that composition requires the second graph to be always random access.
			if ( source[ i ] == null ) graph[ i ] = null;
			else graph[ i ] = load( sourceGraphClass, source[ i ], sequential, offline && ! ( i == 1 && transform.equals( "compose" ) ), pl );
			
		final boolean graph0IsLabelled = graph[ 0 ] instanceof ArcLabelledImmutableGraph;
		final ArcLabelledImmutableGraph graph0Labelled = graph0IsLabelled ? (ArcLabelledImmutableGraph)graph[ 0 ] : null;
		final boolean graph1IsLabelled = graph.length > 1 && graph[ 1 ] instanceof ArcLabelledImmutableGraph;
		
		String notForLabelled = "This transformation will just apply to the unlabelled graph; label information will be absent";

		if ( transform.equals( "identity" ) ) result = graph[ 0 ];
		else if ( transform.equals( "map" ) || transform.equals( "mapOffline" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			pl.start( "Reading map..." ); 
			
			final int n = graph[ 0 ].numNodes();
			final int[] f = new int[ n ];
			if ( ascii ) TextIO.loadInts( map, f );
			else BinIO.loadInts( map, f );

			// Delete from the graph all nodes whose index is above the cutoff, if any.
			if ( cutoff != -1 ) for( int i = f.length; i-- != 0; ) if ( f[ i ] >= cutoff ) f[ i ] = -1;

			pl.count = n;
			pl.done();

			result = transform.equals( "map" ) ? map( graph[ 0 ], f, pl ) : mapOffline( graph[ 0 ], f, batchSize, tempDir, pl );
			LOGGER.info( "Transform computation completed." );
		} 
		else if ( transform.equals( "arcfilter" ) ) {
			if ( graph0IsLabelled && ! ( arcFilter instanceof LabelledArcFilter ) ) {
				LOGGER.warn( notForLabelled );
				result = filterArcs( graph[ 0 ], arcFilter, pl );
			}
			else result = filterArcs( graph[ 0 ], arcFilter, pl );
		}
		else if ( transform.equals( "larcfilter" ) ) {
			if ( ! graph0IsLabelled ) throw new IllegalArgumentException( "Filtering on labelled arcs requires a labelled graph" );
			result = filterArcs( graph0Labelled, labelledArcFilter, pl );
		}
		else if ( transform.equals( "symmetrize" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			result = symmetrize( graph[ 0 ], graph[ 1 ], pl );
		}
		else if ( transform.equals( "symmetrizeOffline" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			result = symmetrizeOffline( graph[ 0 ], batchSize, tempDir, pl );
		}
		else if ( transform.equals( "removeDangling" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			
			final int n = graph[ 0 ].numNodes();
			LOGGER.info( "Finding dangling nodes..." ); 

			final int[] f = new int[ n ];
			NodeIterator nodeIterator = graph[ 0 ].nodeIterator();
			int c = 0;
			for( int i = 0; i < n; i++ ) {
				nodeIterator.nextInt();
				f[ i ] = nodeIterator.outdegree() != 0 ? c++ : -1;
			}
			result = map( graph[ 0 ], f, pl );
		}
		else if ( transform.equals( "transpose" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			result = transpose( graph[ 0 ], pl );
		}
		else if ( transform.equals( "transposeOffline" ) ) {
			result = graph0IsLabelled ? transposeOffline( graph0Labelled, batchSize, tempDir, pl ) : transposeOffline( graph[ 0 ], batchSize, tempDir, pl );
		}
		else if ( transform.equals( "union" ) ) {
			if ( graph0IsLabelled && graph1IsLabelled ) {
				if ( labelMergeStrategy == null ) throw new IllegalArgumentException( "Uniting labelled graphs requires a merge strategy" );
				result = union( graph0Labelled,  (ArcLabelledImmutableGraph)graph[ 1 ], labelMergeStrategy );
			}
			else {
				if ( graph0IsLabelled || graph1IsLabelled ) LOGGER.warn( notForLabelled );
				result = union( graph[ 0 ], graph[ 1 ] );
			}
		}
		else if ( transform.equals( "compose" ) ) {
			if ( graph0IsLabelled && graph1IsLabelled ) {
				if ( labelSemiring == null ) throw new IllegalArgumentException( "Composing labelled graphs requires a composition strategy" );
				result = compose( graph0Labelled, (ArcLabelledImmutableGraph)graph[ 1 ], labelSemiring ); 
			}
			else {
				if ( graph0IsLabelled || graph1IsLabelled ) LOGGER.warn( notForLabelled );
				result = compose( graph[ 0 ], graph[ 1 ] );
			}
		}
		else if ( transform.equals( "gray" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			result = map( graph[ 0 ], grayCodePermutation( graph[ 0 ] ) );
		}
		else if ( transform.equals( "grayPerm" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			BinIO.storeInts( grayCodePermutation( graph[ 0 ] ), param[ 1 ] );
			return;
		}
		else if ( transform.equals( "strictHostByHostGray" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			final int[] f = new int[ graph[ 0 ].numNodes() ];
			if ( ascii ) TextIO.loadInts( map, f );
			else BinIO.loadInts( map, f );
			result = map( graph[ 0 ], hostByHostGrayCodePermutation( graph[ 0 ], f, true ) );
		}
		else if ( transform.equals( "strictHostByHostGrayPerm" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			final int[] f = new int[ graph[ 0 ].numNodes() ];
			if ( ascii ) TextIO.loadInts( map, f );
			else BinIO.loadInts( map, f );
			BinIO.storeInts( hostByHostGrayCodePermutation( graph[ 0 ], f, true ), param[ 1 ] );
			return;
		}
		else if ( transform.equals( "looseHostByHostGray" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			final int[] f = new int[ graph[ 0 ].numNodes() ];
			if ( ascii ) TextIO.loadInts( map, f );
			else BinIO.loadInts( map, f );
			result = map( graph[ 0 ], hostByHostGrayCodePermutation( graph[ 0 ], f, false ) );
		}
		else if ( transform.equals( "looseHostByHostGrayPerm" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			final int[] f = new int[ graph[ 0 ].numNodes() ];
			if ( ascii ) TextIO.loadInts( map, f );
			else BinIO.loadInts( map, f );
			BinIO.storeInts( hostByHostGrayCodePermutation( graph[ 0 ], f, false ), param[ 1 ] );
			return;
		}
		else if ( transform.equals( "lex" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			result = map( graph[ 0 ], lexicographicalPermutation( graph[ 0 ] ) );
		}
		else if ( transform.equals( "lexPerm" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			BinIO.storeInts( lexicographicalPermutation( graph[ 0 ] ), param[ 1 ] );
			return;
		}
		else if ( transform.equals( "random" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			result = map( graph[ 0 ], randomPermutation( graph[ 0 ], seed ) );
		}
		else if ( transform.equals( "line" ) ) {
			if ( graph0IsLabelled ) LOGGER.warn( notForLabelled );
			result = line( graph[ 0 ], map, tempDir, batchSize, pl );
		} else result = null;

		if ( result instanceof ArcLabelledImmutableGraph ) {
			// Note that we derelativise non-absolute pathnames to build the underlying graph name.
			LOGGER.info( "The result is a labelled graph (class: " + destLabelledGraphClass.getName() + ")" );
			final File destFile = new File( dest );
			final String underlyingName = ( destFile.isAbsolute() ? dest : destFile.getName() ) + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX;
			destLabelledGraphClass.getMethod( "store", ArcLabelledImmutableGraph.class, CharSequence.class, CharSequence.class, ProgressLogger.class ).invoke( null, result, dest, underlyingName, pl );
			ImmutableGraph.store( destGraphClass, result, dest + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX, pl );
		}
		else ImmutableGraph.store( destGraphClass, result, dest, pl );
	}
}
