package it.unimi.dsi.webgraph;

/*		 
 * Copyright (C) 2003-2011 Paolo Boldi and Sebastiano Vigna 
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

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

/** An induced subgraph of a given immutable graph. 
 * 
 *  <P><strong>Warning</strong>: this class is experimental, and might be subject to changes.
 * 
 *  <P>The nodes of the subgraph are
 *  specified via an {@link it.unimi.dsi.fastutil.ints.IntSet} or an array of integers. Of course, each node in the subgraph will have
 *  a different index than the corresponding node in the supergraph. The two methods {@link #toSupergraphNode(int)} and {@link #fromSupergraphNode(int)}
 *  are used to translate indices back and forth.
 * 
 *  <P>An immutable subgraph is stored as a property file (which follows the convention established
 *  in {@link it.unimi.dsi.webgraph.ImmutableGraph}), and as a node subset file. The latter must contain an 
 *  (increasing) list of integers in {@link java.io.DataOutput} format representing
 *  the set of nodes of the subgraph.
 *
 *  <P>The property file, with named <samp><var>basename</var>.properties</samp>, contains the following entries:
 * 	<UL>
 *     <LI><samp>supergraphbasename</samp>: the basename of the supergraph. Note that this name is system-dependent:
 *		it is suggested that you use a path-free filename.
 *     <LI><samp>subgraphnodes</samp>: the filename of the node subset file. 
 * 	 If this property is not present, it is assumed to be <samp><var>basename</var>.subgraphnodes</samp>.
 * </UL>
 * 
 *   <P>You can create an immutable subgraph using the public constructor, or you can load one using one of the provided
 *   load methods. Note that there is no <code>store</code> method, because it makes no sense to store a generic {@link it.unimi.dsi.webgraph.ImmutableGraph}
 *   as a subgraph. There is, however, a {@linkplain #save(CharSequence, ProgressLogger) save method} that allows one to save
 *   the files related to a subgraph (the property file and the subgraph node file).
 * 
 * <H2>Root graphs</H2>
 * 
 * <P>When creating tree-shaped hierarchies of subgraphs, the methods {@link #rootBasename()}, {@link #fromRootNode(int)}
 * and {@link #toRootNode(int)} may be used to access information about the root (i.e., the unique highest graph
 * in the hierarchy: note that it cannot be an <code>ImmutableSubgraph</code>).
 * 
 * <P>Should you need to treat uniformly a generic immutable graph as an immutable subgraph, the method
 * {@link #asImmutableSubgraph(ImmutableGraph)} will return a subgraph view of the given immutable graph in which
 * all to/from functions are identities.
 */
public class ImmutableSubgraph extends ImmutableGraph {
	private final static boolean ASSERTS = false;
	private final static boolean DEBUG = false;

	/** The standard property key for the name of the file containing the subgraph nodes. */
	public static final String SUBGRAPHNODES_PROPERTY_KEY = "subgraphnodes";
	/** The standard property key for the supergraph basename. */
	public static final String SUPERGRAPHBASENAME_PROPERTY_KEY = "supergraphbasename";

	/** Builds an immutable subgraph from a given supergraph.
	 * 
	 * @param supergraph the supergraph. 
	 */
	private ImmutableSubgraph( final ImmutableGraph supergraph ) {
		this.supergraph = supergraph;
		this.supergraphAsSubgraph = supergraph instanceof ImmutableSubgraph ? (ImmutableSubgraph)supergraph : null;
	}
	
	public ImmutableSubgraph copy() {
		return new ImmutableSubgraph( supergraph.copy(), subgraphNode );
	}
	
	/** The supergraph. */
	final protected ImmutableGraph supergraph;
	
	/** If {@link #supergraph} is an instance of {@link ImmutableSubgraph}, it is cached here. */
	final protected ImmutableSubgraph supergraphAsSubgraph;
	
	/** The nodes of the subgraph, in increasing order. */
	protected int subgraphNode[];

	/** The number of nodes in the subgraph. */
	protected int subgraphSize;

	/** The number of nodes in the supergraph. */
	protected int supergraphNumNodes;

	/** The basename of this immutable subgraph, if it was loaded from disk, or <code>null</code>. */
	protected CharSequence basename;

  	/** Creates a new immutable subgraph using a given subset of nodes.
  	 * 
  	 * @param supergraph the supegraph.
  	 * @param subgraphNodes the set of nodes defining the induced subgraph.
  	 */
	public ImmutableSubgraph( final ImmutableGraph supergraph, final IntSet subgraphNodes ) {
		this( supergraph );
		this.subgraphNode = subgraphNodes.toIntArray();
		this.subgraphSize = subgraphNode.length;
		this.supergraphNumNodes = supergraph.numNodes();
		java.util.Arrays.sort( subgraphNode );
		if ( subgraphNode[ subgraphSize - 1 ] >= supergraphNumNodes ) throw new IllegalArgumentException( "Subnode index out of bounds: " + subgraphNode[ subgraphSize - 1 ] );
	}

	/** Creates a new immutable subgraph using a given backing node array.
	 * 
	 * <P>Note that <code>subgraphNodes</code> is <em>not</em> copied: thus, it must not
	 * be modified until this subgraph is no longer in use.
	 * 
	 * @param supergraph the supegraph.
	 * @param subgraphNode an increasing array containing the nodes defining the induced subgraph. 
	 */
	public ImmutableSubgraph( final ImmutableGraph supergraph, final int subgraphNode[] ) {
		this( supergraph );
		this.subgraphNode = subgraphNode;
		this.subgraphSize = subgraphNode.length;
		this.supergraphNumNodes = supergraph.numNodes();
		for ( int i = 1; i < subgraphSize; i++ )
			if ( subgraphNode[ i - 1 ] >= subgraphNode[ i ] )
				throw new IllegalArgumentException( "The provided integer array is not strictly increasing: " + (i-1) + "-th element is " + subgraphNode[ i - 1 ] + ", " + i + "-th element is " + subgraphNode[ i ] );
		if ( subgraphNode[ subgraphSize - 1 ] >= supergraphNumNodes ) throw new IllegalArgumentException( "Subnode index out of bounds: " + subgraphNode[ subgraphSize - 1 ] );
	}

	public int numNodes() {
		return subgraphSize;
	}
	
	public long numArcs() {
		throw new UnsupportedOperationException( "Cannot determine the number of arcs in a subgraph" );
	}

	@Override
	public boolean randomAccess() {
		return supergraph.randomAccess();
	}

	public CharSequence basename() {
		if ( basename == null ) throw new IllegalStateException( "This immutable subgraph has no basename" );
		return basename;
	}

	/** Returns the basename of the root graph.
	 * 
	 * @return the {@linkplain ImmutableGraph#basename() basename} of the root graph.
	 */
	public CharSequence rootBasename() {
		return supergraphAsSubgraph != null ? supergraphAsSubgraph.rootBasename() : supergraph.basename();
	}
	
	/** Returns the index of a node of this graph in its supergraph. 
	 * 
	 * @param x an index of a node in this graph.
	 * @return the index of node <code>x</code> in the supergraph.
	 */
	public int toSupergraphNode( final int x ) {
		if ( x < 0 || x >= subgraphSize ) throw new IllegalArgumentException();
		return subgraphNode[ x ];
	}

	/** Returns the index of a node of the supergraph in this graph. 
	 * 
	 * @param x an index of a node in the supergraph.
	 * @return the index of node <code>x</code> in this graph, or a negative value if <code>x</code> does not belong to the subgraph.
	 */
	public int fromSupergraphNode( final int x ) {
		if ( x < 0 || x >= supergraphNumNodes ) throw new IllegalArgumentException();
		return java.util.Arrays.binarySearch( subgraphNode, x );
	}
	
	/** Returns the index of a node of this graph in its root graph. 
	 * 
	 * @param x an index of a node in this graph.
	 * @return the index of node <code>x</code> in the root graph.
	 */
	public int toRootNode( final int x ) {
		return supergraphAsSubgraph != null ? supergraphAsSubgraph.toRootNode( toSupergraphNode( x ) ) : toSupergraphNode( x );
	}

	/** Returns the index of a node of the root graph in this graph. 
	 * 
	 * @param x an index of a node in the root graph.
	 * @return the index of node <code>x</code> in this graph, or a negative value if <code>x</code> does not belong to the root graph.
	 */
	public int fromRootNode( final int x ) {
		if ( supergraphAsSubgraph == null ) return fromSupergraphNode( x ); 
		final int y = supergraphAsSubgraph.fromRootNode( x ); 
		if ( y < 0 ) return -1;
		return fromSupergraphNode( y );
	}

	/** If this variable is non-negative, we are caching the successors' array of node <code>cacheNode</code> (in the subgraph). */
	private int cacheNode = -1;
	
	/** If <code>cacheNode</code>&gt; 0, this array contains the successors of node <code>cacheNode</code> (in the subgraph). */
	private int cacheSuccessors[];
	
	public NodeIterator nodeIterator( final int from ) {
		/** The invariant that we are assuming here is the following: at any time, <code>node</code> is the next (subgraph)
		 * node to be returned by {@link #nextInt()}. This variable contain sensible data
		 * only when <code>node</code> &lt; <code>subgraphSize</code>. Moreover, if outdegree >= 0 then it is 
		 * the outdegree of <code>node</code>-1, and <code>successorsCache</code> contains the successors. */

		// TODO: decide for a strategy. Note that super.nodeIterator is very dangerous, as it uses random access.
		return supergraph.randomAccess() && subgraphSize < supergraphNumNodes / 8 ? super.nodeIterator( from ) :

		new NodeIterator() {
				/** The current node (the next to be returned. */
				int node = from;
				/** This array caches the successors of the node that was returned last (<code>from</code>-1). */
				int[] successorsCache = IntArrays.EMPTY_ARRAY;
				/** The outdegree of the node that was returned last (<code>from</code>-1). */
				int outdegree = -1;
				
				final NodeIterator supergraphNodeIterator = supergraph.nodeIterator();

				public int nextInt() {
					if ( ! hasNext() ) throw new java.util.NoSuchElementException();
					if ( node == from ) {
						supergraphNodeIterator.nextInt();
						supergraphNodeIterator.skip( subgraphNode[ from ] );
					}
					else supergraphNodeIterator.skip( subgraphNode[ node ] - subgraphNode[ node - 1 ] );
					outdegree = -1;
					return node++;
				}

				public boolean hasNext() {
					return ( node < subgraphSize );
				}

				private void unwrapSuccessors() {
					int start = 0, done;
					final LazyIntIterator i = ImmutableSubgraph.this.successors( node - 1, supergraphNodeIterator.successors() );
					// ALERT: we removed i.hasNext() at the end of this check, but it is not necessary
					while( ( done = LazyIntIterators.unwrap( i, successorsCache, start, successorsCache.length - start ) ) == successorsCache.length - start ) { 
						start = successorsCache.length;
						successorsCache = IntArrays.grow( successorsCache, successorsCache.length + 1 ); 
					}
					outdegree = start + done; 
				}

				public int[] successorArray() {
					if ( node == from ) throw new IllegalStateException();
					if ( outdegree == -1 ) unwrapSuccessors();
					return successorsCache;
				}

				public LazyIntIterator successors() {
					if ( node == from ) throw new IllegalStateException();
					if ( outdegree == -1 ) unwrapSuccessors();
					return LazyIntIterators.wrap( successorsCache, outdegree );
				}

				public int outdegree() {
					if ( node == from ) throw new IllegalStateException();
					if ( outdegree == -1 ) unwrapSuccessors();
					return outdegree;
				}
				
			};
	}
	
	public LazyIntIterator successors( final int x ) {
		return successors( x, supergraph.successors( toSupergraphNode( x ) ) );
	}
	
	private LazyIntIterator successors( final int x, final LazyIntIterator supergraphSuccessors ) {
		if ( DEBUG ) System.err.println( this.getClass().getName() + ".successors(" + x + ", " + supergraphSuccessors + ")" );

		if ( x < 0 || x >= subgraphSize ) throw new IllegalArgumentException();
		if ( cacheNode == x ) return LazyIntIterators.wrap( cacheSuccessors );
		
		if ( DEBUG ) System.err.println( this.getClass().getName() + ": returning new iterator" );

		return new LazyIntIterator() {

			public int nextInt() {
				int x, result;
				while ( ( x = supergraphSuccessors.nextInt() ) != -1 ) {
					result = java.util.Arrays.binarySearch( subgraphNode, x );
					if ( result >= 0 ) return result;
				}
				
				return -1;
			}
			
			public int skip( final int n ) {
				int i;
				for( i = 0; i < n && nextInt() != -1; i++ );
				return i;
			}
		};
	}

	public int outdegree( final int x ) {
		return outdegree( x, supergraph.successors( toSupergraphNode( x ) ) );
	}

	public int outdegree( final int x, final LazyIntIterator supergraphSuccessors ) {
		if ( x < 0 || x >= subgraphSize ) throw new IllegalArgumentException();
		if ( cacheNode == x ) return cacheSuccessors.length;
		// TODO: this is not really efficient--we should reuse the cache.
		cacheSuccessors = LazyIntIterators.unwrap( successors( x, supergraphSuccessors ) );
		cacheNode = x;
		if ( ASSERTS ) assert cacheSuccessors != null;
		return cacheSuccessors.length;
	}

	/** A wrapper for immutable graphs, which exhibits them as immutable subgraphs.
	 * Essentially, all functions concerning supergraphs are defined as identities.
	 */
	
	private static class ImmutableGraphWrapper extends ImmutableSubgraph {
		
		public ImmutableGraphWrapper( final ImmutableGraph graph ) {
			super( graph );
			try {
				basename = graph.basename();
			}
			catch( UnsupportedOperationException e ) {
				basename = null;
			}
		}
		
		public NodeIterator nodeIterator() { return supergraph.nodeIterator(); }
		public NodeIterator nodeIterator( final int from ) { return supergraph.nodeIterator( from ); }
		public long numArcs() { return supergraph.numArcs(); }
		public int numNodes() { return supergraph.numNodes(); }
		public int outdegree( final int x ) { return supergraph.outdegree( x ); }
		public int[] successorArray( final int x ) { return supergraph.successorArray( x ); }
		public LazyIntIterator successors( final int x ) { return supergraph.successors( x ); }
		public int toSupergraphNode( final int x ) { return x; }
		public int fromSupergraphNode( final int x ) { return x; }
		public int toRootNode( final int x ) { return x; }
		public int fromRootNode( final int x ) { return x; }
	}
	
	
	/** Returns a subgraph view of the given immutable graph.
	 * 
	 * <P>The wrapper returned by this method may be used whenever immutable
	 * graphs and subgraphs must be mixed.
	 * 
	 * @param graph an immutable graph.
	 * @return the given graph, viewed as a trivial subgraph of itself.
	 */
	public static ImmutableSubgraph asImmutableSubgraph( final ImmutableGraph graph ) {
		return new ImmutableGraphWrapper( graph );	
	}
	
	
	public static ImmutableGraph loadSequential( final CharSequence basename ) throws IOException {
		return load( LoadMethod.STANDARD, basename ); // TODO: is this what we really want?
	}

	public static ImmutableGraph loadSequential( final CharSequence basename, final ProgressLogger pl ) throws IOException {
		return load( LoadMethod.STANDARD, basename, pl );
	}

	public static ImmutableGraph loadOffline( final CharSequence basename ) throws IOException {
		return load( LoadMethod.OFFLINE, basename );
	}

	public static ImmutableGraph loadOffline( final CharSequence basename, final ProgressLogger pl ) throws IOException {
		return load( LoadMethod.OFFLINE, basename, pl );
	}

	public static ImmutableGraph load( final CharSequence basename ) throws IOException {
		return load( LoadMethod.STANDARD, basename );
	}

	public static ImmutableGraph load( final CharSequence basename, final ProgressLogger pl) throws IOException {
		return load( LoadMethod.STANDARD, basename, pl );
	}

	private static ImmutableGraph load( final LoadMethod method, final CharSequence basename ) throws IOException {
		return load( method, basename, null );
	}

	/** Creates a new immutable subgraph by loading the supergraph, delegating the 
	 *  actual loading to the class specified in the <samp>supergraphclass</samp> property within the property
	 *  file (named <samp><var>basename</var>.properties</samp>), and loading the subgraph array in memory.
	 *  The exact load method to be used depends on the <code>method</code> argument.
	 * 
	 * @param method the method to be used to load the supergraph.
	 * @param basename the basename of the graph.
	 * @param pl the progress logger; it can be <code>null</code>.
	 * @return an immutable subgraph containing the specified graph.
	 */

	protected static ImmutableGraph load( final LoadMethod method, final CharSequence basename, final ProgressLogger pl ) throws IOException {
		final FileInputStream propertyFile = new FileInputStream( basename + PROPERTIES_EXTENSION );
		final Properties properties = new Properties();
		properties.load( propertyFile );
		propertyFile.close();

		final String graphClassName = properties.getProperty( ImmutableGraph.GRAPHCLASS_PROPERTY_KEY );
		if ( ! graphClassName.equals( ImmutableSubgraph.class.getName() ) ) throw new IOException( "This class (" + ImmutableSubgraph.class.getName() + ") cannot load a graph stored using " + graphClassName );
		
		final String supergraphBasename = properties.getProperty( SUPERGRAPHBASENAME_PROPERTY_KEY );
		if ( supergraphBasename == null ) throw new IOException( "This property file does not specify the required property supergraphbasename" );
		
		final ImmutableGraph supergraph = ImmutableGraph.load( method, supergraphBasename, null, pl );
		
		if ( pl != null ) pl.start( "Reading nodes..." );
		final String nodes = properties.getProperty( SUBGRAPHNODES_PROPERTY_KEY );
		final ImmutableSubgraph isg = new ImmutableSubgraph( supergraph, BinIO.loadInts( nodes != null ? nodes : basename + ".nodes" ) );
		if ( pl != null ) {
			pl.count = isg.numNodes();
			pl.done();
		}
		isg.basename = new MutableString( basename );
		return isg;
	}

	/** Throws an {@link UnsupportedOperationException}. */
	@SuppressWarnings("unused")
	public static void store( final ImmutableGraph graph, final CharSequence basename, final ProgressLogger pm ) {
		throw new UnsupportedOperationException( "You cannot store a generic immutable graph as a subgraph" );
	}

	/** Throws an {@link UnsupportedOperationException}. */
	public static void store( final ImmutableGraph graph, final CharSequence basename ) {
		store( graph, basename, (ProgressLogger)null );
	}
	
	/** Saves this immutable subgraph with a given basename. 
	 * 
	 * <P>Note that this method will <strong>not</strong> save the
	 * supergraph, but only the subgraph files, that is, the subgraph property file
	 * (with extension <samp>.properties</samp>) and the file containing
	 * the subgraph nodes (with extension <samp>.nodes</samp>). A reference
	 * to the supergraph basename will be stored in the property file.
	 * 
	 * @param basename the basename to be used to save the subgraph.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public void save( final CharSequence basename, final ProgressLogger pl ) throws IOException {

		final Properties properties = new Properties();
		properties.setProperty( ImmutableGraph.GRAPHCLASS_PROPERTY_KEY, ImmutableSubgraph.class.getName() );
		properties.setProperty( SUPERGRAPHBASENAME_PROPERTY_KEY, supergraph.basename().toString() );

		final FileOutputStream propertyFile = new FileOutputStream( basename + PROPERTIES_EXTENSION );
		properties.store( propertyFile, null );
		propertyFile.close();
		
		// Save the subgraph nodes
		if ( pl != null ) pl.start( "Saving nodes..." );
		BinIO.storeInts( subgraphNode, 0, subgraphNode.length, basename + ".nodes" );
		
		if ( pl != null ) {
			pl.count = subgraphNode.length;
			pl.done();
		}
	}


	public void save( final CharSequence basename ) throws IOException {
		save( basename, (ProgressLogger)null );
	}
	
	public static void main( String args[] ) throws IllegalArgumentException, SecurityException, JSAPException, UnsupportedEncodingException, FileNotFoundException {

		final SimpleJSAP jsap = new SimpleJSAP( ImmutableSubgraph.class.getName(), "Writes the property file of an immutable subgraph.",
				new Parameter[] {
						new UnflaggedOption( "supergraphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the supergraph." ),
						new FlaggedOption( "subgraphNodes", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 's', "subgraph-nodes", "Sets a subgraph node file. If not specified, the name will be stemmed from the basename." ),
						new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of resulting immutable subgraph." ),
					}		
				);
		
		final JSAPResult jsapResult = jsap.parse( args );
		if ( jsap.messagePrinted() ) System.exit( 1 );

		final PrintWriter pw = new PrintWriter( new OutputStreamWriter( new FileOutputStream( jsapResult.getString( "basename" ) + ImmutableGraph.PROPERTIES_EXTENSION ), "UTF-8" ) );
		pw.println( ImmutableGraph.GRAPHCLASS_PROPERTY_KEY + " = " + ImmutableSubgraph.class.getName() );
		pw.println( "supergraphbasename = " + jsapResult.getString( "supergraphBasename" ) );
		if ( jsapResult.userSpecified( "subgraphNodes" )  ) pw.println( "subgraphnodes = " + jsapResult.getString( "subgraphNodes" ) );	
		pw.close();
	}

	
}
