package it.unimi.dsi.webgraph.algo;

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
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** Computes the neighbourhood function of a graph by multiple breadth-first visits.
 * 
 * <p>Note that performing all breadth-first visits requires time <i>O</i>(<var>n</var><var>m</var>), so this class
 * is usable only on very small graphs.
 * 
 * <p>Additionally, this class provides several useful static methods such as {@link #shortestPathsCumulativeDistributionFunction(double[])},
 * {@link #effectiveDiameter(double, double[])} and {@link #spid(double[])} that act on neighbourhood functions.
 * 
 * @author Paolo Boldi
 * @author Sebastiano Vigna
 */
public class NeighbourhoodFunction {
	private static final Logger LOGGER = Util.getLogger( NeighbourhoodFunction.class );
	
	/** Computes and returns the neighbourhood function of the specified graph by multiple breadth-first visits.
	 * 
	 * @param g a graph.
	 * @return the neighbourhood function of the specified graph.
	 */
	public static double[] computeNeighbourhoodFunction( final ImmutableGraph g ) {
		return computeNeighbourhoodFunction( g, null );
	}

	/** Computes and returns the neighbourhood function of the specified graph by multiple breadth-first visits.
	 * 
	 * @param g a graph.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the neighbourhood function of the specified graph.
	 */
	public static double[] computeNeighbourhoodFunction( final ImmutableGraph g, final ProgressLogger pl ) {
		final int n = g.numNodes();
		final IntArrayFIFOQueue queue = new IntArrayFIFOQueue(); 
		final int[] dist = new int[ n ];
		final long count[] = new long[ n ];
		
		if ( pl != null ) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start();
		}

		for( int i = 0; i < n; i++ ) {
			queue.clear();
			queue.enqueue( i );
			IntArrays.fill( dist, -1 );
			dist[ i ] = 0;
			count[ 0 ]++;
			
			while( ! queue.isEmpty() ) {
				final int x = queue.dequeueInt();
				final LazyIntIterator successors = g.successors( x );
				int s;
				while( ( s = successors.nextInt() ) != -1 ) {
					if ( dist[ s ] < 0 ) {
						queue.enqueue( s );
						dist[ s ] = dist[ x ] + 1;
						count[ dist[ x ] + 1 ]++;
					}
				}
			}

			if ( pl != null ) pl.update();
		}
	
		if ( pl != null ) pl.done();
		
		int last;
		for( last = 0; last < n && count[ last ] != 0; last++ );
		final double[] result = new double[ last ];
		result[ 0 ] = count[ 0 ];
		for( int i = 1; i < last; i++ ) result[ i ] = result[ i - 1 ] + count[ i ];
		return result;
	}
	
	/** Computes the shortest-paths cumulative distribution function.
	 * 
	 * @param neighbourhoodFunction a neighbourhood function (or shortest-paths cumulative distribution function, which will be left untouched).
	 * @return the shortest-paths cumulative distribution function.
	 * @deprecated Use {@link #distanceCumulativeDistributionFunction(double[])}.
	 */
	@Deprecated
	public static double[] shortestPathsCumulativeDistributionFunction( final double[] neighbourhoodFunction ) {
		return distanceCumulativeDistributionFunction( neighbourhoodFunction );
	}
	
	/** Computes the distance cumulative distribution function.
	 * 
	 * @param neighbourhoodFunction a neighbourhood function (or distance cumulative distribution, which will be left untouched).
	 * @return the distance cumulative distribution function.
	 */
	public static double[] distanceCumulativeDistributionFunction( final double[] neighbourhoodFunction ) {
		final double[] result = neighbourhoodFunction.clone();
		double lastValue = result[ result.length - 1 ];
		for( int i = result.length; i-- != 0; ) result[ i ] /= lastValue;
		return result;
	}

	/** Computes the shortest-paths density function.
	 * 
	 * @param neighbourhoodFunction a neighbourhood function (or shortest-paths cumulative distribution function).
	 * @return the shortest-paths density function.
	 * @deprecated Use {@link #distanceDensityFunction(double[])}.
	 */
	@Deprecated
	public static double[] shortestPathsDensityFunction( final double[] neighbourhoodFunction ) {
		return distanceDensityFunction( neighbourhoodFunction );
	}

	/** Computes the distance density function.
	 * 
	 * @param neighbourhoodFunction a neighbourhood function (or distance cumulative distribution function).
	 * @return the distance density function.
	 */
	public static double[] distanceDensityFunction( final double[] neighbourhoodFunction ) {
		final double[] result = neighbourhoodFunction.clone();
		double lastValue = result[ result.length - 1 ];
		// Not necessary, but not harmful.
		for( int i = result.length; i-- != 0; ) result[ i ] /= lastValue;
		for( int i = result.length; i-- != 1; ) result[ i ] -= result[ i - 1 ];
		return result;
	}
	
	/** Returns an estimate to the effective diameter at a specified fraction.
	 * 
	 * @param fraction the desired fraction of pairs of nodes (usually, 0.9).
	 * @param neighbourhoodFunction a neighbourhood function (or distance cumulative distribution function).
	 * @return the estimate of the diameter.
	 */
	public static double effectiveDiameter( double fraction, double[] neighbourhoodFunction ) {
		double finalFraction = neighbourhoodFunction[ neighbourhoodFunction.length - 1 ];
		int i;
		for ( i = 0; neighbourhoodFunction[ i ] / finalFraction < fraction; i++ );
		
		if ( i == 0 ) // In this case we assume the previous ordinate to be zero
			return i + ( fraction * finalFraction - neighbourhoodFunction[ i ] ) / ( neighbourhoodFunction[ i ] );
		else			
			return i + ( fraction * finalFraction - neighbourhoodFunction[ i ] ) / ( neighbourhoodFunction[ i ] - neighbourhoodFunction[ i - 1 ] );
	}

	/** Returns an estimate to the effective diameter at 0.9.
	 * 
	 * @param neighbourhoodFunction a neighbourhood function (or distance cumulative distribution function).
	 * @return the estimate of the diameter.
	 */
	public static double effectiveDiameter( double[] neighbourhoodFunction ) {
		return effectiveDiameter( .9, neighbourhoodFunction );
	}

	/** Returns an estimate to the spid (shortest-paths index of dispersion).
	 * 
	 * @param neighbourhoodFunction a neighbourhood function (or distance cumulative distribution function).
	 * @return the estimate of the spid.
	 */
	public static double spid( double[] neighbourhoodFunction ) {
		final double[] shortestPathDistribution = NeighbourhoodFunction.distanceDensityFunction( neighbourhoodFunction );
		double mean = 0,  meanOfSquares = 0;
		for( int i = 0; i < shortestPathDistribution.length; i++ ) {
			mean += shortestPathDistribution[ i ] * i;
			meanOfSquares += shortestPathDistribution[ i ] * i * i;
		}
		
		return ( meanOfSquares - mean * mean ) / mean;
	}

	/** Returns an estimate to the average shortest-path length.
	 * 
	 * @param neighbourhoodFunction a neighbourhood function (or shortest-paths cumulative distribution function).
	 * @return the estimate of the average shortest-path length.
	 * @deprecated Use {@link #averageDistance(double[])}.
	 */
	@Deprecated
	public static double averageShortestPathLength( double[] neighbourhoodFunction ) {
		return averageDistance( neighbourhoodFunction );
	}

	
	/** Returns an estimate to the average distance.
	 * 
	 * @param neighbourhoodFunction a neighbourhood function (or distance cumulative distribution function).
	 * @return the estimate of the average distance.
	 */
	public static double averageDistance( double[] neighbourhoodFunction ) {
		final double[] shortestPathDistribution = NeighbourhoodFunction.distanceDensityFunction(  neighbourhoodFunction );
		double mean = 0;
		for( int i = 0; i < shortestPathDistribution.length; i++ ) mean += shortestPathDistribution[ i ] * i;
		return mean;
	}

	/** Returns an estimate to the average distance.
	 * 
	 * @param n the number of nodes in the graph.
	 * @param neighbourhoodFunction a neighbourhood function (or distance cumulative distribution function).
	 * @return the estimate of the average distance.
	 */
	public static double harmonicDiameter( final int n, double[] neighbourhoodFunction ) {
		double t = 0;
		for( int i = 1; i < neighbourhoodFunction.length; i++ ) t += ( neighbourhoodFunction[ i ] - neighbourhoodFunction[ i - 1 ] ) / i;
		return (double)n * ( n - 1) / t;
	}

	
	
	public static void main( String arg[] ) throws IOException, JSAPException {
		SimpleJSAP jsap = new SimpleJSAP( NeighbourhoodFunction.class.getName(), 
				"Prints the neighbourhood function of a graph, computing it via breadth-first visits.",
				new Parameter[] {
			new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
			new Switch( "expand", 'e', "expand", "Expand the graph to increase speed (no compression)." ),
			new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph." ),
		}		
		);

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) System.exit( 1 );

		final String basename = jsapResult.getString( "basename" );
		ProgressLogger pl = new ProgressLogger( LOGGER, jsapResult.getLong( "logInterval" ) );
		ImmutableGraph g =ImmutableGraph.load( basename );
		if ( jsapResult.userSpecified( "expand" ) ) g = new ArrayListMutableGraph( g ).immutableView();
		TextIO.storeDoubles( computeNeighbourhoodFunction( g, pl ), System.out );
	}
}

