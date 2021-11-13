package proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import http.HttpClient;
import http.HttpClient10;
import media.MovieManifest;
import media.MovieManifest.Manifest;
import media.MovieManifest.SegmentContent;
import proxy.server.ProxyServer;

public class Main {
	static final String MEDIA_SERVER_BASE_URL = "http://localhost:9999";

	public static void main(String[] args) {
		ProxyServer.start((movie, queue) -> new DashPlaybackHandler(movie, queue));
	}
	
	/**
	 * Class that implements the client-side logic.
	 * 
	 * Feeds the player queue with movie segment data fetched
	 * from the HTTP server.
	 * 
	 * The fetch algorithm should prioritize:
	 * 1) avoid stalling the browser player by allowing the queue to go empty
	 * 2) if network conditions allow, retrieve segments from higher quality tracks
	 */
	private static class DashPlaybackHandler implements Runnable  {
		
		private final String movie;
		private final Manifest manifest;
		private final BlockingQueue<SegmentContent> queue;
		private final HttpClient http;
		
		public DashPlaybackHandler(String movie, BlockingQueue<SegmentContent> queue) {
			this.movie = movie;
			this.queue = queue;
			
			http = new HttpClient10();
			
			String manifestURL = MEDIA_SERVER_BASE_URL + "/" + movie + "/manifest.txt";
			byte[] manifestBytes = http.doGet(manifestURL);
			manifest = MovieManifest.parse(new String(manifestBytes));
		}
		
		/**
		 * Runs automatically in a dedicated thread...
		 * 
		 * Needs to feed the queue with segment data fast enough to
		 * avoid stalling the browser player
		 * 
		 * Upon reaching the end of stream, the queue should
		 * be fed with a zero-length data segment
		 */
		public void run() {
			List<MovieManifest.Track> tracks = manifest.tracks();
			AverageKbps averageKbps = new AverageKbps(3);
			int currentSegment = 0, numSegments = tracks.get(0).segments().size();
			
			MovieManifest.Track bestTrack = null;
			
			while (currentSegment < numSegments) {
				MovieManifest.Track previousTrack = bestTrack;
				bestTrack = bestTrack(averageKbps.calculate(), tracks);
				
				String bestTrackURL = MEDIA_SERVER_BASE_URL + "/" + movie + "/" + bestTrack.filename();
				
				if (previousTrack != null && !previousTrack.filename().equals(bestTrack.filename())) {
					MovieManifest.Segment firstSegment = bestTrack.segments().get(0);
					byte[] segmentData = http.doGetRange(bestTrackURL, firstSegment.offset(), firstSegment.offset() + firstSegment.length() - 1);
					queueSegment(queue, bestTrack.contentType(), segmentData);
				}
				
				MovieManifest.Segment segment = bestTrack.segments().get(currentSegment);
				
				long transferStart = System.nanoTime();
				byte[] segmentData = http.doGetRange(bestTrackURL, segment.offset(), segment.offset() + segment.length() - 1);
				double transferTimeSeconds = (System.nanoTime() - transferStart) / Math.pow(10.0, 9.0);
				double transferKbps = (segmentData.length * 8) / (1000 * transferTimeSeconds);
				averageKbps.add(transferKbps, currentSegment);
				currentSegment++;
				
				queueSegment(queue, bestTrack.contentType(), segmentData);
			}
			
			queue.add(new SegmentContent("", new byte[0]));
		}
		
		private static void queueSegment(BlockingQueue<MovieManifest.SegmentContent> queue, String contentType, byte[] segmentData) {
			try {
				queue.put(new SegmentContent(contentType, segmentData));
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		private static MovieManifest.Track bestTrack(double averageKbps, List<MovieManifest.Track> tracks) {
			MovieManifest.Track bestTrack = tracks.get(0);
			
			for (MovieManifest.Track track : tracks) {
				if (Math.abs((track.avgBandwidth() / (double) 1000) - averageKbps) <
						Math.abs((bestTrack.avgBandwidth() / (double) 1000) - averageKbps)) {
					bestTrack = track;
				}
			}
			
			return bestTrack;
		}
		
		private static class AverageKbps {
			
			private int maxSaved, numSaved;
			private double[] saved;
			
			private AverageKbps(int maxSaved) {
				this.maxSaved = maxSaved;
				numSaved = 0;
				saved = new double[maxSaved];
			}
			
			private void add(double kbps, int segmentNumber) {
				saved[segmentNumber % maxSaved] = kbps;
				
				if (numSaved < maxSaved) {
					numSaved++;
				}
			}
			
			private double calculate() {
				if (numSaved == 0) {
					return 0;
				}
				
				double total = 0;
				
				for (int i = 0; i < numSaved; i++) {
					total += saved[i];
				}
				
				return total / numSaved;
			}
		}
	}
}
