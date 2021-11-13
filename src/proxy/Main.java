package proxy;

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
			MovieManifest.Track bestTrack = tracks.get(0);
			
			for (MovieManifest.Track track : tracks) {
				if (track.avgBandwidth() > bestTrack.avgBandwidth()) {
					bestTrack = track;
				}
			}
			
			System.out.println(bestTrack.filename());
			
			String bestTrackURL = MEDIA_SERVER_BASE_URL + "/" + manifest.name() + "/" + bestTrack.filename();
			List<MovieManifest.Segment> segments = bestTrack.segments();
			
			for (MovieManifest.Segment segment : segments) {
				byte[] segmentData = http.doGetRange(bestTrackURL, segment.offset(), segment.offset() + segment.length() - 1);
				try {
					queue.put(new SegmentContent(bestTrack.contentType(), segmentData));
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			queue.add(new SegmentContent(bestTrack.contentType(), new byte[0]));
		}
	}
}
