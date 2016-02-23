package io.druid.segment.realtime.skunkworks;

import java.io.IOException;

import com.metamx.emitter.EmittingLogger;

public class IndexReaderRefresher extends Thread {

	private static final EmittingLogger log = new EmittingLogger(IndexReaderRefresher.class);
	
	private final long refreshRateInSec;
	private volatile boolean stop = false;
	private SkunkworksSegment segment;

	public IndexReaderRefresher(long refreshRateInSec) {
		this.refreshRateInSec = refreshRateInSec;
		this.segment = null;
	}
	
	public void setCurrentSegment(SkunkworksSegment segment) {
		this.segment = segment;
	}
	
	public void terminate() {
		stop = true;
		this.interrupt();
		try {
			this.join();
		} catch (InterruptedException e) {
			log.error(e.getMessage(), e);
		}
	}

	@Override
	public void run() {
		while (!stop) {
			try {
				if (segment != null) {
					segment.refreshReader();
					log.info("segment reader refreshed");
				}
			} catch (IOException ioe) {
				log.error(ioe.getMessage(), ioe);
			} finally {
			  try
        {
          Thread.sleep(refreshRateInSec * 1000);
        } catch (InterruptedException e)
        {
          continue;
        }
			}
		}
	}
}
