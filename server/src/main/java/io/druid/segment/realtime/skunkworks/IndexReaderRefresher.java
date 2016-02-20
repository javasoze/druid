package io.druid.segment.realtime.skunkworks;

import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

import com.metamx.emitter.EmittingLogger;

public class IndexReaderRefresher extends Thread {

	private static final EmittingLogger log = new EmittingLogger(IndexReaderRefresher.class);
	
	private DirectoryReader currentReader;
	private final long refreshRateInSec;
	private volatile boolean stop = false;
	private Directory dir;

	public IndexReaderRefresher(long refreshRateInSec) {
		this.refreshRateInSec = refreshRateInSec;
		this.dir = null;
	}
	
	public void updateDirectory(Directory dir) {
		this.dir = dir;
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
				if (dir != null) {
					if (currentReader == null) {
						currentReader = DirectoryReader.open(dir);
					} else {
						IndexReader tmpReader = currentReader;
						if (currentReader.directory() != dir) {  // directory updated
							currentReader = DirectoryReader.open(dir);
						} else {
							currentReader = DirectoryReader.openIfChanged(currentReader);
						}
						if (tmpReader != currentReader) {
							tmpReader.close();
						}
					}
				}
				Thread.sleep(refreshRateInSec * 1000);
			} catch (InterruptedException e) {
				continue;
			} catch (IOException ioe) {
				log.error(ioe.getMessage(), ioe);
			}
		}
	}
	
	public DirectoryReader getReader() {
		return currentReader;
	}
}
