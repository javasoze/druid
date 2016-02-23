package io.druid.segment.realtime.skunkworks;

import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;

import com.metamx.emitter.EmittingLogger;

public class IndexReaderRefresher extends Thread {

	private static final EmittingLogger log = new EmittingLogger(IndexReaderRefresher.class);
	
	private DirectoryReader currentReader;
	private final long refreshRateInSec;
	private volatile boolean stop = false;
	private IndexWriter currentWriter;

	public IndexReaderRefresher(long refreshRateInSec) {
		this.refreshRateInSec = refreshRateInSec;
		this.currentWriter = null;
	}
	
	public void setCurrentWriter(IndexWriter currentWriter) {
		this.currentWriter = currentWriter;
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
				if (currentWriter != null) {
					if (currentReader == null) {
						currentReader = DirectoryReader.open(currentWriter, false);
					} else {
						IndexReader tmpReader = currentReader;
						currentReader = DirectoryReader.openIfChanged(currentReader);
						if (tmpReader != currentReader) {
						  tmpReader.close();
						}						
					}
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
	
	public DirectoryReader getReader() {
		return currentReader;
	}
}
