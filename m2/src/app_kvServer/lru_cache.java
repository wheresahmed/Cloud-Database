package app_kvServer;

import java.util.LinkedHashMap;
import java.util.Map;

public class lru_cache<K, V> extends LinkedHashMap <K, V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int cache_size;

	public lru_cache(int cache_size){
		//true allows for eviction by access order (LRU)
		super (cache_size, 0.75f, true);
		this.cache_size = cache_size;
	}


	@Override 
	protected boolean removeEldestEntry (Map.Entry<K,V> eldest){
		return size() > cache_size;
	}
}
