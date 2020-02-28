/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package app_kvServer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * LFU cache implementation based on http://dhruvbird.com/lfu.pdf, with some notable differences:
 * <ul>
 * <li>
 * Frequency list is stored as an array with no next/prev pointers between nodes: looping over the array should be faster and more CPU-cache friendly than
 * using an ad-hoc linked-pointers structure.
 * </li>
 * <li>
 * The max frequency is capped at the cache size to avoid creating more and more frequency list entries, and all elements residing in the max frequency entry
 * are re-positioned in the frequency entry linked set in order to put most recently accessed elements ahead of less recently ones,
 * which will be collected sooner.
 * </li>
 * <li>
 * The eviction factor determines how many elements (more specifically, the percentage of) will be evicted.
 * </li>
 * </ul>
 * As a consequence, this cache runs in *amortized* O(1) time (considering the worst case of having the lowest frequency at 0 and having to evict all
 * elements).
 *
 * @author Sergio Bossa
 */
public class lfu_cache<Key, Value> implements Map<Key, Value> {

    private final Map<Key, CacheNode<Key, Value>> cache;
    private final LinkedHashSet[] frequencyList;
    private int lowestFrequency;
    private int maxFrequency;
    //
    private final int maxCacheSize;
    private final float evictionFactor;

    public lfu_cache(int maxCacheSize, float evictionFactor) {
        if (evictionFactor <= 0 || evictionFactor >= 1) {
            throw new IllegalArgumentException("Eviction factor must be greater than 0 and lesser than or equal to 1");
        }
        this.cache = new HashMap<Key, CacheNode<Key, Value>>(maxCacheSize);
        this.frequencyList = new LinkedHashSet[maxCacheSize];
        this.lowestFrequency = 0;
        this.maxFrequency = maxCacheSize - 1;
        this.maxCacheSize = maxCacheSize;
        this.evictionFactor = evictionFactor;
        initFrequencyList();
    }

    public Value put(Key k, Value v) {
        Value oldValue = null;
        CacheNode<Key, Value> currentNode = cache.get(k);
        if (currentNode == null) {
            if (cache.size() == maxCacheSize) {
                doEviction();
            }
            LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[0];
            currentNode = new CacheNode(k, v, 0);
            nodes.add(currentNode);
            cache.put(k, currentNode);
            lowestFrequency = 0;
        } else {
            oldValue = currentNode.v;
            currentNode.v = v;
        }
        return oldValue;
    }


    public void putAll(Map<? extends Key, ? extends Value> map) {
        for (Map.Entry<? extends Key, ? extends Value> me : map.entrySet()) {
            put(me.getKey(), me.getValue());
        }
    }

    public Value get(Object k) {
        CacheNode<Key, Value> currentNode = cache.get(k);
        if (currentNode != null) {
            int currentFrequency = currentNode.frequency;
            if (currentFrequency < maxFrequency) {
                int nextFrequency = currentFrequency + 1;
                LinkedHashSet<CacheNode<Key, Value>> currentNodes = frequencyList[currentFrequency];
                LinkedHashSet<CacheNode<Key, Value>> newNodes = frequencyList[nextFrequency];
                moveToNextFrequency(currentNode, nextFrequency, currentNodes, newNodes);
                cache.put((Key) k, currentNode);
                if (lowestFrequency == currentFrequency && currentNodes.isEmpty()) {
                    lowestFrequency = nextFrequency;
                }
            } else {
                // Hybrid with LRU: put most recently accessed ahead of others:
                LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[currentFrequency];
                nodes.remove(currentNode);
                nodes.add(currentNode);
            }
            return currentNode.v;
        } else {
            return null;
        }
    }

    public Value remove(Object k) {
        CacheNode<Key, Value> currentNode = cache.remove(k);
        if (currentNode != null) {
            LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[currentNode.frequency];
            nodes.remove(currentNode);
            if (lowestFrequency == currentNode.frequency) {
                findNextLowestFrequency();
            }
            return currentNode.v;
        } else {
            return null;
        }
    }

    public int frequencyOf(Key k) {
        CacheNode<Key, Value> node = cache.get(k);
        if (node != null) {
            return node.frequency + 1;
        } else {
            return 0;
        }
    }

    public void clear() {
        for (int i = 0; i <= maxFrequency; i++) {
            frequencyList[i].clear();
        }
        cache.clear();
        lowestFrequency = 0;
    }

    public Set<Key> keySet() {
        return this.cache.keySet();
    }

    public Collection<Value> values() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Set<Entry<Key, Value>> entrySet() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int size() {
        return cache.size();
    }

    public boolean isEmpty() {
        return this.cache.isEmpty();
    }

    public boolean containsKey(Object o) {
        return this.cache.containsKey(o);
    }

    public boolean containsValue(Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }


    private void initFrequencyList() {
        for (int i = 0; i <= maxFrequency; i++) {
            frequencyList[i] = new LinkedHashSet<CacheNode<Key, Value>>();
        }
    }

    private void doEviction() {
        int currentlyDeleted = 0;
        float target = maxCacheSize * evictionFactor;
        while (currentlyDeleted < target) {
            LinkedHashSet<CacheNode<Key, Value>> nodes = frequencyList[lowestFrequency];
            if (nodes.isEmpty()) {
                throw new IllegalStateException("Lowest frequency constraint violated!");
            } else {
                Iterator<CacheNode<Key, Value>> it = nodes.iterator();
                while (it.hasNext() && currentlyDeleted++ < target) {
                    CacheNode<Key, Value> node = it.next();
		    it.remove();
                    cache.remove(node.k);
                }
                if (!it.hasNext()) {
                    findNextLowestFrequency();
                }
            }
        }
    }

    private void moveToNextFrequency(CacheNode<Key, Value> currentNode, int nextFrequency, LinkedHashSet<CacheNode<Key, Value>> currentNodes, LinkedHashSet<CacheNode<Key, Value>> newNodes) {
        currentNodes.remove(currentNode);
        newNodes.add(currentNode);
        currentNode.frequency = nextFrequency;
    }

    private void findNextLowestFrequency() {
        while (lowestFrequency <= maxFrequency && frequencyList[lowestFrequency].isEmpty()) {
            lowestFrequency++;
        }
        if (lowestFrequency > maxFrequency) {
            lowestFrequency = 0;
        }
    }

    private static class CacheNode<Key, Value> {

        public final Key k;
        public Value v;
        public int frequency;

        public CacheNode(Key k, Value v, int frequency) {
            this.k = k;
            this.v = v;
            this.frequency = frequency;
        }

    }
}

/*
package app_kvServer;
import java.nio.file.attribute.DosFileAttributeView;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class lfu_cache <Key,Value> implements Map<Key,Value>{
    private final Map<Key,CachNode<Key,Value>>cache;
    private final LinkedHashSet[] freq_list;
    private int lowest_freq;
    private int max_freq;
    private final int max_size;
    private final float eviction_factor;
    
    public lfu_cache(int max_size, float eviction_factor){
        this.cache=new HashMap<Key, CacheNode<Key,Value>>(max_size);
        this.freq_list=new LinkedHashSet[max_size];
        this.lowest_freq=0;
        this.max_freq=max_size-1;
        this.max_size=max_size;
        this.eviction_factor=eviction_factor;
        initialize_freq_list();
    }
    private void initialize_freq_list(){
        for(int i=0;i<=max_freq;i++){
            freq_list[i]=new LinkedHashSet<CacheNode<Key,Value>>();
        }
    }
    public Value put(Key k, Value v){
        Value old_val=null;
        CacheNode<Key,Value> cur_node=cache.get(k);
        if(cur_node!=null){
            old_val=cur_node.v;
            cur_node.v=v;
        }
        else{
            if(cache.size()==max_size){
                evict_cache();
            }
            LinkedHashSet<CacheNode<Key,Value>>nodes=freq_list[0];
            cur_node=new CacheNode(k,v,0);
            nodes.add(cur_node);
            cache.put(k,cur_node);
            lowest_freq=0;
        }
        return old_val;

    }
    public Value get(Object k){
        CacheNode<Key, Value> cur_node=cache.get(k);
        if (cur_node==null){
            return null;
        }
        int cur_freq=cur_node.frequency;
        if(cur_freq<max_freq){
            int next_freq=cur_freq+1;
            LinkedHashSet<CacheNode<Key, Value>> cur_nodes = freq_list[cur_freq];
            LinkedHashSet<CacheNode<Key, Value>> next_nodes = freq_list[next_freq];
            move_to_next_freq(cur_node,next_freq,cur_nodes, next_nodes);
            cache.put((Key),k,cur_node);
            if (lowest_freq==cur_freq && cur_nodes.isEmpty()){
                lowest_freq=next_freq;
            }
        } else {
            LinkedHashSet<CacheNode<Key, Value>> nodes=freq_list[cur_freq];
            nodes.remove(cur_node);
            nodes.add(cur_node);
        }
        return cur_node.v;
        } 

    }
    private void evict_cache(){
        int cur_deleted=0;
        float target=max_size* eviction_factor;
        while (cur_deleted<target){
            LinkedHashSet<CacheNode<Key,Value>>nodes=freq_list[lowest_freq];
            Iterator<CacheNode<Key,Value>>iterator=nodes.iterator();
            while(iterator.hasNext()&&cur_deleted++<target){
                CacheNode<Key,Value> node=iterator.next;
                it.remove();
                cache.remove(node.k);
            }
            if(!iterator.hasNext()){
                find_next_lowest_freq();
            }

        }
    }
    private void find_next_lowest_freq(){
        while(lowest_freq<=max_freq && freq_list[lowest_freq].isEmpty()){
            lowest_freq++;
        }
        if (lowest_freq>max_freq){
            lowest_freq=0;
        }
        
    }
    private void move_to_next_freq(CacheNode<Key, Value> cur_node, int next_freq, LinkedHashSet<CacheNode<Key, Value>> cur_nodes, LinkedHashSet<CacheNode<Key, Value>> new_nodes) {
        cur_nodes.remove(cur_node);
        next_nodes.add(cur_node);
        cur_node.frequency = next_freq;
    }
    public Value remove(Object k) {
        CacheNode<Key, Value> currentNode = cache.remove(k);
        if (currentNode != null) {
            LinkedHashSet<CacheNode<Key, Value>> nodes = freq_list[currentNode.frequency];
            nodes.remove(currentNode);
            if (lowestFrequency == currentNode.frequency) {
                find_next_lowest_frequency();
            }
            return currentNode.v;
        } else {
            return null;
        }
    }
    public void clear() {
        for (int i = 0; i <= max_freq; i++) {
            freq_list[i].clear();
        }
        cache.clear();
        lowest_freq = 0;
    }

    public Set<Key> keySet() {
        return this.cache.keySet();
    }

    public Collection<Value> values() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Set<Entry<Key, Value>> entrySet() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int size() {
        return cache.size();
    }

    public boolean isEmpty() {
        return this.cache.isEmpty();
    }

    public boolean containsKey(Object o) {
        return this.cache.containsKey(o);
    }

    public boolean containsValue(Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private static class CacheNode<Key, Value> {

        public final Key k;
        public Value v;
        public int frequency;

        public CacheNode(Key k, Value v, int frequency) {
            this.k = k;
            this.v = v;
            this.frequency = frequency;
        }

    }
}
*/
