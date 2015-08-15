package com.datatorrent.alerts.Store;

import com.datatorrent.alerts.Config;
import com.datatorrent.alerts.LevelChangeNotifier;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MessageTracker <K,V> {

    public static class HeadTail<V> {
            public Node<V> head = null ;
            public Node<V> tail = null ;
    }

    public static class Node<T> {
        public Node<T> next = null;
        public Node<T> prev = null;
        public T val ;
        public Integer level = 0 ;
        public Date lastNotified ;

        public Node( T val, Integer level ) {
            this.val = val ;
            lastNotified = new Date() ;
            this.level = level ;
        }

        public Node() {} ;
    }

    private HashMap<K, HeadTail<V>> table ;

    /*
    * Message to Node in the table mapping.
    * TODO: Is there a need to store, information like level & Wait time here ?
    * */
    private HashMap<V, Node<V>> index ;
    private volatile Integer timeToSleep = 10000 ;

    private LevelChangeNotifier levelChangeNotifier ;
    private Config config ;

    public MessageTracker(LevelChangeNotifier levelChangeNotifier, Config config) {

        table = new HashMap<>() ;
        index = new HashMap<>() ;

        this.levelChangeNotifier = levelChangeNotifier ;
        this.config = config ;
        timeToSleep = config.WaitTimeForEscalation(0) ;

        Thread timer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(timeToSleep);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                isItTimeToEscalate() ;
            }
        }) ;

       timer.start();
    }

    public synchronized void put(K key, Integer level, V value) {

        //TODO: If already exists what to do ?
        Node<V> node = new Node<>(value, level) ;
        index.put(value, node) ;

        if ( table.containsKey(key) ) {

            HeadTail headTail = table.get(key) ;
            node.prev = headTail.tail ;
            headTail.tail.next = node ;
            headTail.tail = node ;
        }
        else {
            HeadTail<V> headTail = new HeadTail<>() ;
            headTail.tail = node ;
            headTail.head = new Node<>() ;
            headTail.head.next = node ;

            node.prev = headTail.head ;

            table.put(key, headTail) ;
        }
    }

    public synchronized void remove( V value ) {

        if ( index.containsKey(value) ) {

            Node<V> node = index.get(value) ;

            Node<V> prev = node.prev;
            Node<V> next = node.next ;

            prev.next = next ;

            if ( next != null ) {
                next.prev = prev;
            }

            index.remove(value) ;
        }
    }

     private synchronized void isItTimeToEscalate() {

         Date now = new Date() ;

         for (Map.Entry<K,HeadTail<V>> entry : table.entrySet() ) {

                HeadTail<V> headTail = entry.getValue() ;

                Node<V> curr = headTail.head.next ;

                while ( curr != null ) {

                  Long timeDiff = now.getTime() - curr.lastNotified.getTime() ;

                    if ( timeDiff >= config.WaitTimeForEscalation(curr.level) ) {

                        // Keep the temporary list of update nodes ;
                    }
                    else {
                          if ( timeDiff < timeToSleep ) {

                              timeToSleep = timeDiff.intValue() ;
                          }
                        }
                        break ;
                    }
                }

                // TODO: fill in the the list
         }

    }
