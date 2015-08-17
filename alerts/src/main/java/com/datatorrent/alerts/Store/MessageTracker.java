package com.datatorrent.alerts.Store;

import com.datatorrent.alerts.Config;
import com.datatorrent.alerts.LevelChangeNotifier;
import com.datatorrent.alerts.Message;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MessageTracker {

    public static class HeadTail {
            public Node head = null ;
            public Node tail = null ;
    }

    public static class Node {
        public Node next = null;
        public Node prev = null;
        public Message val ;
        public Integer level = 0 ;
        public Date lastNotified ;

        public Node( Message val, Integer level ) {
            this.val = val ;
            lastNotified = new Date() ;
            this.level = level ;
        }

        public Node() {} ;
    }

    private HashMap<Long, HeadTail> table ;

    /*
    * Message to Node in the table mapping.
    * TODO: Is there a need to store, information like level & Wait time here ?
    * */
    private HashMap<Message, Node> index ;
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

    public synchronized void put(Long key, Integer level, Message value) {

        //TODO: If already exists what to do ?
        Node node = new Node(value, level) ;
        index.put(value, node) ;

        if ( table.containsKey(key) ) {

            HeadTail headTail = table.get(key) ;
            node.prev = headTail.tail ;
            headTail.tail.next = node ;
            headTail.tail = node ;
        }
        else {
            HeadTail headTail = new HeadTail() ;
            headTail.tail = node ;
            headTail.head = new Node() ;
            headTail.head.next = node ;

            node.prev = headTail.head ;

            table.put(key, headTail) ;
        }
    }

    public synchronized void remove( Message value ) {

        if ( index.containsKey(value) ) {

            Node node = index.get(value) ;

            Node prev = node.prev;
            Node next = node.next ;

            prev.next = next ;

            if ( next != null ) {
                next.prev = prev;
            }

            index.remove(value) ;
        }
    }

     private synchronized void isItTimeToEscalate() {

         Date now = new Date() ;

         for (Map.Entry<Long,HeadTail> entry : table.entrySet() ) {

             HeadTail headTail = entry.getValue();

             Node curr = headTail.head.next;

             Node levelChangedNodesHead = null;
             Node levelChangedNodesTail = null;

             while (curr != null) {

                 Long timeDiff = now.getTime() - curr.lastNotified.getTime();
                 Node next = curr.next ;
                 Node prev = curr.prev ;

                 if (timeDiff >= config.WaitTimeForEscalation(curr.level)) {

                     //TODO: Keep the temporary list of update nodes ;

                     if (levelChangedNodesHead == null) {
                         levelChangedNodesHead = curr;
                         levelChangedNodesTail = curr;

                         curr.next = null;
                         curr.prev = null;
                     } else {
                         levelChangedNodesTail.next = curr;
                         curr.prev = levelChangedNodesTail;
                         curr.next = null;
                         levelChangedNodesTail = curr;
                     }

                     curr.level++;
                     curr.lastNotified = now;

                     next.prev = prev ;
                     prev.next = next ;

                     levelChangeNotifier.OnChange(curr.val);
                 } else {
                     if (timeDiff < timeToSleep) {

                         timeToSleep = timeDiff.intValue();
                     }

                     break ;
                 }

                 curr = next ;
             }

             if (levelChangedNodesHead != null) {

                 headTail.tail.next = levelChangedNodesHead;
                 headTail.tail = levelChangedNodesTail;
             }
           }
        }

    }
