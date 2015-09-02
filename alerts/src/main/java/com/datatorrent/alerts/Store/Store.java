package com.datatorrent.alerts.Store;

import com.datatorrent.alerts.conf.Config;
import com.datatorrent.alerts.LevelChangeNotifier;
import com.datatorrent.alerts.Message;

import java.util.*;

/*
* Implementation is inspired from LRU cache.
*
* "alertsWithSameTimeout" - All the messages with same timeout are clubbed together and
*           the clubbed messages are kept in the sorted on last refresh time.
*
* "messageToNode" - Helps to quickly find the message for various purposes like removing the messages.
*
* */

/*
*  TODO : Add Idempotent manager support, Which timestamp to use, old ones ?
* */
public class Store {

    protected HashMap<Long, DoublyLinkedList> alertsWithSameTimeout;
    private HashMap<Message, Node> messageToNode ;
    private volatile int timeToSleep = 10000 ;
    private LevelChangeNotifier levelChangeNotifier ;
    private Config config ;
    private Thread timer ;

    public Store(LevelChangeNotifier levelChangeNotifier, Config config) {

        alertsWithSameTimeout = new HashMap<>() ;
        messageToNode = new HashMap<>() ;

        this.levelChangeNotifier = levelChangeNotifier ;
        this.config = config ;
        this.timeToSleep = config.WaitTimeForEscalation(0) ;

        timer = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {

                try {
                    Thread.sleep(timeToSleep);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                isItTimeToEscalate();
            }
        }
        }) ;

      timer.start();
    }

    public synchronized void put(Long key, Integer level, Message value) {

        //TODO: If message already exists what to do ?
        Node node = new Node(value, level) ;
        messageToNode.put(value, node) ;

        if ( !alertsWithSameTimeout.containsKey(key) ) {

            alertsWithSameTimeout.put(key, new DoublyLinkedList());
        }

        alertsWithSameTimeout.get(key).append(node);
    }

    public synchronized boolean isPresent( Message message ) {

        return messageToNode.containsKey(message) ;
    }

    // TODO : Time specified by the user
    //      : Keep track that the message was snoozed and make it a part of the message.
    public synchronized void setSnooze( Message message, boolean set ) {

        if ( isPresent(message) ) {
            messageToNode.get(message).snooze = set ;
        }
    }

    public synchronized void remove( Message value ) {

        if ( messageToNode.containsKey(value) ) {

            DoublyLinkedList.removeNode(messageToNode.get(value)) ;
            messageToNode.remove(value) ;
        }
    }

    /**
     * This decides whether the messages needs to be escalated to next level or not and sends the notification via callback.
     * Some messages may be in the Max Escalation Level in that case it just send the notification the callback after the timeout.
     */
     protected synchronized void isItTimeToEscalate() {

         Date now = new Date() ;
         timeToSleep = config.WaitTimeForEscalation(0);

         Iterator<Map.Entry<Long,DoublyLinkedList>> it = alertsWithSameTimeout.entrySet().iterator() ;
         ArrayList<Node> goingToNewLevel = new ArrayList<>() ;

         while ( it.hasNext() ) {

             Map.Entry<Long,DoublyLinkedList> entry = it.next();

             DoublyLinkedList list = entry.getValue();
             DoublyLinkedList notifiedList = new DoublyLinkedList() ;

             Node curr = list.head.next;

             if ( curr == list.tail ) {
                it.remove(); continue ;
             }

             while (curr != list.tail ) {

                 Long timeDiff = now.getTime() - curr.lastNotified.getTime();

                 Node next = curr.next ;

                 if ( curr.snooze ) {
                     curr = next ; continue;
                 }

                 if (timeDiff >= config.WaitTimeForEscalation(curr.level)) {

                     curr.lastNotified = now;

                     if ( curr.level < config.MaxLevel() ) {
                         curr.level++;

                         goingToNewLevel.add(curr) ;
                     }
                     else {
                         notifiedList.append(curr);
                     }

                     levelChangeNotifier.OnChange(curr.val);

                 } else {

                     timeToSleep = Math.min(timeDiff.intValue(), timeToSleep) ;
                     break ;
                 }

                 curr = next ;
             }

             list.appendAndDrain(notifiedList);
           }

         for ( Node node : goingToNewLevel ) {

            Long waitTime = (long) config.WaitTimeForEscalation(node.level) ;
            put(waitTime, node.level, node.val);
         }
     }
}

