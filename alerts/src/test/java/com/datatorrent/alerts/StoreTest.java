package com.datatorrent.alerts;

import com.datatorrent.alerts.Store.Store;
import java.util.*;

import com.datatorrent.alerts.Store.DoublyLinkedList;
import com.datatorrent.alerts.Store.Node;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class StoreTest {

    private TestStore store ;
    private ArrayList<Message> messageList ;
    private int len = 500 ;

    public class LevelChange implements LevelChangeNotifier {

        @Override
        public void OnChange( Message message ) {

            // System.out.println("Change Notifier " + message.getId());
        }
    }

    public static class TestStore extends Store {

        public TestStore(LevelChangeNotifier levelChangeNotifier, Integer DefaultWaitTime) {

            super(levelChangeNotifier, DefaultWaitTime) ;
        }

        public synchronized boolean allSorted() {

            Iterator<Map.Entry<Integer,DoublyLinkedList>> it = alertsWithSameTimeout.entrySet().iterator() ;

            while ( it.hasNext() ) {

                Map.Entry<Integer,DoublyLinkedList> entry = it.next();
                DoublyLinkedList list = entry.getValue();

                Node curr = list.head.next;

                while ( curr != list.tail ) {

                    if ( curr.next != list.tail ) {

                        if ( curr.lastNotified.getTime() - curr.next.lastNotified.getTime() > 0) return false ;
                    }

                    curr = curr.next ;
                }
            }

            return true ;
        }
    }

    private Message generator(Integer id, Integer level) {
        Message message = new Message() ;
        message.setFlag(true);
        message.setEventId(id.toString());
        message.setCurrentLevel(1); ;
        message.setAppId(id.toString());

        return message ;
    }

    @Before
    public void Init() {

        store = new TestStore(new LevelChange(), 10000);
        messageList = new ArrayList<>() ;

        for ( int i = 0 ; i < len ; ++i ) {

            Message message = generator(i, 1) ;

            Random rnd = new Random() ;
            Integer n = rnd.nextInt(100) + 1 ;
            store.put(n, 1, message);
            messageList.add(message) ;
        }
    }

    @Test
    public void testMessageStore_addRemove() throws Exception {


        for ( int i = 0 ; i < len ; ++i ) {

            assertTrue ( " Message present ", store.isPresent(messageList.get(i)) ) ;
        }

        for ( Message message: messageList) {

            store.remove(message);
        }

        for ( int i = 0 ; i < len ; ++i ) {

            assertFalse(" Message not present ", store.isPresent(messageList.get(i)));
        }

        for ( Message message: messageList) {

            store.remove(message);
        }
    }

    @Test
    public void testMessageStore_sortedlist() throws Exception {

        for ( int i = 0 ; i < 10 ; ++i ) {
            assertTrue("All the queues are sorted", store.allSorted());
            Thread.sleep(10000);
        }
    }
}

