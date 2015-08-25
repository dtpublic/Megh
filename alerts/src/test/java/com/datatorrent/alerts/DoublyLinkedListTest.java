package com.datatorrent.alerts;

import com.datatorrent.alerts.Store.DoublyLinkedList;
import com.datatorrent.alerts.Store.Node;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

public class DoublyLinkedListTest {

    @Test
    public void appendingAndOrder() {

        DoublyLinkedList dll = new DoublyLinkedList() ;
        ArrayList<Node> nodes = new ArrayList<>() ;

        int len = 10 ;

        for ( int i = 0 ; i < len ; ++i ) {

            AlertMessage message = new AlertMessage();
            message.setFlag(true);
            message.setEventId(i);
            message.setLevel(i) ;

            Node node = new Node( message ,i) ;

            dll.append(node);
            nodes.add(node) ;
        }

        Node curr = dll.head.next ;

        for ( int i = 0 ; i < len ; ++i ) {

            Node fromArrayList = nodes.get(i) ;

            assertTrue ( " Level in Nodes ", fromArrayList.level == curr.level  ) ;
            assertTrue ( " Id in Message ", fromArrayList.val.getEventID() == curr.val.getEventID()  ) ;

            curr = curr.next ;
        }
    }

    @Test
    public void removing() {

        DoublyLinkedList dll = new DoublyLinkedList() ;
        ArrayList<Node> nodes = new ArrayList<>() ;

        assertTrue("Initially empty", dll.isEmpty() );

        int len = 10 ;

        for ( int i = 0 ; i < len ; ++i ) {

            AlertMessage message = new AlertMessage();
            message.setFlag(true);
            message.setEventId(i);
            message.setLevel(i) ;

            Node node = new Node( message ,i) ;

            dll.append(node);
            nodes.add(node) ;
        }


        for ( int i = 0 ; i < len ; ++i ) {

            Node fromArrayList = nodes.get(i) ;
            DoublyLinkedList.removeNode(fromArrayList);
        }

        assertTrue("It should be empty", dll.isEmpty() );
    }

    @Test
    public void appending2Lists() {

        DoublyLinkedList dll = new DoublyLinkedList() ;
        DoublyLinkedList dll2 = new DoublyLinkedList() ;
        ArrayList<Node> nodes = new ArrayList<>() ;

        int len = 10 ;

        for ( int i = 0 ; i < len ; ++i ) {

            AlertMessage message = new AlertMessage();
            message.setFlag(true);
            message.setEventId(i);
            message.setLevel(i) ;

            Node node = new Node( message ,i) ;

            dll.append(node);
            nodes.add(node) ;
        }

        for ( int i = 0 ; i < len ; ++i ) {

            AlertMessage message = new AlertMessage();
            message.setFlag(true);
            message.setEventId(i);
            message.setLevel(i) ;

            Node node = new Node( message, i ) ;

            dll2.append(node);
            nodes.add(node) ;
        }

        dll.appendAndDrain(dll2);

        Node curr = dll.head.next ;

        for ( int i = 0 ; i < 2 * len ; ++i ) {

            Node fromArrayList = nodes.get(i) ;

            assertTrue ( " Level in Nodes ", fromArrayList.level == curr.level  ) ;
            assertTrue ( " Id in Message ", fromArrayList.val.getEventID() == curr.val.getEventID()  ) ;

            curr = curr.next ;
        }
    }
}
