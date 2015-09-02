package com.datatorrent.alerts;

import com.datatorrent.alerts.Store.DoublyLinkedList;
import com.datatorrent.alerts.Store.Node;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DoublyLinkedListTest {

    DoublyLinkedList dll  ;
    ArrayList<Node> nodes  ;
    int len = 10 ;

    @Test
    public void base() {

        dll = new DoublyLinkedList() ;
        assertTrue("Init empty ", dll.isEmpty());
    }

    @Test
    public void appendingAndOrder() {

        Node curr = dll.head.next ;

        for ( int i = 0 ; i < len ; ++i ) {

            Node fromArrayList = nodes.get(i) ;

            assertTrue ( " Level in Nodes ", fromArrayList.level.equals(curr.level) ) ;
            assertTrue ( " Id in Message ", fromArrayList.val.getEventId().equals(curr.val.getEventId()) ) ;

            curr = curr.next ;
        }
    }

    @Test
    public void removing() {

        for ( int i = 0 ; i < len ; ++i ) {

            Node fromArrayList = nodes.get(i) ;
            DoublyLinkedList.removeNode(fromArrayList);
        }

        assertTrue("List should be empty", dll.isEmpty() );
    }

    @Test
    public void appending2Lists() {

        DoublyLinkedList dll2 = new DoublyLinkedList() ;
        ArrayList<Node> tempNodes = new ArrayList<>() ;

        generate(dll2, tempNodes);

        nodes.addAll(tempNodes) ;

        dll.appendAndDrain(dll2);

        Node curr = dll.head.next ;

        for ( int i = 0 ; i < 2 * len ; ++i ) {

            Node fromArrayList = nodes.get(i) ;

            assertTrue ( " Level in Nodes ", fromArrayList.level.equals( curr.level ) );
            assertTrue ( " Id in Message ", fromArrayList.val.getEventId().equals(curr.val.getEventId()) ) ;

            curr = curr.next ;
        }
    }

    private void generate( DoublyLinkedList dll, ArrayList<Node> nodes ) {

        for ( Integer i = 0 ; i < len ; ++i ) {

            Message message = new Message();
            message.setFlag(true);
            message.setEventId( i.toString() );
            message.setCurrentLevel(i);

            Node node = new Node( message ,i) ;

            dll.append(node);
            nodes.add(node) ;
        }
    }

    @Before
    public void init() {

        dll = new DoublyLinkedList() ;
        nodes = new ArrayList<>() ;

        generate(dll, nodes) ;
    }
}
