package com.datatorrent.alerts.Store;


/**
 * Sentinel nodes are used in the beginning and at the end to simplify the operations.
 */
public class DoublyLinkedList {

    public Node head = null ;
    public Node tail = null ;

    public DoublyLinkedList() {

        head = new Node() ;
        tail = new Node() ;

        head.next = tail ;
        tail.prev = head ;
    }

    public void append( Node node ){

        Node prev = tail.prev ;

        prev.next = node ;
        tail.prev = node ;

        node.next = tail ;
        node.prev = prev ;
    }

    public void appendAndDrain(DoublyLinkedList tlist) {

        if ( tlist.isEmpty() ) {
            return ;
        }

        if ( head.next == tail ) {

            head = tlist.head ;
            tail = tlist.tail ;
        }
        else
        if ( tlist.head.next != tlist.tail ) {

            tail.prev.next = tlist.head.next ;
            tlist.head.next.prev = tail.prev ;
        }

        tlist.head.next = tlist.tail ;
        tlist.tail.prev = tlist.head ;
    }

    public boolean isEmpty() {

        return head.next == tail ;

    }

    public static void removeNode( Node node ) {

        Node prev = node.prev ;
        Node next = node.next ;

        prev.next = next ;
        next.prev = prev ;
    }
}
