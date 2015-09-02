package com.datatorrent.alerts.Store;

import com.datatorrent.alerts.Message;

import java.util.Date;

public class Node {
        public Node next = null;
        public Node prev = null;
        public Message val ;
        public Integer level = 0 ;
        public boolean snooze = false ;
        public Date lastNotified ;

        public Node( Message val, Integer level ) {
            this.val = val ;
            lastNotified = new Date() ;
            this.level = level ;
        }

        public Node() {} ;
}
