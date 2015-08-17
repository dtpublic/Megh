package com.datatorrent.alerts.Store;

import com.datatorrent.alerts.Config;
import com.datatorrent.alerts.LevelChangeNotifier;
import com.datatorrent.alerts.Message;

import java.util.*;

/**
 * Stores the message and it periodically raises the alert level of the messages based on the Config and gives the callback
 * mechanism for handling the level changed messages.
 */
public class AlertsStore {

    private LevelChangeNotifier levelChangeNotifier ;
    private ArrayList<Message> store;
    private volatile long timeToSleep ;
    private Config config ;
    private Thread escalate;

    public AlertsStore(LevelChangeNotifier levelChangeNotifier, Config config) {
        this.levelChangeNotifier = levelChangeNotifier ;
        this.config = config ;
        timeToSleep = config.WaitTimeForEscalation(0) ;

        store = new ArrayList<>() ;

        escalate = new Thread( new Runnable() {
            @Override
            public void run() {

                while (true) {

                    try {
                        isItTimeToEscalate();
                        Thread.sleep(timeToSleep);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        escalate.start();
    }

    synchronized public void add ( Message message ) {

        store.add(message) ;
    }

    synchronized public void remove ( Message message ) {

        store.remove(message) ;
    }

    synchronized private void isItTimeToEscalate() {

        Date date = new Date() ;

        Iterator<Message> iter = store.iterator() ;

        while (iter.hasNext()) {

            Message message = iter.next() ;

            Long timeDiff = date.getTime() - message.getLastLevelChange().getTime() ;

            if ( config.WaitTimeForEscalation(message.getLevel()) <= timeDiff ) {

                if ( !message.getLevel().equals(config.MaxLevel()) ) {
                    message.setLevel(message.getLevel() + 1) ;
                }

                message.setLastLevelChange(date);
                levelChangeNotifier.OnChange(message);
            }
            else {
                timeToSleep = timeDiff ;
            }
        }
    }
}