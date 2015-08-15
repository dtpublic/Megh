package com.datatorrent.alerts;

import com.datatorrent.alerts.Store.MessageTracker;
import org.junit.Test;
import java.util.Date;


public class MessageTrackerTest {

    public class LevelChange implements LevelChangeNotifier {

        @Override
        public void OnChange( Message message ) {
            System.out.println(message.getId());
        }
    }

    private Message generator(Integer id, Integer level) {
        Message message = new Message() ;
        message.setFlag(true);
        message.setId(id);
        message.setLevel(1) ;
        message.setLastLevelChange(new Date());

        return message ;
    }

    @Test
    public void testMessageTracker() throws Exception {

        MessageTracker<Long, Message> store = new MessageTracker<>(new LevelChange(), new ConfigImpl());

        Message message1 = generator(10,1) ;
        Message message2 = generator(11,1) ;
        Message message3 = generator(12,1) ;
        Message message4 = generator(13,1) ;

        store.put(10l, 1, message1);
        store.put(10l, 1, message2);
        store.put(10l, 1, message3);
        store.put(10l, 1, message4);

        store.remove(message2);
        store.remove(message3);
        store.remove(message1);
        store.remove(message4);
    }
}
