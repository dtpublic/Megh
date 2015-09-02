package com.datatorrent.alerts;

 import com.datatorrent.alerts.Store.Store;
 import com.datatorrent.alerts.conf.ConfigImpl;
 import com.datatorrent.api.DefaultOutputPort;
 import com.datatorrent.common.util.BaseOperator;
 import com.datatorrent.api.Context;
 import com.datatorrent.api.DefaultInputPort;

/**
 * @since 2.1.0
 */
public class Engine extends BaseOperator
{
    Store store;

    public final transient DefaultOutputPort<Message> messageOutput = new DefaultOutputPort<Message>();

    public class LevelChange implements LevelChangeNotifier {

        @Override
        public void OnChange( Message message ) {
            sendMessage(message);
        }
    }

    private void sendMessage( Message message ) {
        messageOutput.emit(message);
    }

    public transient final DefaultInputPort<Message> messageInput = new DefaultInputPort<Message>()
    {
        @Override
        public void process( Message message )
        {
            if ( message.isFlag() ) {
                Long val = 30l ;
                store.put(val,message.getCurrentLevel(), message);
                sendMessage(message);
            }
            else {
                store.remove(message) ;
            }
        }
    };

    @Override
    public void setup(Context.OperatorContext context)
    {
        store = new Store(new LevelChange(), new ConfigImpl());
    }

    @Override
    public void teardown()
    {
    }
}
