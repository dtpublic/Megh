package com.datatorrent.alerts;

 import com.datatorrent.alerts.Store.AlertsStore;
 import com.datatorrent.alerts.Store.MessageTracker;
 import com.datatorrent.api.DefaultOutputPort;
 import com.datatorrent.common.util.BaseOperator;
 import com.datatorrent.api.Context;
 import com.datatorrent.api.DefaultInputPort;

/**
 * @since 2.1.0
 */
public class AlertsEngine extends BaseOperator
{
    AlertsStore alerts ;
    MessageTracker<Long, Message> messageTracker ;

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
        public void process(Message message)
        {
            if ( message.isFlag() ) {
                alerts.add(message) ;
                Long val = 30l ;
                messageTracker.put(val,message.getLevel(), message);
                sendMessage(message);
            }
            else {
                alerts.remove(message) ;
            }
        }
    };

    @Override
    public void setup(Context.OperatorContext context)
    {
        alerts = new AlertsStore(new LevelChange(), new ConfigImpl());
        messageTracker = new MessageTracker<>(new LevelChange(), new ConfigImpl());
    }

    @Override
    public void teardown()
    {
    }
}