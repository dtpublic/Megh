package com.datatorrent.alerts;

 import com.datatorrent.alerts.Store.AlertsStore;
 import com.datatorrent.api.DefaultOutputPort;
 import com.datatorrent.common.util.BaseOperator;
 import com.datatorrent.api.Context;
 import com.datatorrent.api.DefaultInputPort;

/**
 * @since 2.1.0
 */
public class AlertsEngine extends BaseOperator
{
    AlertsStore alertsStore;

    public final transient DefaultOutputPort<AlertMessage> messageOutput = new DefaultOutputPort<AlertMessage>();

    public class LevelChange implements LevelChangeNotifier {

        @Override
        public void OnChange( AlertMessage message ) {
            sendMessage(message);
        }
    }

    private void sendMessage( AlertMessage message ) {
        messageOutput.emit(message);
    }

    public transient final DefaultInputPort<AlertMessage> messageInput = new DefaultInputPort<AlertMessage>()
    {
        @Override
        public void process( AlertMessage message )
        {
            if ( message.isFlag() ) {
                Long val = 30l ;
                alertsStore.put(val,message.getLevel(), message);
                sendMessage(message);
            }
            else {
                alertsStore.remove(message) ;
            }
        }
    };

    @Override
    public void setup(Context.OperatorContext context)
    {
        alertsStore = new AlertsStore(new LevelChange(), new ConfigImpl());
    }

    @Override
    public void teardown()
    {
    }
}
