package com.datatorrent.alerts;

 import com.datatorrent.alerts.Store.AlertsStore;
 import com.datatorrent.alerts.conf.Config;
 import com.datatorrent.alerts.conf.ConfigImpl;
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
    Config config = new ConfigImpl() ;

    public final transient DefaultOutputPort<ActionTuple> messageOutput = new DefaultOutputPort<ActionTuple>();

    public class LevelChange implements LevelChangeNotifier {

        @Override
        public void OnChange( AlertMessage message ) {

            sendMessage(message);
        }
    }

    /*
    *  TODO: Should we use single *Message* format in the App ?
    * */
    private void sendMessage( AlertMessage message ) {

        ActionTuple at = new ActionTuple();
        at.setAction(ActionTuple.Action.NOTIFY_EMAIL);
        at.setAppName(message.getAppId());
        at.setLevel(message.getLevel());

        messageOutput.emit(at);
    }

    public transient final DefaultInputPort<AlertMessage> messageInput = new DefaultInputPort<AlertMessage>()
    {
        @Override
        public void process( AlertMessage message )
        {

            if ( message.isFlagUp() ) {
                alertsStore.put((long) config.WaitTimeForEscalation(message.getLevel()), message.getLevel(), message);
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
        alertsStore = new AlertsStore(new LevelChange(), config);
    }

    @Override
    public void teardown()
    {
    }
}
