package com.datatorrent.alerts;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class Notify extends BaseOperator {

    public transient final DefaultInputPort<AlertMessage> messageInput = new DefaultInputPort<AlertMessage>()
    {
        @Override
        public void process(AlertMessage message)
        {
            System.out.println("Nofity2 " + message.getEventID() + " " + message.getLevel().toString());
        }
    };
}
