package com.datatorrent.alerts;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class Notify extends BaseOperator {

    public transient final DefaultInputPort<Message> messageInput = new DefaultInputPort<Message>()
    {
        @Override
        public void process(Message message)
        {
            System.out.println("Nofity2 " + message.getAppId() + " " + message .toString());
        }
    };
}
