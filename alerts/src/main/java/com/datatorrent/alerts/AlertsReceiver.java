package com.datatorrent.alerts;

import java.util.Date;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * @since 2.1.0
 */
public class AlertsReceiver implements InputOperator
{
    private Integer i = 0 ;

    @Override
    public void emitTuples()
    {
        Message message = new Message() ;

        message.setFlag(true);
        ++i ;
        message.setId(i);
        message.setLevel(1) ;
        message.setLastLevelChange(new Date());

        messageOutput.emit(message);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void beginWindow(long l)
    {

    }

    @Override
    public void endWindow()
    {

    }

    public final transient DefaultOutputPort<Message> messageOutput = new DefaultOutputPort<Message>();

    @Override
    public void setup(Context.OperatorContext context)
    {

    }

    @Override
    public void teardown()
    {

    }
}