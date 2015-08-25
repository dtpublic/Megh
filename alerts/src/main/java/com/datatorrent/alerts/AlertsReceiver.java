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
        AlertMessage message = new AlertMessage() ;

        message.setFlag(true);
        ++i ;
        message.setEventId(i);
        message.setLevel(1) ;
        message.setAppId(i.toString());

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

    public final transient DefaultOutputPort<AlertMessage> messageOutput = new DefaultOutputPort<AlertMessage>();

    @Override
    public void setup(Context.OperatorContext context)
    {

    }

    @Override
    public void teardown()
    {

    }
}