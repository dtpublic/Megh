package com.datatorrent.alerts;

import java.awt.image.BufferedImage;
import java.util.List;

public interface AlertAction
{
    public class EscalationLevel
    {
        int levelTimeOut;
        List<String> toList;
        List<String> ccList;
        List<String> bccList;
    }

    Integer getEventID();

    String getApplicationId();

    String getAlertTitle();

    String getAlertContent();

    /* Tentative: Check if we need this */
    BufferedImage getScreenShot();

    String getAlertType();

    List<EscalationLevel> getEscalationPolicy();

    int getInitialAlertLevel();
}
