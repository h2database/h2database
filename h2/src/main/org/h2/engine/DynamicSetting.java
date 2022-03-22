package org.h2.engine;

import java.util.ArrayList;

import org.h2.command.CommandInterface;
import org.h2.jdbc.meta.DatabaseMeta;
import org.h2.message.Trace;
import org.h2.result.ResultInterface;
import org.h2.store.DataHandler;
import org.h2.util.NetworkConnectionInfo;
import org.h2.util.TimeZoneProvider;
import org.h2.util.Utils;
import org.h2.value.ValueLob;

/**
 * Dynamic settings.
 */
public class DynamicSetting {

    /**
     * The database mode.
     */
    public final Mode mode;

    /**
     * The current time zone.
     */
    public final TimeZoneProvider timeZone;

    /**
     * Creates new instance of dynamic settings.
     *
     * @param mode
     *            the database mode
     * @param timeZone
     *            the current time zone
     */
    public DynamicSetting(Mode mode, TimeZoneProvider timeZone) {
        this.mode = mode;
        this.timeZone = timeZone;
    }

}
