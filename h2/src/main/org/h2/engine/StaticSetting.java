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
 * Static settings.
 */
public class StaticSetting
{
    /**
     * Whether unquoted identifiers are converted to upper case.
     */
    public final boolean databaseToUpper;

    /**
     * Whether unquoted identifiers are converted to lower case.
     */
    public final boolean databaseToLower;

    /**
     * Whether all identifiers are case insensitive.
     */
    public final boolean caseInsensitiveIdentifiers;

    /**
     * Creates new instance of static settings.
     *
     * @param databaseToUpper
     *            whether unquoted identifiers are converted to upper case
     * @param databaseToLower
     *            whether unquoted identifiers are converted to lower case
     * @param caseInsensitiveIdentifiers
     *            whether all identifiers are case insensitive
     */
    public StaticSetting(boolean databaseToUpper, boolean databaseToLower, boolean caseInsensitiveIdentifiers) {
        this.databaseToUpper = databaseToUpper;
        this.databaseToLower = databaseToLower;
        this.caseInsensitiveIdentifiers = caseInsensitiveIdentifiers;
    }

}
