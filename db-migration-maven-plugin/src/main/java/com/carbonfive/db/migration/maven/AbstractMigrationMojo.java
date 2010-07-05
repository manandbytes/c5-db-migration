/**
 * Copyright (c) 2008, 2010 Christian Nelson
 * Copyright (c) 2010 Mykola Nikishov <mn@mn.com.ua>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.carbonfive.db.migration.maven;

import com.carbonfive.db.jdbc.DatabaseType;
import com.carbonfive.db.jdbc.DatabaseUtils;
import com.carbonfive.db.migration.DriverManagerMigrationManager;
import com.carbonfive.db.migration.ResourceMigrationResolver;
import com.carbonfive.db.migration.SimpleVersionExtractor;
import com.carbonfive.db.migration.SimpleVersionStrategy;
import com.carbonfive.db.migration.VersionExtractor;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.springframework.util.StringUtils;

import static org.apache.commons.io.FilenameUtils.separatorsToUnix;
import static org.apache.commons.lang.StringUtils.*;

public abstract class AbstractMigrationMojo extends AbstractMojo
{

    private enum VersionExtractorsEnum
    {
        BASE_NAME(BaseNameVersionExtractor.class), TIMESTAMP(SimpleVersionExtractor.class);

        private final Class<? extends VersionExtractor> extarctor;

        VersionExtractorsEnum(final Class<? extends VersionExtractor> anExtarctor)
        {
            extarctor = anExtarctor;
        }

        public VersionExtractor getExtarctor() throws InstantiationException, IllegalAccessException
        {
            return extarctor.newInstance();
        }
    }

    /**
     * @parameter expression="${project}"
     * @required
     */
    protected MavenProject project;

    /** @parameter */
    private String url;
    /** @parameter */
    private String driver;
    /** @parameter */
    private String username;
    /** @parameter */
    private String password = "";

    /** @parameter */
    private String databaseType;

    /** @parameter */
    private String migrationsPath = "src/main/db/migrations/";

    /** @parameter */
    private String versionTable;
    /** @parameter */
    private String versionColumn;
    /** @parameter */
    private String appliedDateColumn;
    /** @parameter */
    private String durationColumn;
    /** @parameter */
    private String createSql;
    /** @parameter */
    private String dropSql;

    public abstract void executeMojo() throws MojoExecutionException;

    public final void execute() throws MojoExecutionException
    {
        if (isBlank(url) && isBlank(username)) { return; }

        password = isBlank(password) ? "" : password;

        if (isBlank(driver))
        {
            driver = DatabaseUtils.driverClass(url);
        }

        if (databaseType == null)
        {
            databaseType = DatabaseUtils.databaseType(url).toString();
        }

        validateConfiguration();

        executeMojo();
    }

    protected void validateConfiguration() throws MojoExecutionException
    {
        if (isBlank(driver))
        {
            throw new MojoExecutionException("No database driver. Specify one in the plugin configuration.");
        }

        if (isBlank(url))
        {
            throw new MojoExecutionException("No database url. Specify one in the plugin configuration.");
        }

        if (isBlank(username))
        {
            throw new MojoExecutionException("No database username. Specify one in the plugin configuration.");
        }

        try
        {
            DatabaseType.valueOf(databaseType);
        }
        catch (IllegalArgumentException e)
        {
            throw new MojoExecutionException(
                    "Database type of '" + databaseType + "' is invalid.  Valid values: " + StringUtils.arrayToDelimitedString(DatabaseType.values(), ", "));
        }

        try
        {
            final VersionExtractorsEnum versionExtractorClass = VersionExtractorsEnum.valueOf(versionExtractor);
            versionExtarctorInstance = versionExtractorClass.getExtarctor();
        } catch (IllegalArgumentException e)
        {
            throw new MojoExecutionException(
                    "Version extractor type of '" + versionExtractor + "' is invalid.  Valid values: " + StringUtils.arrayToDelimitedString(VersionExtractorsEnum.values(), ", "));
        } catch (Exception e)
        {
            throw new MojoExecutionException("Unable to instantiate version extarctor " + versionExtractor, e);
        }

        try
        {
            Class.forName(driver);
        }
        catch (ClassNotFoundException e)
        {
            throw new MojoExecutionException("Can't load driver class " + driver + ". Be sure to include it as a plugin dependency.");
        }
    }

    /**
     * Which version extraction strategy to use to get ID of the migration
     * script from its filename.
     *
     * @parameter expression="${versionExtractor}"
     */
    private String versionExtractor = VersionExtractorsEnum.TIMESTAMP.name();

    private VersionExtractor versionExtarctorInstance = null;

    public DriverManagerMigrationManager createMigrationManager()
    {
        DriverManagerMigrationManager manager = new DriverManagerMigrationManager(driver, url, username, password, DatabaseType.valueOf(databaseType));

        String path = migrationsPath;

        if (path.startsWith("file:"))
        {
            path = substring(path, 5);
        }
        if (!path.startsWith("classpath:") && !path.startsWith("\"") && !path.startsWith("/"))
        {
            path = project.getBasedir().getAbsolutePath() + "/" + path;
        }
        path = separatorsToUnix(path);

        final ResourceMigrationResolver migrationResolver = new ResourceMigrationResolver(path);
        migrationResolver.setVersionExtractor(versionExtarctorInstance);
        manager.setMigrationResolver(migrationResolver);

        SimpleVersionStrategy strategy = new SimpleVersionStrategy();
        strategy.setVersionTable(defaultIfEmpty(versionTable, SimpleVersionStrategy.DEFAULT_VERSION_TABLE));
        strategy.setVersionColumn(defaultIfEmpty(versionColumn, SimpleVersionStrategy.DEFAULT_VERSION_COLUMN));
        strategy.setAppliedDateColumn(defaultIfEmpty(appliedDateColumn, SimpleVersionStrategy.DEFAULT_APPLIED_DATE_COLUMN));
        strategy.setDurationColumn(defaultIfEmpty(durationColumn, SimpleVersionStrategy.DEFAULT_DURATION_COLUMN));
        manager.setVersionStrategy(strategy);

        return manager;
    }

    public String getUrl()
    {
        return url;
    }

    public String getDriver()
    {
        return driver;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public String getDatabaseType()
    {
        return databaseType;
    }

    public String getMigrationsPath()
    {
        return migrationsPath;
    }

    public String getVersionTable()
    {
        return versionTable;
    }

    public String getVersionColumn()
    {
        return versionColumn;
    }

    public String getAppliedDateColumn()
    {
        return appliedDateColumn;
    }

    public String getDurationColumn()
    {
        return durationColumn;
    }

    public String getCreateSql()
    {
        return createSql;
    }

    public String getDropSql()
    {
        return dropSql;
    }
}
