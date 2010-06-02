package com.carbonfive.db.migration;

import com.carbonfive.db.jdbc.DatabaseType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.util.*;

import static org.apache.commons.collections.CollectionUtils.find;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.springframework.util.StringUtils.collectionToCommaDelimitedString;

/**
 * A MigrationResolver which leverages Spring's robust Resource loading mechanism, supporting 'file:', 'classpath:', and standard url format resources.
 * <p/>
 * Migration Location Examples: <ul> <li>classpath:/db/migrations/</li> <li>file:src/main/db/migrations/</li> <li>file:src/main/resources/db/migrations/</li>
 * </ul> All of the resources found in the migrations location which do not start with a '.' will be considered migrations.
 * <p/>
 * Configured out of the box with a SimpleVersionExtractor and the default resource pattern CLASSPATH_MIGRATIONS_SQL.
 *
 * @see Resource
 * @see PathMatchingResourcePatternResolver
 * @see VersionExtractor
 * @see MigrationFactory
 */
public class ResourceMigrationResolver implements MigrationResolver
{
    private static final Collection<String> CLASSPATH_MIGRATIONS_SQL = Arrays.asList("classpath:/db/migrations/");

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private Collection<String> migrationsLocations;
    private VersionExtractor versionExtractor;
    private MigrationFactory migrationFactory = new MigrationFactory();

    public ResourceMigrationResolver()
    {
        this(CLASSPATH_MIGRATIONS_SQL);
    }

    public ResourceMigrationResolver(String migrationsLocation)
    {
        this(Arrays.asList(migrationsLocation), new SimpleVersionExtractor());
    }

    public ResourceMigrationResolver(Collection<String> migrationsLocation)
    {
        this(migrationsLocation, new SimpleVersionExtractor());
    }

    public ResourceMigrationResolver(Collection<String> migrationsLocation, VersionExtractor versionExtractor)
    {
        setMigrationsLocations(migrationsLocation);
        setVersionExtractor(versionExtractor);
    }

    public ResourceMigrationResolver(String migrationsLocation, VersionExtractor versionExtractor)
    {
        this(migrationsLocation);
        setVersionExtractor(versionExtractor);
    }

    public Set<Migration> resolve(DatabaseType dbType)
    {
        Set<Migration> migrations = new HashSet<Migration>();

        final List<Resource> resources = findResources(dbType);

        // Remove resources starting with a '.' (e.g. .svn, .cvs, etc)
        CollectionUtils.filter(resources, new Predicate()
        {
            public boolean evaluate(Object object)
            {
                try
                {
                    return (((Resource) object).isReadable() && !((Resource) object).getFilename().startsWith("."));
                }
                catch (Exception e)
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Exception while filtering resource.", e);
                    }
                    return false;
                }
            }
        });

        if (resources.isEmpty())
        {
            String message = "No migrations were found using resource pattern '" + migrationsLocations + "'.";
            logger.error(message);
            throw new MigrationException(message);
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("Found " + resources.size() + " resources: " + collectionToCommaDelimitedString(resources));
        }

        // Extract versions and create executable migrations for each resource.
        for (Resource resource : resources)
        {
            String version = versionExtractor.extractVersion(resource.getFilename());
            if (find(migrations, new Migration.MigrationVersionPredicate(version)) != null)
            {
                String message = "Non-unique migration version.";
                logger.error(message);
                throw new MigrationException(message);
            }
            migrations.add(migrationFactory.create(version, resource));
        }

        return migrations;
    }

    /**
     * Find all resources in all migrations locations
     *
     * @param dbType
     * @return all resources in the migrations location
     * @see #setMigrationsLocations(Collection)
     */
    private List<Resource> findResources(DatabaseType dbType)
    {
        final List<Resource> resources = new ArrayList<Resource>();
        for (String location : migrationsLocations)
        {
            String convertedMigrationsLocation = convertMigrationsLocation(location, dbType);

            PathMatchingResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
            try
            {
                resources.addAll(Arrays.asList(patternResolver.getResources(convertedMigrationsLocation)));
            } catch (IOException e)
            {
                throw new MigrationException(e);
            }
        }
        return resources;
    }

    public Set<Migration> resolve()
    {
        return resolve(DatabaseType.UNKNOWN);
    }

    protected String convertMigrationsLocation(String migrationsLocation, DatabaseType dbType)
    {
        String converted = migrationsLocation;

        if (!(isBlank(FilenameUtils.getName(converted)) || FilenameUtils.getName(converted).contains("*")))
        {
            converted += "/";
        }

        if (!FilenameUtils.getName(converted).contains("*"))
        {
            converted += "*";
        }

        if (!(converted.startsWith("file:") || converted.startsWith("classpath:")))
        {
            converted = "file:" + converted;
        }

        return converted;
    }

    public void setMigrationsLocation(String migrationsLocation)
    {
        setMigrationsLocations(Arrays.asList(migrationsLocation));
    }

    public void setMigrationsLocations(Collection<String> migrationsLocation)
    {
        this.migrationsLocations = migrationsLocation;
    }

    public void setVersionExtractor(VersionExtractor versionExtractor)
    {
        this.versionExtractor = versionExtractor;
    }

    public void setMigrationFactory(MigrationFactory migrationFactory)
    {
        this.migrationFactory = migrationFactory;
    }
}
