/**
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

import org.apache.commons.io.FilenameUtils;

import com.carbonfive.db.migration.VersionExtractor;

/**
 * Assumes the filename minus the extension is the migration version:
 * <ul>
 * <li>01234_create_bar.sql -> 01234_create_bar</li>
 * <li>20080518134512_create_foo.sql -> 20080518134512_create_foo</li>
 * <li>20080718214051_add_foo_name.sql -> 20080718214051_add_foo_name</li>
 * </ul>
 */
final class BaseNameVersionExtractor implements VersionExtractor {
    public String extractVersion(final String name) {
        return FilenameUtils.getBaseName(name);
    }
}
