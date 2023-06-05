/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.junit;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;

public class ResourceExtension implements ParameterResolver {
    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return
                parameterContext.isAnnotated(PathForResource.class) ||
                        parameterContext.isAnnotated(PathForOutput.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        if (parameterContext.isAnnotated(PathForResource.class)) {
            return resolveInput(parameterContext, extensionContext);
        } else {
            return resolveOutput(parameterContext, extensionContext);
        }
    }

    private static Object resolveInput(ParameterContext parameterContext, ExtensionContext extensionContext) {
        Optional<PathForResource> annotation = parameterContext.findAnnotation(PathForResource.class);

        if (annotation.isEmpty()) {
            throw new ParameterResolutionException("URIForResource annotation not present");
        }

        PathForResource pathForResource = annotation.get();

        Class type = pathForResource.type() == Object.class ? extensionContext.getRequiredTestClass() : pathForResource.type();
        URL resource = type.getResource(pathForResource.value());

        if (resource == null) {
            throw new ParameterResolutionException("resource not found: " + pathForResource.value());
        }

        Path resolved = Paths.get(resource.getPath())
                .normalize();

        return resolveParam(parameterContext, resolved);
    }

    private Object resolveOutput(ParameterContext parameterContext, ExtensionContext extensionContext) {
        Optional<PathForOutput> annotation = parameterContext.findAnnotation(PathForOutput.class);

        if (annotation.isEmpty()) {
            throw new ParameterResolutionException("URIForResource annotation not present");
        }

        PathForOutput pathForOutput = annotation.get();

        String path = pathForOutput.value();

        Class<?> requiredTestClass = extensionContext.getRequiredTestClass();
        Method requiredTestMethod = extensionContext.getRequiredTestMethod();

        Path resolved = Paths.get("build")
                .resolve("output")
                .resolve(requiredTestClass.getName())
                .resolve(requiredTestMethod.getName())
                .resolve(path)
                .toAbsolutePath()
                .normalize();

        if (Files.exists(resolved)) {
            try {
                Files.walk(resolved)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        // only create the parent directories to the target directory
        resolved.getParent().toFile().mkdirs();

        return resolveParam(parameterContext, resolved);
    }

    @NotNull
    private static Serializable resolveParam(ParameterContext parameterContext, Path resolved) {
        if (parameterContext.getParameter().getType() == URI.class) {
            return resolved.toUri();
        } else if (parameterContext.getParameter().getType() == URL.class) {
            return url(resolved.toUri());
        } else {
            return resolved.toString();
        }
    }

    @NotNull
    private static URL url(URI uri) {
        try {
            return uri.toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private static URI uri(URL resource) {
        try {
            return resource.toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
