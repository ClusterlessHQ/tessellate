# {{jreleaserCreationStamp}}
FROM {{dockerBaseImage}}

{{#dockerLabels}}
    LABEL {{.}}
{{/dockerLabels}}

{{#dockerPreCommands}}
    {{.}}
{{/dockerPreCommands}}

COPY assembly/* /

RUN unzip {{distributionArtifactFile}} && \
rm {{distributionArtifactFile}} && \
chmod +x {{distributionArtifactRootEntryName}}/bin/{{distributionExecutableUnix}} && \
mv /{{distributionArtifactRootEntryName}} /{{distributionExecutableName}}

ENV PATH="${PATH}:/{{distributionExecutableName}}/bin"

{{#dockerPostCommands}}
    {{.}}
{{/dockerPostCommands}}

ENTRYPOINT ["/{{distributionExecutableName}}/bin/{{distributionExecutableName}}"]

CMD ["--help"]
