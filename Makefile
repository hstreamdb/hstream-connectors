spec_docs:
	generate-schema-doc --config-file conf/json_schema_for_humans.json source-debezium/app/src/main/resources/ docs/specs/
	generate-schema-doc --config-file conf/json_schema_for_humans.json sink-jdbc/app/src/main/resources/ docs/specs/
	generate-schema-doc --config-file conf/json_schema_for_humans.json sink-mongodb/app/src/main/resources/spec.json docs/specs/sink_mongodb_spec.md
	generate-schema-doc --config-file conf/json_schema_for_humans.json sink-null/app/src/main/resources/spec.json docs/specs/sink_null_spec.md

build_images:
	(cd java-toolkit && ./gradlew publishToMavenLocal -PdisableSigning --info)
	(cd source-debezium && ./gradlew buildImages)
	(cd sink-jdbc && ./gradlew buildImages)
	(cd sink-mongodb && ./gradlew buildImages)
	(cd sink-null && ./gradlew buildImages)

pull_images:
	docker pull hstreamdb/hstream
	docker pull mcr.microsoft.com/mssql/server:2022-latest
	docker pull mysql
	docker pull postgres
	
pull_connector_images:
	docker pull hstreamdb/source-mysql
	docker pull hstreamdb/source-postgresql
	docker pull hstreamdb/source-sqlserver
	docker pull hstreamdb/source-mongodb
	docker pull hstreamdb/sink-mysql
	docker pull hstreamdb/sink-postgresql
	docker pull hstreamdb/sink-mongodb
	docker pull hstreamdb/sink-null

test:
	( \
		cd integration_tests && \
    export HSTREAM_IMAGE_NAME=hstreamdb/hstream && \
		export HSTREAM_IO_USE_DEFAULT_IMAGES=true && \
    ./gradlew test --rerun-tasks --info --fail-fast \
	)
