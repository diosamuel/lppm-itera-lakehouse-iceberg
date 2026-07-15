# lakehouse_docs dbt project

## Manual dbt docs

This project uses dbt as a documentation layer only. You can manually edit
schema, table, and column descriptions in `models/sources.yml` without creating
SQL models or running `dbt run`.

The local `profiles.yml` is configured for the Trino service in the root
`docker-compose.yaml`.

Generate docs from the host machine:

```bash
uv run dbt docs generate --project-dir dbt/lakehouse_docs --profiles-dir dbt/lakehouse_docs
uv run dbt docs serve --project-dir dbt/lakehouse_docs --profiles-dir dbt/lakehouse_docs
```

If you only want the manually typed docs and do not want dbt to query Trino for
catalog metadata, use:

```bash
uv run dbt docs generate --project-dir dbt/lakehouse_docs --profiles-dir dbt/lakehouse_docs --empty-catalog
```

From inside the Docker network, use the `docker` target:

```bash
uv run dbt docs generate --project-dir dbt/lakehouse_docs --profiles-dir dbt/lakehouse_docs --target docker
```

Defaults:

| Setting | Host target | Docker target |
|---------|-------------|---------------|
| Host | `localhost` | `lppm-trino` |
| Port | `8085` | `8085` |
| User | `trino` | `trino` |
| Catalog | `default` | `default` |
| Schema | `gold` | `gold` |

Override with `TRINO_HOST`, `TRINO_HOST_DOCKER`, `TRINO_PORT`, `TRINO_USER`,
`TRINO_CATALOG`, or `TRINO_SCHEMA`.
