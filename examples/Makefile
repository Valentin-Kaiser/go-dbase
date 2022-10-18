all: read_table write_table create_table search_table database_export database_schema  database_documentation
read_table:
	cd read && go run ./read.go
write_table:
	cd write && go run write.go
create_table:
	cd create && go run create.go
search_table: 
	cd search && go run search.go
database_export:
	cd database && go run database.go
database_schema:
	cd schema && go run schema.go
database_documentation:
	cd documentation && go run documentation.go