all: clean read_table write_table create_table open_table_custom  search_table database_export database_schema  database_documentation
read_table:
	cd read && go run ./read.go
write_table:
	cd write && go run write.go
create_table:
	cd create && go run create.go
open_table_custom:
	cd custom && go run custom.go
search_table: 
	cd search && go run search.go
database_export:
	cd database && go run export.go
database_schema:
	cd schema && go run schema.go
database_documentation:
	cd documentation && go run documentation.go
clean:
	cd read && rm -f debug.log
	cd write && rm -f debug.log
	cd create && rm -f debug.log rm -f TEST.DBF && rm -f TEST.FPT
	cd custom && rm -f debug.log
	cd search && rm -f debug.log
	cd database && rm -f debug.log
	cd schema && rm -f debug.log
	cd documentation && rm -f debug.log
