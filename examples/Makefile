all: read_table write_table create_table database_schema search_table
read_table:
	cd read && go run ./read.go
write_table:
	cd write && go run write.go
create_table:
	cd create && go run create.go
search_table: 
	cd search && go run search.go
database_schema:
	cd database && go run database.go


