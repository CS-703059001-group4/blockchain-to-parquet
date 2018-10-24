all: convert

index:
	@echo "Index transaction outputs"
	@go run cmd/indexer/main.go
	@echo ""

convert: index
	@echo "Convert dat file to parquet format"
	@go run cmd/converter/main.go
	@echo ""

clean:
	@rm -rf db out